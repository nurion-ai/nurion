"""Integration tests for Solstice workflows and runtime coordination."""

import json

import pytest
import ray
from pyiceberg.catalog.sql import SqlCatalog

from solstice.actors.stage_master import StageMasterActor
from solstice.core.models import Batch, Record
from solstice.operators.map import MapOperator
from solstice.runtime.local_runner import LocalJobRunner
from solstice.state.backend import LocalStateBackend
from workflows.simple_etl import create_job
from tests.testdata import (
    ICEBERG_NUM_ROWS,
    LANCE_NUM_ROWS,
    ensure_iceberg_catalog,
    ensure_lance_dataset,
)

pytestmark = pytest.mark.integration


def _mark_seen(value):
    result = dict(value)
    result["seen"] = True
    return result


def _identity_transform(value):
    return dict(value)


class FailOnceMapOperator(MapOperator):
    """Operator that fails on the first batch and succeeds afterwards."""

    _failed_once = False

    def __init__(self, config):
        super().__init__(config)

    def process_batch(self, batch: Batch) -> Batch:
        if not type(self)._failed_once:
            type(self)._failed_once = True
            raise RuntimeError("intentional failure for testing")
        return super().process_batch(batch)


class StatefulCounterOperator(MapOperator):
    """Operator that tracks a monotonically increasing position in state."""

    def __init__(self, config):
        super().__init__(config)
        self.total_count = 0

    def open(self, context=None):
        super().open(context)
        self.total_count = self.context.get_state("total_count", 0)

    def process(self, record: Record):
        self.total_count += 1
        self.context.set_state("total_count", self.total_count)
        new_value = dict(self.map_fn(record.value))
        new_value["position"] = self.total_count
        return [
            Record(
                key=record.key,
                value=new_value,
                timestamp=record.timestamp,
                metadata=record.metadata.copy(),
            )
        ]


@pytest.fixture(scope="module")
def lance_test_table():
    """Return the path to the shared Lance dataset used across tests."""
    dataset_path = ensure_lance_dataset(refresh=False)
    yield str(dataset_path)
    FailOnceMapOperator._failed_once = False


@pytest.fixture(scope="module")
def iceberg_sql_catalog():
    """Provide a SQL catalog pointing at the generated Iceberg test data."""
    metadata = ensure_iceberg_catalog(refresh=True)
    catalog = SqlCatalog(
        metadata["catalog_name"],
        uri=metadata["catalog_uri"],
        warehouse=metadata["warehouse_uri"],
    )
    identifier = tuple(metadata["table_identifier"].split("."))
    return catalog, identifier


@pytest.fixture(scope="module")
def ray_cluster():
    """Initialise a lightweight in-process Ray runtime for actor tests."""
    ray.init(
        local_mode=True,
        include_dashboard=False,
        ignore_reinit_error=True,
        log_to_driver=False,
        runtime_env={"working_dir": None},
    )
    try:
        yield
    finally:
        ray.shutdown()


@pytest.fixture
def local_backend(tmp_path):
    backend_dir = tmp_path / "state"
    backend_dir.mkdir()
    return LocalStateBackend(str(backend_dir))


def test_simple_etl_workflow_local_runner(lance_test_table, local_backend, tmp_path):
    output_path = tmp_path / "results.json"
    job = create_job(
        job_id="etl_job",
        config={
            "input": lance_test_table,
            "output": str(output_path),
            "source_batch_size": 10,
            "transform_parallelism": 2,
            "filter_parallelism": 1,
        },
        state_backend=local_backend,
    )

    runner = LocalJobRunner(job)
    stage_results = runner.run()

    assert output_path.exists()

    sink_batches = stage_results["sink"]
    total_records = sum(len(batch.records) for batch in sink_batches)
    expected_records = max(LANCE_NUM_ROWS - 2, 0)
    assert total_records == expected_records

    with output_path.open() as fh:
        output_rows = [json.loads(line) for line in fh]

    assert len(output_rows) == total_records
    sample = output_rows[0]["value"]
    assert sample["processed"] is True
    assert sample["value_doubled"] == sample["value"] * 2


def test_iceberg_sql_catalog_contains_expected_rows(iceberg_sql_catalog):
    catalog, identifier = iceberg_sql_catalog
    table = catalog.load_table(identifier)
    arrow_table = table.scan().to_arrow()

    assert arrow_table.num_rows == ICEBERG_NUM_ROWS
    assert set(arrow_table.schema.names) == {"event_id", "event_type", "amount", "region"}


@pytest.mark.usefixtures("ray_cluster")
def test_stage_master_assigns_batches_and_collects(local_backend):
    stage_master = StageMasterActor.remote(
        stage_id="map_stage",
        operator_class=MapOperator,
        operator_config={"map_fn": _mark_seen},
        state_backend=local_backend,
        worker_resources={"num_cpus": 0.1},
        initial_workers=1,
        max_workers=2,
        min_workers=1,
    )

    try:
        batch = Batch(
            records=[Record(key=str(i), value={"value": i}) for i in range(5)],
            batch_id="batch-1",
        )
        ray.get(stage_master.add_input_batch.remote(batch))
        ray.get(stage_master.tick.remote(timeout=0.5))

        output_batch = ray.get(stage_master.get_output_batch.remote())
        assert output_batch is not None
        assert len(output_batch.records) == 5
        assert all(record.value["seen"] for record in output_batch.records)
    finally:
        ray.get(stage_master.shutdown.remote())


@pytest.mark.usefixtures("ray_cluster")
def test_stage_master_recovers_from_worker_failure(local_backend):
    stage_master = StageMasterActor.remote(
        stage_id="fail_stage",
        operator_class=FailOnceMapOperator,
        operator_config={"map_fn": _mark_seen},
        state_backend=local_backend,
        worker_resources={"num_cpus": 0.1},
        initial_workers=1,
        max_workers=3,
        min_workers=1,
    )

    try:
        batch = Batch(
            records=[Record(key=str(i), value={"value": i}) for i in range(3)],
            batch_id="batch-failure",
        )

        ray.get(stage_master.add_input_batch.remote(batch))

        output_batch = None
        for _ in range(5):
            ray.get(stage_master.tick.remote(timeout=0.5))
            output_batch = ray.get(stage_master.get_output_batch.remote())
            if output_batch is not None:
                break

        assert output_batch is not None
        assert all(record.value["seen"] for record in output_batch.records)
    finally:
        ray.get(stage_master.shutdown.remote())


@pytest.mark.usefixtures("ray_cluster")
def test_stage_master_checkpoint_and_restore(local_backend):
    stage_master = StageMasterActor.remote(
        stage_id="stateful_stage",
        operator_class=StatefulCounterOperator,
        operator_config={"map_fn": _identity_transform},
        state_backend=local_backend,
        worker_resources={"num_cpus": 0.1},
        initial_workers=1,
        max_workers=2,
        min_workers=1,
    )

    try:
        first_batch = Batch(
            records=[Record(key=str(i), value={"value": i}) for i in range(5)],
            batch_id="batch-0",
        )
        ray.get(stage_master.add_input_batch.remote(first_batch))
        ray.get(stage_master.tick.remote(timeout=0.5))
        output_batch = ray.get(stage_master.get_output_batch.remote())
        positions = [record.value["position"] for record in output_batch.records]
        assert positions == [1, 2, 3, 4, 5]

        checkpoint_id = "cp-1"
        ray.get(stage_master.trigger_checkpoint.remote(checkpoint_id))
        handles = ray.get(stage_master.collect_checkpoints.remote())
        assert handles

        ray.get(stage_master.restore_from_checkpoint.remote(checkpoint_id))

        second_batch = Batch(
            records=[Record(key=str(i), value={"value": i}) for i in range(5, 8)],
            batch_id="batch-1",
        )
        ray.get(stage_master.add_input_batch.remote(second_batch))
        ray.get(stage_master.tick.remote(timeout=0.5))

        output_batch_two = ray.get(stage_master.get_output_batch.remote())
        positions_two = [record.value["position"] for record in output_batch_two.records]
        assert positions_two == [6, 7, 8]
    finally:
        ray.get(stage_master.shutdown.remote())
