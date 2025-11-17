"""
Utility runtime for executing Solstice workflows locally (synchronously).

This runner is intended for tests and developer experiments where spinning up
Ray actors is overkill. It evaluates the job DAG produced by a workflow and
invokes each operator in topological order, propagating `Batch` objects between
stages. State-aware operators still receive an `OperatorContext`, but execution
occurs within a single process.
"""

from __future__ import annotations

import itertools
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from solstice.core.job import Job
import pyarrow as pa

from solstice.core.models import Batch, Record
from solstice.core.operator import Operator, SourceOperator

BatchHook = Callable[[str, Batch, Operator], None]
StageHook = Callable[[str, Operator], None]


class LocalJobRunner:
    """Synchronously execute a `Job` definition produced by a workflow."""

    def __init__(self, job: Job):
        self.job = job

    def run(
        self,
        *,
        before_stage: Optional[StageHook] = None,
        after_stage: Optional[StageHook] = None,
        before_batch: Optional[BatchHook] = None,
        after_batch: Optional[BatchHook] = None,
        failure_injector: Optional[BatchHook] = None,
    ) -> Dict[str, List[Batch]]:
        """
        Execute the job DAG and return the batches emitted by each stage.

        Hooks receive the stage_id (and batch when applicable) along with the
        operator instance. `failure_injector` can raise to simulate errors; an
        exception escapes the runner so callers can assert recovery behaviour.
        """
        reverse_dag = self._build_reverse_dag()
        stage_order = self._topological_order(reverse_dag)
        stage_results: Dict[str, List[Batch]] = {}

        for stage_id in stage_order:
            stage = self.job.stages[stage_id]
            operator = stage.operator_class(stage.operator_config)

            if before_stage:
                before_stage(stage_id, operator)

            if not reverse_dag.get(stage_id):
                # Source stage
                batches = self._run_source_stage(stage_id, stage.operator_config, operator)
            else:
                upstream_batches = list(
                    itertools.chain.from_iterable(
                        stage_results[upstream_id] for upstream_id in reverse_dag[stage_id]
                    )
                )
                batches = self._run_operator_stage(
                    stage_id,
                    operator,
                    upstream_batches,
                    before_batch=before_batch,
                    after_batch=after_batch,
                    failure_injector=failure_injector,
                )

            if after_stage:
                after_stage(stage_id, operator)

            operator.close()
            stage_results[stage_id] = batches

        return stage_results

    def _run_source_stage(
        self,
        stage_id: str,
        operator_config: Dict[str, Any],
        operator: Operator,
    ) -> List[Batch]:
        if not isinstance(operator, SourceOperator):
            raise TypeError(f"Stage {stage_id} expected SourceOperator, got {type(operator)}")

        items = list(operator.read())

        if not items:
            return []

        first_item = items[0]

        if isinstance(first_item, Batch):
            normalized: List[Batch] = []
            for index, batch in enumerate(items):
                batch_id = batch.batch_id or f"{stage_id}_batch_{index}"
                source_split = batch.source_split or f"{stage_id}_split_{index}"
                if batch.batch_id and batch.source_split:
                    normalized.append(batch)
                else:
                    normalized.append(
                        batch.replace(
                            data=batch.to_table(),
                            batch_id=batch_id,
                            source_split=source_split,
                        )
                    )
            return normalized

        if isinstance(first_item, (pa.Table, pa.RecordBatch)):
            arrow_batches: List[Batch] = []
            for index, payload in enumerate(items):
                arrow_batches.append(
                    Batch.from_arrow(
                        payload,
                        batch_id=f"{stage_id}_batch_{index}",
                        source_split=f"{stage_id}_split_{index}",
                    )
                )
            return arrow_batches

        records: List[Union[Record, Dict[str, Any]]] = items  # type: ignore[assignment]
        batch_size = operator_config.get("batch_size") or len(records) or 1
        batches: List[Batch] = []

        for index, start in enumerate(range(0, len(records), batch_size)):
            chunk = records[start : start + batch_size]
            batches.append(
                Batch.from_records(
                    chunk,
                    batch_id=f"{stage_id}_batch_{index}",
                    source_split=f"{stage_id}_split_{index}",
                )
            )

        return batches

    def _run_operator_stage(
        self,
        stage_id: str,
        operator: Operator,
        input_batches: Iterable[Batch],
        *,
        before_batch: Optional[BatchHook] = None,
        after_batch: Optional[BatchHook] = None,
        failure_injector: Optional[BatchHook] = None,
    ) -> List[Batch]:
        output_batches: List[Batch] = []

        for index, batch in enumerate(input_batches):
            if before_batch:
                before_batch(stage_id, batch, operator)

            if failure_injector:
                failure_injector(stage_id, batch, operator)

            processed_output = operator.process_batch(batch)

            if isinstance(processed_output, Batch):
                processed = processed_output
            elif isinstance(processed_output, (pa.Table, pa.RecordBatch)):
                processed = batch.replace(processed_output)
            elif isinstance(processed_output, Iterable):
                materialized = list(processed_output)
                if not materialized:
                    processed = Batch.empty(
                        batch_id=f"{batch.batch_id}_out_{index}",
                        source_split=batch.source_split,
                        schema=batch.schema,
                    )
                else:
                    first_element = materialized[0]
                    if isinstance(first_element, (Record, dict)):
                        processed = Batch.from_records(
                            materialized,
                            batch_id=f"{batch.batch_id}_out_{index}",
                            source_split=batch.source_split,
                            metadata=batch.metadata,
                            schema=batch.schema
                            if set(batch.column_names).issuperset(
                                {Batch.SOLSTICE_KEY_COLUMN, Batch.SOLSTICE_TS_COLUMN}
                            )
                            else None,
                        )
                    elif isinstance(first_element, (pa.RecordBatch, pa.Table)):
                        processed = Batch.from_arrow(
                            materialized,
                            batch_id=f"{batch.batch_id}_out_{index}",
                            source_split=batch.source_split,
                            metadata=batch.metadata,
                        )
                    else:
                        raise TypeError(
                            f"Operator {operator} returned unsupported iterable element "
                            f"type {type(first_element)!r}"
                        )
            else:
                raise TypeError(
                    f"Operator {operator} returned unsupported type {type(processed_output)!r}"
                )

            if after_batch:
                after_batch(stage_id, processed, operator)

            if len(processed):
                output_batches.append(processed)

        return output_batches

    def _build_reverse_dag(self) -> Dict[str, List[str]]:
        reverse_dag: Dict[str, List[str]] = {stage_id: [] for stage_id in self.job.stages}
        for upstream_id, downstream_ids in self.job.dag_edges.items():
            for downstream_id in downstream_ids:
                reverse_dag[downstream_id].append(upstream_id)
        return reverse_dag

    def _topological_order(self, reverse_dag: Dict[str, List[str]]) -> List[str]:
        visited = set()
        order: List[str] = []

        def visit(stage_id: str) -> None:
            if stage_id in visited:
                return
            visited.add(stage_id)
            for upstream in reverse_dag.get(stage_id, []):
                visit(upstream)
            order.append(stage_id)

        for stage_id in self.job.stages:
            visit(stage_id)

        return order
