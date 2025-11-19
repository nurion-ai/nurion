"""
Utility runtime for executing Solstice workflows locally (synchronously).

This runner is intended for tests and developer experiments where spinning up
Ray actors is overkill. It evaluates the job DAG produced by a workflow and
invokes each operator in topological order, propagating `SplitPayload` objects
between stages.
occurs within a single process.
"""

from __future__ import annotations

import itertools
from typing import Any, Callable, Dict, Iterable, List, Optional

from solstice.core.job import Job
import pyarrow as pa

from solstice.core.models import Record, SplitPayload
from solstice.core.operator import Operator, SourceOperator

BatchHook = Callable[[str, SplitPayload, Operator], None]
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
    ) -> Dict[str, List[SplitPayload]]:
        """
        Execute the job DAG and return the batches emitted by each stage.

        Hooks receive the stage_id (and batch when applicable) along with the
        operator instance. `failure_injector` can raise to simulate errors; an
        exception escapes the runner so callers can assert recovery behaviour.
        """
        reverse_dag = self._build_reverse_dag()
        stage_order = self._topological_order(reverse_dag)
        stage_results: Dict[str, List[SplitPayload]] = {}

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
    ) -> List[SplitPayload]:
        if not isinstance(operator, SourceOperator):
            raise TypeError(f"Stage {stage_id} expected SourceOperator, got {type(operator)}")

        batches: List[SplitPayload] = []
        splits = operator.plan_splits()

        for index, split in enumerate(splits):
            batch = operator.process_split(split)
            if batch is None:
                continue

            split_id = batch.split_id or split.split_id
            if batch.split_id:
                batches.append(batch)
            else:
                batches.append(
                    batch.with_new_data(
                        data=batch.to_table(),
                        split_id=split_id,
                    )
                )

        return batches

    def _run_operator_stage(
        self,
        stage_id: str,
        operator: Operator,
        input_batches: Iterable[SplitPayload],
        *,
        before_batch: Optional[BatchHook] = None,
        after_batch: Optional[BatchHook] = None,
        failure_injector: Optional[BatchHook] = None,
    ) -> List[SplitPayload]:
        output_batches: List[SplitPayload] = []

        for index, batch in enumerate(input_batches):
            if before_batch:
                before_batch(stage_id, batch, operator)

            if failure_injector:
                failure_injector(stage_id, batch, operator)

            # Create a dummy split for local runner
            from solstice.core.models import Split, SplitStatus

            dummy_split = Split(
                split_id=f"{stage_id}_split_local",
                stage_id=stage_id,
                data_range={},
                status=SplitStatus.PENDING,
            )
            processed_output = operator.process_split(dummy_split, batch)

            if processed_output is None:
                processed = None
            elif isinstance(processed_output, SplitPayload):
                processed = processed_output
            elif isinstance(processed_output, (pa.Table, pa.RecordBatch)):
                processed = batch.with_new_data(data=processed_output)
            else:
                raise TypeError(
                    f"Operator {operator} returned unsupported type {type(processed_output)!r}"
                )

            if after_batch:
                after_batch(stage_id, processed if processed is not None else batch, operator)

            if processed is not None and len(processed):
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
