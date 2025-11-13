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
from typing import Any, Callable, Dict, Iterable, List, Optional

from solstice.core.job import Job
from solstice.core.models import Batch
from solstice.core.operator import Operator, OperatorContext, SourceOperator

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
            operator.open(OperatorContext(stage_id=stage_id))

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

        records = list(operator.read())
        batch_size = operator_config.get("batch_size") or len(records) or 1
        batches: List[Batch] = []

        for index, start in enumerate(range(0, len(records), batch_size)):
            chunk = records[start : start + batch_size]
            batches.append(
                Batch(
                    records=chunk,
                    batch_id=f"{stage_id}_batch_{index}",
                    source_split=None,
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

        for batch in input_batches:
            if before_batch:
                before_batch(stage_id, batch, operator)

            if failure_injector:
                failure_injector(stage_id, batch, operator)

            processed = operator.process_batch(batch)

            if after_batch:
                after_batch(stage_id, processed, operator)

            if processed.records:
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
