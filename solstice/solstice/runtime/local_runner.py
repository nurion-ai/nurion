"""
Utility runtime for executing Solstice workflows locally (synchronously).

This runner is intended for tests and developer experiments where spinning up
Ray actors is overkill. It evaluates the job DAG produced by a workflow and
invokes each operator in topological order, propagating `SplitPayload` objects
between stages.
occurs within a single process.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from solstice.core.job import Job
from solstice.core.models import Split, SplitPayload
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
        source_splits: Optional[Mapping[str, Iterable[Split]]] = None,
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
        supplied_source_splits: Dict[str, List[Split]] = {}
        if source_splits:
            supplied_source_splits = {
                stage_id: list(splits) for stage_id, splits in source_splits.items()
            }

        for stage_id in stage_order:
            stage = self.job.stages[stage_id]
            operator = stage.operator_class(stage.operator_config)

            if before_stage:
                before_stage(stage_id, operator)

            if not reverse_dag.get(stage_id):
                # Source stage
                batches = self._run_source_stage(
                    stage_id,
                    operator,
                    supplied_source_splits.get(stage_id),
                )
            else:
                upstream_batches: List[Tuple[str, SplitPayload]] = []
                for upstream_id in reverse_dag.get(stage_id, []):
                    for batch in stage_results.get(upstream_id, []):
                        upstream_batches.append((upstream_id, batch))
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
        operator: Operator,
        provided_splits: Optional[Iterable[Split]],
    ) -> List[SplitPayload]:
        if not isinstance(operator, SourceOperator):
            raise TypeError(f"Stage {stage_id} expected SourceOperator, got {type(operator)}")

        batches: List[SplitPayload] = []
        if provided_splits is not None:
            splits = list(provided_splits)
        elif hasattr(operator, "plan_splits"):
            splits = list(getattr(operator, "plan_splits")())
        else:
            raise ValueError(
                f"Source stage {stage_id} did not receive splits. "
                "Provide `source_splits` or implement `plan_splits` on the operator."
            )

        for index, split in enumerate(splits):
            batch = operator.process_split(split)
            if batch is None:
                continue

            split_id = batch.split_id or split.split_id or f"{stage_id}_split_{index}"
            if not batch.split_id:
                batch = batch.with_new_data(
                    data=batch.to_table(),
                    split_id=split_id,
                )
            if len(batch):
                batches.append(batch)
        return batches

    def _run_operator_stage(
        self,
        stage_id: str,
        operator: Operator,
        input_batches: Iterable[Tuple[str, SplitPayload]],
        *,
        before_batch: Optional[BatchHook] = None,
        after_batch: Optional[BatchHook] = None,
        failure_injector: Optional[BatchHook] = None,
    ) -> List[SplitPayload]:
        output_batches: List[SplitPayload] = []

        for index, (upstream_stage, batch) in enumerate(input_batches):
            if before_batch:
                before_batch(stage_id, batch, operator)

            if failure_injector:
                failure_injector(stage_id, batch, operator)

            processing_split = self._build_processing_split(
                stage_id=stage_id,
                upstream_stage_id=upstream_stage,
                batch=batch,
                sequence=index,
            )
            processed_output = operator.process_split(processing_split, batch)
            processed = self._normalize_operator_output(
                batch, processed_output, processing_split.split_id
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

    def _build_processing_split(
        self,
        stage_id: str,
        upstream_stage_id: Optional[str],
        batch: SplitPayload,
        sequence: int,
    ) -> Split:
        split_id = batch.split_id or f"{stage_id}_split_{sequence}"
        parent_ids: List[str] = []
        if batch.split_id and batch.split_id != split_id:
            parent_ids.append(batch.split_id)
        data_range: Dict[str, Any] = {}
        if upstream_stage_id:
            data_range["source_stage"] = upstream_stage_id
        return Split(
            split_id=split_id,
            stage_id=stage_id,
            data_range=data_range,
            parent_split_ids=parent_ids,
        )

    def _normalize_operator_output(
        self,
        base_batch: SplitPayload,
        processed_output: Any,
        split_id: str,
    ) -> Optional[SplitPayload]:
        if processed_output is None:
            return None
        if isinstance(processed_output, SplitPayload):
            return processed_output
        if isinstance(processed_output, (pa.Table, pa.RecordBatch)):
            return base_batch.with_new_data(data=processed_output, split_id=split_id)
        if isinstance(processed_output, Sequence) and not isinstance(
            processed_output, (str, bytes)
        ):
            try:
                return base_batch.with_new_data(data=processed_output, split_id=split_id)
            except TypeError:
                pass
        raise TypeError(
            f"Operator {type(processed_output).__name__} returned unsupported type "
            f"{type(processed_output)!r}"
        )
