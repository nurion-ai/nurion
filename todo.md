# TODO

## Runtime Architecture Follow-Ups
- [ ] Make `StageMasterActor.run()` fully asynchronous: replace the internal blocking `ray.get` calls with awaitable `ray.wait` usage (or `asyncio` Ray API) so the event loop can schedule downstream work without yielding to threads.
- [ ] Rework `_collect_ready_results_async` to avoid `run_in_executor`; instead, refactor result handling into an async-friendly path that keeps all Ray RPCs non-blocking.
- [ ] Introduce an event-driven capacity signal between `RayJobRunner` and stage masters (e.g., awaitable backpressure notifications) to replace `_wait_for_capacity`’s polling sleep.
- [ ] Extend the per-stage run-loop monitoring in `RayJobRunner` with heartbeat timestamps and auto-restart logic so a stalled or crashed stage can be recovered without stopping the whole job.

## Scheduling & Scaling
- [ ] Implement adaptive worker scaling policies (queue depth, processing rate) that periodically call `StageMasterActor.scale_workers()` rather than relying on manual configuration.
- [ ] Add prioritisation or fairness in the StageMaster scheduler so workers do not starve long-waiting splits when new splits keep arriving.
- [ ] Explore batching of `ray.get_split_counters` calls (e.g., subscribe/publish) to reduce driver pressure and improve idle detection accuracy.

## Checkpointing & Fault Tolerance
- [ ] Ensure every `StageWorker.process_split()` result includes enough metadata for the StageMaster’s split-level checkpoints, and surface warnings via `MetaService` when checkpoints contain no handles.
- [ ] Support asynchronous checkpoint drains in `StageMasterActor` so checkpoint barriers do not block normal split processing.
- [ ] Wire `RayJobRunner.trigger_checkpoint()` into periodic/autonomous policies (time, records, backpressure) with coordination through `GlobalStateMaster`.

## Observability & Diagnostics
- [ ] Emit structured logs/events when stage masters enqueue/dequeue splits, including split IDs and downstream targets, to aid debugging.
- [ ] Expose runtime metrics (queue depth, worker utilisation, processing rate) via `MetaService` streaming updates for external monitoring.
- [ ] Add tracing hooks (OpenTelemetry or Ray timeline spans) around worker processing to diagnose slow operators.

## Testing & Documentation
- [ ] Add dedicated tests covering run-loop restart scenarios (stage crash, worker failure) to validate resilience of the new `RayJobRunner`.
- [ ] Write integration tests for checkpoint restore using `StatefulCounterOperator` to assert that restored state resumes counting without duplication.
- [ ] Update the design docs with sequence diagrams showing async split flow and checkpoint coordination under the new architecture.

