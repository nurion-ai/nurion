"""Stage Master v2 - Simplified queue-based architecture.

Key differences from v1:
- Master only manages its output queue
- Workers pull directly from upstream queue (not master-to-master)
- Uses QueueBackend abstraction for flexibility
- Cleaner separation of concerns

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                     Stage Master                            │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │              Output Queue (QueueBackend)            │   │
    │  │  - Persistent (Tansu) or in-memory                  │   │
    │  │  - Offset tracking for exactly-once                 │   │
    │  └─────────────────────────────────────────────────────┘   │
    │                           ▲                                 │
    │                           │ produce                         │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
    │  │  Worker 1  │  │  Worker 2  │  │  Worker N  │            │
    │  │            │  │            │  │            │            │
    │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘            │
    │        │               │               │                    │
    │        │ fetch         │ fetch         │ fetch              │
    │        ▼               ▼               ▼                    │
    └────────────────────────────────────────────────────────────┘
                             │
                             │ fetch from upstream queue
                             ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                  Upstream Stage Master                      │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │              Output Queue (QueueBackend)            │   │
    │  └─────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

import ray

from solstice.queue import QueueBackend, MemoryBackend, Record
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.core.stage import Stage


class QueueType(str, Enum):
    """Type of queue backend to use."""
    MEMORY = "memory"      # In-process only (for single-worker testing)
    RAY = "ray"            # Shared via Ray actor (for distributed testing)
    TANSU = "tansu"        # Persistent broker (for production)


@dataclass
class StageConfigV2:
    """Configuration for Stage Master v2.
    
    Attributes:
        queue_type: Type of queue backend:
            - MEMORY: In-process only (single-worker testing)
            - RAY: Shared via Ray actor (distributed testing)
            - TANSU: Persistent broker (production)
        tansu_storage_url: Storage URL for Tansu backend (s3://, sqlite://, etc.)
        tansu_port: Port for Tansu broker
        max_workers: Maximum number of workers
        min_workers: Minimum number of workers
        batch_size: Number of messages to fetch per batch
        commit_interval_ms: Interval between offset commits (ms)
        processing_timeout_s: Timeout for processing a single message
    """
    queue_type: QueueType = QueueType.RAY  # Default to Ray for distributed
    tansu_storage_url: str = "memory://"
    tansu_port: int = 9092
    
    max_workers: int = 4
    min_workers: int = 1
    
    batch_size: int = 100
    commit_interval_ms: int = 5000
    processing_timeout_s: float = 300.0
    
    # Worker resources
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "tansu_storage_url": self.tansu_storage_url,
            "tansu_port": self.tansu_port,
            "max_workers": self.max_workers,
            "min_workers": self.min_workers,
            "batch_size": self.batch_size,
            "commit_interval_ms": self.commit_interval_ms,
            "processing_timeout_s": self.processing_timeout_s,
        }


@dataclass
class QueueMessage:
    """Message format for inter-stage communication.
    
    The actual data payload is stored in Ray object store, 
    only the reference is passed through the queue.
    """
    message_id: str
    split_id: str
    data_ref: str  # Serialized Ray ObjectRef or S3 URI
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    def to_bytes(self) -> bytes:
        return json.dumps({
            "message_id": self.message_id,
            "split_id": self.split_id,
            "data_ref": self.data_ref,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
        }).encode()
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "QueueMessage":
        d = json.loads(data.decode())
        return cls(**d)


@dataclass
class StageStatus:
    """Status of a stage."""
    stage_id: str
    worker_count: int
    output_queue_size: int
    is_running: bool
    is_finished: bool
    failed: bool = False
    failure_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueueEndpoint:
    """Queue connection info that can be serialized to workers.
    
    Workers use this to create their own queue connections.
    For Ray backend, includes the actor reference.
    """
    queue_type: QueueType
    host: str = "localhost"
    port: int = 9092
    storage_url: str = "memory://"
    # Ray actor reference (for QueueType.RAY)
    actor_ref: Optional[ray.actor.ActorHandle] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "host": self.host,
            "port": self.port,
            "storage_url": self.storage_url,
        }


class StageMasterV2:
    """Simplified stage master that only manages output queue.
    
    Responsibilities:
    1. Manage output queue (create, provide access)
    2. Spawn and monitor workers
    3. Track stage completion
    
    NOT responsible for:
    - Pulling from upstream (workers do this)
    - Scheduling splits to workers (workers self-schedule)
    - Complex backpressure (queue handles this)
    """
    
    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        config: StageConfigV2,
        upstream_endpoint: Optional[QueueEndpoint] = None,
        upstream_topic: Optional[str] = None,
    ):
        self.job_id = job_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.config = config
        self.upstream_endpoint = upstream_endpoint
        self.upstream_topic = upstream_topic
        
        self.logger = create_ray_logger(f"MasterV2-{self.stage_id}")
        
        # Output queue (managed by master)
        self._output_queue: Optional[QueueBackend] = None
        self._output_topic = f"{job_id}_{self.stage_id}_output"
        
        # Output endpoint info for workers/downstream
        self._output_endpoint: Optional[QueueEndpoint] = None
        
        # Workers
        self._workers: Dict[str, ray.actor.ActorHandle] = {}
        self._worker_tasks: Dict[str, ray.ObjectRef] = {}
        
        # State
        self._running = False
        self._finished = False
        self._failed = False
        self._failure_message: Optional[str] = None
        self._start_time: Optional[float] = None
        
        # Consumer group for offset tracking
        self._consumer_group = f"{job_id}_{self.stage_id}"
    
    async def _create_queue(self) -> QueueBackend:
        """Create the appropriate queue backend."""
        if self.config.queue_type == QueueType.TANSU:
            from solstice.queue import TansuBackend
            queue = TansuBackend(
                storage_url=self.config.tansu_storage_url,
                port=self.config.tansu_port,
            )
            self._output_endpoint = QueueEndpoint(
                queue_type=QueueType.TANSU,
                port=self.config.tansu_port,
                storage_url=self.config.tansu_storage_url,
            )
        elif self.config.queue_type == QueueType.RAY:
            from solstice.queue import RayBackend
            queue = RayBackend()
            await queue.start()
            self._output_endpoint = QueueEndpoint(
                queue_type=QueueType.RAY,
                actor_ref=queue.get_actor_ref(),
            )
            await queue.create_topic(self._output_topic)
            return queue
        else:
            # MEMORY - only for single-process testing
            queue = MemoryBackend()
            self._output_endpoint = QueueEndpoint(
                queue_type=QueueType.MEMORY,
            )
        
        await queue.start()
        await queue.create_topic(self._output_topic)
        return queue
    
    async def start(self) -> None:
        """Start the stage master."""
        if self._running:
            return
        
        self.logger.info(f"Starting stage {self.stage_id}")
        self._start_time = time.time()
        self._running = True
        
        # Create output queue
        self._output_queue = await self._create_queue()
        
        # Spawn workers
        for i in range(self.config.min_workers):
            await self._spawn_worker()
        
        self.logger.info(f"Stage {self.stage_id} started with {len(self._workers)} workers")
    
    async def _spawn_worker(self) -> str:
        """Spawn a new worker."""
        worker_id = f"{self.stage_id}_w{len(self._workers)}_{uuid.uuid4().hex[:6]}"
        
        # Create worker actor
        resources = {}
        if self.config.num_cpus > 0:
            resources["num_cpus"] = self.config.num_cpus
        if self.config.num_gpus > 0:
            resources["num_gpus"] = self.config.num_gpus
        if self.config.memory_mb > 0:
            resources["memory"] = self.config.memory_mb * 1024 * 1024
        
        worker = StageWorkerV2.options(
            name=f"{self.stage_id}:{worker_id}",
            **resources,
        ).remote(
            worker_id=worker_id,
            stage=self.stage,
            upstream_endpoint=self.upstream_endpoint,
            upstream_topic=self.upstream_topic,
            output_endpoint=self._output_endpoint,
            output_topic=self._output_topic,
            consumer_group=self._consumer_group,
            config=self.config,
        )
        
        self._workers[worker_id] = worker
        
        # Start worker run loop
        task = worker.run.remote()
        self._worker_tasks[worker_id] = task
        
        self.logger.info(f"Spawned worker {worker_id}")
        return worker_id
    
    async def run(self) -> bool:
        """Run the stage until completion."""
        if not self._running:
            await self.start()
        
        try:
            # Wait for all workers to complete
            while self._running and not self._finished:
                # Check worker status
                done_tasks = []
                for worker_id, task in list(self._worker_tasks.items()):
                    try:
                        ready, _ = ray.wait([task], timeout=0.1)
                        if ready:
                            try:
                                result = ray.get(ready[0])
                                self.logger.info(f"Worker {worker_id} completed: {result}")
                            except Exception as e:
                                self.logger.error(f"Worker {worker_id} failed: {e}")
                                self._failed = True
                                self._failure_message = str(e)
                            done_tasks.append(worker_id)
                    except Exception as e:
                        self.logger.error(f"Error checking worker {worker_id}: {e}")
                
                # Remove completed workers
                for worker_id in done_tasks:
                    self._workers.pop(worker_id, None)
                    self._worker_tasks.pop(worker_id, None)
                
                # Check if all workers done
                if not self._workers:
                    self._finished = True
                    break
                
                await asyncio.sleep(0.1)
            
            if self._failed:
                raise RuntimeError(self._failure_message)
            
            return True
            
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the stage master."""
        self._running = False
        
        # Stop all workers
        for worker_id, worker in list(self._workers.items()):
            try:
                ray.get(worker.stop.remote(), timeout=5)
            except Exception as e:
                self.logger.warning(f"Error stopping worker {worker_id}: {e}")
        
        self._workers.clear()
        self._worker_tasks.clear()
        
        # Stop output queue
        if self._output_queue:
            await self._output_queue.stop()
            self._output_queue = None
        
        self.logger.info(f"Stage {self.stage_id} stopped")
    
    def get_output_queue(self) -> Optional[QueueBackend]:
        """Get the output queue for downstream stages."""
        return self._output_queue
    
    def get_output_topic(self) -> str:
        """Get the output topic name."""
        return self._output_topic
    
    def get_status(self) -> StageStatus:
        """Get current stage status."""
        return StageStatus(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            output_queue_size=0,  # Use async get_status_async for queue size
            is_running=self._running,
            is_finished=self._finished,
            failed=self._failed,
            failure_message=self._failure_message,
        )
    
    async def get_status_async(self) -> StageStatus:
        """Get current stage status with queue metrics."""
        output_size = 0
        if self._output_queue:
            try:
                output_size = await self._output_queue.get_latest_offset(self._output_topic)
            except Exception:
                pass
        
        return StageStatus(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            output_queue_size=output_size,
            is_running=self._running,
            is_finished=self._finished,
            failed=self._failed,
            failure_message=self._failure_message,
        )


@ray.remote
class StageWorkerV2:
    """Worker that pulls from upstream queue and produces to output queue.
    
    This worker is self-scheduling: it pulls messages from upstream,
    processes them, and produces results to the output queue.
    
    Exactly-once semantics:
    1. Fetch batch from upstream
    2. Process each message
    3. Produce output to output queue
    4. Commit upstream offset (only after output is durably stored)
    
    Note: Workers create their own queue connections from endpoints,
    since QueueBackend instances contain locks and cannot be serialized.
    """
    
    def __init__(
        self,
        worker_id: str,
        stage: "Stage",
        upstream_endpoint: Optional[QueueEndpoint],
        upstream_topic: Optional[str],
        output_endpoint: QueueEndpoint,
        output_topic: str,
        consumer_group: str,
        config: StageConfigV2,
    ):
        self.worker_id = worker_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.config = config
        
        # Store endpoints (will create connections in run())
        self.upstream_endpoint = upstream_endpoint
        self.upstream_topic = upstream_topic
        self.output_endpoint = output_endpoint
        self.output_topic = output_topic
        self.consumer_group = consumer_group
        
        # Queue connections (created lazily)
        self.upstream_queue: Optional[QueueBackend] = None
        self.output_queue: Optional[QueueBackend] = None
        
        self.logger = create_ray_logger(f"WorkerV2-{self.stage_id}-{worker_id}")
        
        # Initialize operator
        from solstice.core.operator import Operator
        self.operator: Operator = stage.operator_config.setup(worker_id=worker_id)
        
        # State
        self._running = False
        self._processed_count = 0
        self._error_count = 0
        self._last_commit_time = time.time()
    
    async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint) -> QueueBackend:
        """Create a queue connection from endpoint info."""
        if endpoint.queue_type == QueueType.TANSU:
            from solstice.queue import TansuBackend
            queue = TansuBackend(
                storage_url=endpoint.storage_url,
                port=endpoint.port,
            )
        elif endpoint.queue_type == QueueType.RAY:
            from solstice.queue import RayBackend
            # Use existing actor reference
            queue = RayBackend.from_actor_ref(endpoint.actor_ref)
        else:
            queue = MemoryBackend()
        
        await queue.start()
        return queue
    
    async def run(self) -> Dict[str, Any]:
        """Main processing loop."""
        self._running = True
        self.logger.info(f"Worker {self.worker_id} starting")
        
        try:
            # Create queue connections
            self.output_queue = await self._create_queue_from_endpoint(self.output_endpoint)
            
            if self.upstream_endpoint and self.upstream_topic:
                self.upstream_queue = await self._create_queue_from_endpoint(self.upstream_endpoint)
                # Regular worker: pull from upstream
                await self._process_from_upstream()
            else:
                # Source worker: generate data
                await self._process_as_source()
            
            return {
                "worker_id": self.worker_id,
                "processed_count": self._processed_count,
                "error_count": self._error_count,
            }
            
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
            raise
        finally:
            self._running = False
            # Cleanup queue connections
            if self.upstream_queue:
                await self.upstream_queue.stop()
            if self.output_queue:
                await self.output_queue.stop()
    
    async def _process_from_upstream(self) -> None:
        """Process messages from upstream queue."""
        # Get starting offset
        offset = await self.upstream_queue.get_committed_offset(
            self.consumer_group, self.upstream_topic
        ) or 0
        
        self.logger.info(f"Starting from offset {offset}")
        
        consecutive_empty = 0
        max_empty_polls = 100  # Give up after 100 empty polls (~10 seconds)
        
        while self._running:
            # Fetch batch from upstream
            records = await self.upstream_queue.fetch(
                self.upstream_topic,
                offset=offset,
                max_records=self.config.batch_size,
                timeout_ms=100,
            )
            
            if not records:
                consecutive_empty += 1
                if consecutive_empty >= max_empty_polls:
                    # Check if upstream is done
                    latest = await self.upstream_queue.get_latest_offset(self.upstream_topic)
                    if offset >= latest:
                        self.logger.info(f"Upstream exhausted at offset {offset}")
                        break
                await asyncio.sleep(0.1)
                continue
            
            consecutive_empty = 0
            
            # Process each record
            for record in records:
                try:
                    message = QueueMessage.from_bytes(record.value)
                    await self._process_message(message)
                    self._processed_count += 1
                except Exception as e:
                    self.logger.error(f"Error processing message at offset {record.offset}: {e}")
                    self._error_count += 1
                    # Continue processing - don't block on single errors
                
                offset = record.offset + 1
            
            # Commit offset periodically
            if time.time() - self._last_commit_time > self.config.commit_interval_ms / 1000:
                await self.upstream_queue.commit_offset(
                    self.consumer_group, self.upstream_topic, offset
                )
                self._last_commit_time = time.time()
        
        # Final commit
        if self.upstream_queue:
            await self.upstream_queue.commit_offset(
                self.consumer_group, self.upstream_topic, offset
            )
    
    async def _process_as_source(self) -> None:
        """Process as a source stage (no upstream)."""
        # For source operators, we need to call generate_splits
        # This is a simplified version - full implementation would handle pagination
        from solstice.core.models import Split
        
        splits = self.operator.generate_splits() if hasattr(self.operator, 'generate_splits') else []
        
        for split in splits:
            if not self._running:
                break
            
            try:
                # Process split
                output_payload = self.operator.process_split(split, None)
                
                if output_payload:
                    # Store payload in Ray object store
                    output_ref = ray.put(output_payload)
                    
                    # Create output message
                    message = QueueMessage(
                        message_id=f"{self.worker_id}_{self._processed_count}",
                        split_id=split.split_id,
                        data_ref=output_ref.hex(),  # Serialize ObjectRef
                        metadata={"source_stage": self.stage_id},
                    )
                    
                    # Produce to output queue
                    await self.output_queue.produce(
                        self.output_topic, message.to_bytes()
                    )
                
                self._processed_count += 1
                
            except Exception as e:
                self.logger.error(f"Error processing split {split.split_id}: {e}")
                self._error_count += 1
    
    async def _process_message(self, message: QueueMessage) -> None:
        """Process a single message."""
        from solstice.core.models import Split, SplitPayload
        
        # Reconstruct the data reference
        data_ref = ray.ObjectRef.from_hex(message.data_ref)
        
        # Fetch the actual payload
        payload: SplitPayload = ray.get(data_ref, timeout=self.config.processing_timeout_s)
        
        # Create split for processing
        split = Split(
            split_id=message.split_id,
            stage_id=self.stage_id,
            data_range={"message_id": message.message_id},
            parent_split_ids=[message.split_id],
        )
        
        # Process with operator
        output_payload = self.operator.process_split(split, payload)
        
        if output_payload:
            # Store output in Ray object store
            output_ref = ray.put(output_payload)
            
            # Create output message
            output_message = QueueMessage(
                message_id=f"{self.worker_id}_{self._processed_count}",
                split_id=f"{self.stage_id}_{message.split_id}",
                data_ref=output_ref.hex(),
                metadata={
                    "source_stage": self.stage_id,
                    "parent_message_id": message.message_id,
                },
            )
            
            # Produce to output queue
            await self.output_queue.produce(
                self.output_topic, output_message.to_bytes()
            )
    
    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.logger.info(f"Worker {self.worker_id} stopping")
        
        try:
            self.operator.close()
        except Exception as e:
            self.logger.error(f"Error closing operator: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
        }
