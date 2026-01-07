# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Spark Source V2: Direct Queue Integration.

This module provides SparkSourceV2, an optimized Spark source that has JVM-side
executors write directly to Ray Object Store and output_queue.

Key improvements over V1:
- Eliminates Python-side plan_splits() iteration
- Eliminates source_queue and operator read step
- JVM writes directly to output_queue with managed ObjectRef lifetime
- Single serialization path (Spark → Arrow → Object Store → output_queue)

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    SparkSourceV2Master                       │
    │  (Python - control plane)                                    │
    │                                                              │
    │  1. Create output_queue                                      │
    │  2. Call JVM with (storeActorName, queueEndpoint)           │
    │  3. Wait for JVM to complete                                 │
    │  4. Update metrics, notify downstream                        │
    └──────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    JVM (Spark Executor)                      │
    │                                                              │
    │  1. Ray.put(arrowBytes, owner=storeActor)  ← managed lifetime│
    │  2. Kafka produce to output_queue          ← direct write    │
    │     payload_key = "_v2ref:{object_id_b64}"                   │
    └─────────────────────────────────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────┐
    │               Downstream Stage Workers                       │
    │                                                              │
    │  1. Consume from output_queue                                │
    │  2. payload_store.get(payload_key)                          │
    │     → detects _v2ref: prefix                                 │
    │     → reconstructs ObjectRef from ID                         │
    │     → ray.get() → auto-convert Arrow to SplitPayload        │
    └─────────────────────────────────────────────────────────────┘

Usage:
    from solstice.operators.sources.sparkv2 import SparkSourceV2Config

    config = SparkSourceV2Config(
        dataframe_fn=lambda spark: spark.read.parquet("/data"),
        num_executors=2,
    )
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, Optional, TYPE_CHECKING

from solstice.core.models import Split
from solstice.core.operator import OperatorConfig
from solstice.core.stage_master import StageMaster, StageConfig
from solstice.queue import QueueType
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from solstice.core.stage import Stage
    from solstice.core.split_payload_store import SplitPayloadStore


# Type alias for the DataFrame factory function
DataFrameFactory = Callable[["SparkSession"], "DataFrame"]


@dataclass
class SparkSourceV2Config(OperatorConfig):
    """Configuration for Spark Source V2.

    V2 bypasses source_queue and operator by having JVM write directly
    to output_queue with ObjectRef ID embedded in payload_key.

    Attributes:
        app_name: Spark application name
        num_executors: Number of Spark executors
        executor_cores: Number of cores per executor
        executor_memory: Memory per executor (e.g., "1g", "2g")
        spark_configs: Additional Spark configurations
        dataframe_fn: Function that takes SparkSession and returns DataFrame
        parallelism: Number of partitions for the output data
    """

    # Spark configuration
    app_name: str = "solstice-spark-v2"
    num_executors: int = 1
    executor_cores: int = 2
    executor_memory: str = "1g"
    spark_configs: Dict[str, str] = field(default_factory=dict)

    # DataFrame factory function: (SparkSession) -> DataFrame
    dataframe_fn: Optional[DataFrameFactory] = None

    # Output configuration
    parallelism: Optional[int] = None


class SparkSourceV2Master(StageMaster):
    """Spark Source V2: JVM writes directly to output_queue.

    This is a simplified source master that:
    - Does NOT use source_queue (JVM writes directly to output_queue)
    - Does NOT need operators (data is already in Object Store)
    - Only acts as control plane for Spark initialization and metrics

    The downstream stage workers:
    - Consume from output_queue
    - Call payload_store.get(payload_key) which handles _v2ref: prefix
    - Receive SplitPayload directly (auto-converted from Arrow)
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        payload_store: "SplitPayloadStore",
        **kwargs,
    ):
        # Get config from stage.operator_config
        operator_cfg = stage.operator_config
        if not isinstance(operator_cfg, SparkSourceV2Config):
            raise TypeError(
                f"SparkSourceV2Master requires SparkSourceV2Config, got {type(operator_cfg)}"
            )

        # Create stage config for queue setup
        # upstream_endpoint/topic are None for source stages
        stage_config = StageConfig(
            queue_type=QueueType.TANSU,
            min_workers=0,  # No workers needed - JVM writes directly
            max_workers=0,
            upstream_endpoint=None,
            upstream_topic=None,
        )

        super().__init__(
            job_id=job_id,
            stage=stage,
            config=stage_config,
            payload_store=payload_store,
        )

        self._config = operator_cfg
        self._spark = None
        self._spark_initialized = False
        self._splits_produced = 0

        # Override logger
        self.logger = create_ray_logger(f"SparkSourceV2Master-{self.stage_id}")

    async def start(self) -> None:
        """Start the source master.

        V2 simplified flow:
        1. Create output_queue
        2. Execute Spark write (JVM writes directly to output_queue)
        3. Mark as complete
        """
        if self._running:
            return

        self.logger.info(f"Starting SparkSourceV2 {self.stage_id}")
        self._start_time = time.time()
        self._running = True

        # 1. Create output_queue (JVM will write directly to this)
        self._output_queue = await self._create_queue()

        # 2. Execute Spark write (JVM writes to Object Store + output_queue)
        splits_count = await self._execute_spark_write()
        self._splits_produced = splits_count

        self.logger.info(
            f"SparkSourceV2 {self.stage_id} completed: {splits_count} splits "
            f"written directly to output_queue"
        )

        # V2 is complete immediately - no workers to spawn
        # Downstream stage will consume from our output_queue

    async def _execute_spark_write(self) -> int:
        """Execute Spark write via JVM with backpressure awareness.

        JVM writes directly to output_queue:
        1. Ray.put(arrowBytes, owner=storeActor) - managed lifetime
        2. Kafka produce to output_queue with payload_key = "_v2ref:{id}"

        Note: Current implementation writes all data at once. For true backpressure
        support, JVM-side streaming write with periodic backpressure checks is needed.
        This is a TODO for future enhancement.

        Returns:
            Number of splits written
        """
        import raydp

        # Check backpressure before starting write
        if await self._check_backpressure_before_produce():
            self.logger.warning(
                f"Backpressure detected before Spark write for {self.stage_id}. "
                f"Proceeding anyway (current implementation doesn't support streaming write)."
            )

        # Initialize Spark
        spark_configs = {
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            **self._config.spark_configs,
        }

        self._spark = raydp.init_spark(
            app_name=self._config.app_name,
            num_executors=self._config.num_executors,
            executor_cores=self._config.executor_cores,
            executor_memory=self._config.executor_memory,
            configs=spark_configs,
        )
        self._spark_initialized = True
        self.logger.info(f"Initialized Spark session: {self._config.app_name}")

        # Get DataFrame
        if self._config.dataframe_fn is None:
            raise ValueError(
                "dataframe_fn must be provided in SparkSourceV2Config. "
                "Example: dataframe_fn=lambda spark: spark.read.json('/path/to/data')"
            )

        df = self._config.dataframe_fn(self._spark)

        # Repartition if parallelism is specified
        if self._config.parallelism is not None:
            num_partitions = df.rdd.getNumPartitions()
            if num_partitions != self._config.parallelism:
                df = df.repartition(self._config.parallelism)

        # Output queue connection info
        queue_bootstrap = f"{self._output_endpoint.host}:{self._output_endpoint.port}"
        queue_topic = self._output_topic

        self.logger.info(f"JVM writing directly to output_queue: {queue_bootstrap}/{queue_topic}")

        # Call JVM method to write Arrow data directly to output_queue
        # TODO: For true backpressure support, this should be a streaming write
        # that periodically checks backpressure and pauses/resumes accordingly.
        # This requires JVM-side changes to support incremental writes.
        jvm = df.sql_ctx.sparkSession.sparkContext._jvm
        writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(df._jdf)

        count = writer.saveToStoreAndQueue(
            False,  # useBatch
            queue_bootstrap,
            queue_topic,
            self.stage_id,
        )

        self.logger.info(f"JVM write completed: {count} splits to output_queue")

        # Check backpressure after write
        if await self._check_backpressure_before_produce():
            self.logger.warning(
                f"Backpressure detected after Spark write for {self.stage_id}. "
                f"Downstream may be overwhelmed."
            )

        return count

    def plan_splits(self) -> Iterator[Split]:
        """Not used in V2 - JVM writes directly to output_queue."""
        raise NotImplementedError(
            "V2 does not use plan_splits(). JVM writes directly to output_queue."
        )

    async def stop(self) -> None:
        """Stop the source master and cleanup Spark."""
        await super().stop()
        self._stop_spark()

    def _stop_spark(self) -> None:
        """Internal method to stop Spark session."""
        if self._spark_initialized:
            import raydp

            raydp.stop_spark()
            self._spark = None
            self._spark_initialized = False
            self.logger.info("Stopped Spark session")

    def get_status(self):
        """Get current source status."""
        status = super().get_status()
        status.metrics["splits_produced"] = self._splits_produced
        return status


# Set master_class after class definition
SparkSourceV2Config.master_class = SparkSourceV2Master
