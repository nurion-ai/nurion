"""Spark source operator and source master for reading data via raydp."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, List, Optional, TYPE_CHECKING

import pyarrow as pa
import ray

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SourceOperator, OperatorConfig
from solstice.operators.sources.source import SourceMaster, SourceConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from solstice.core.stage import Stage


# Type alias for the DataFrame factory function
DataFrameFactory = Callable[["SparkSession"], "DataFrame"]


@dataclass
class SparkSourceConfig(OperatorConfig):
    """Unified configuration for Spark source (both operator and master).

    Contains raydp init_spark parameters and a DataFrame factory function.
    The operator reads Arrow data from Ray object store (ObjectRefs),
    while the master uses the Spark config to initialize Spark and create splits.

    Attributes:
        app_name: Spark application name
        num_executors: Number of Spark executors
        executor_cores: Number of cores per executor
        executor_memory: Memory per executor (e.g., "1g", "2g")
        spark_configs: Additional Spark configurations
        dataframe_fn: Function that takes SparkSession and returns DataFrame.
                      This is the main way to define your data source.
        parallelism: Number of partitions for the output data

    Example:
        >>> config = SparkSourceConfig(
        ...     app_name="my-app",
        ...     num_executors=2,
        ...     dataframe_fn=lambda spark: spark.read.json("/data/events.json"),
        ... )

        >>> # Or with SQL:
        >>> config = SparkSourceConfig(
        ...     dataframe_fn=lambda spark: spark.sql("SELECT * FROM my_table"),
        ... )

        >>> # Or with complex logic:
        >>> def load_data(spark):
        ...     df1 = spark.read.parquet("/data/users")
        ...     df2 = spark.read.parquet("/data/orders")
        ...     return df1.join(df2, "user_id")
        >>> config = SparkSourceConfig(dataframe_fn=load_data)
    """

    # raydp init_spark parameters
    app_name: str = "solstice-spark-source"
    num_executors: int = 1
    executor_cores: int = 2
    executor_memory: str = "1g"
    spark_configs: Dict[str, str] = field(default_factory=dict)

    # DataFrame factory function: (SparkSession) -> DataFrame
    dataframe_fn: Optional[DataFrameFactory] = None

    # Output configuration
    parallelism: Optional[int] = None
    
    # SourceConfig fields for master
    tansu_storage_url: str = "memory://"
    """Tansu storage URL (s3://, sqlite://, memory://)."""


class SparkSource(SourceOperator):
    """Source operator for reading Arrow data from Ray object store.

    This operator reads Arrow data from ObjectRefs that were persisted
    by SparkSourceMaster using raydp.
    """

    def __init__(
        self,
        config: SparkSourceConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)

    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read Arrow data from Ray object store.

        The split contains:
            - object_ref: ObjectRef to the Arrow data in object store
            - block_size: Number of records in this block
        """
        object_ref = split.data_range.get("object_ref")
        if object_ref is None:
            raise ValueError("Split missing 'object_ref' for SparkSource")

        # Get Arrow data from object store
        arrow_data = ray.get(object_ref)

        if arrow_data is None:
            return None

        # Handle different data types from object store
        if isinstance(arrow_data, pa.Table):
            arrow_table = arrow_data
        elif isinstance(arrow_data, pa.RecordBatch):
            arrow_table = pa.Table.from_batches([arrow_data])
        elif isinstance(arrow_data, bytes):
            # Arrow IPC format (from raydp) - deserialize using IPC reader
            import pyarrow.ipc as ipc
            import io

            reader = ipc.open_stream(io.BytesIO(arrow_data))
            arrow_table = reader.read_all()
        else:
            raise ValueError(f"Unsupported data type from object store: {type(arrow_data)}")

        if arrow_table.num_rows == 0:
            return None

        return SplitPayload.from_arrow(
            arrow_table,
            split_id=split.split_id,
        )

    def close(self) -> None:
        """Clean up resources."""
        pass


# Set operator_class after class definition
SparkSourceConfig.operator_class = SparkSource


class SparkSourceMaster(SourceMaster):
    """Source master for Spark that handles split planning.

    Initializes Spark via raydp, loads data using the dataframe_fn,
    persists to Ray object store, then yields splits containing ObjectRefs.
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        **kwargs,
    ):
        # Get config from stage.operator_config
        operator_cfg = stage.operator_config
        if not isinstance(operator_cfg, SparkSourceConfig):
            raise TypeError(
                f"SparkSourceMaster requires SparkSourceConfig, "
                f"got {type(operator_cfg)}"
            )
        
        super().__init__(job_id, stage, **kwargs)
        
        self._config = operator_cfg
        self._spark = None
        self._spark_initialized = False

    def _init_spark(self):
        """Initialize Spark session via raydp."""
        if self._spark_initialized:
            return

        import raydp

        # Merge default configs with user configs
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

    def _get_dataframe(self):
        """Get DataFrame by calling the dataframe_fn with SparkSession."""
        if self._config.dataframe_fn is None:
            raise ValueError(
                "dataframe_fn must be provided in SparkSourceConfig. "
                "Example: dataframe_fn=lambda spark: spark.read.json('/path/to/data')"
            )

        self.logger.info("Calling dataframe_fn to load data")
        return self._config.dataframe_fn(self._spark)

    def plan_splits(self) -> Iterator[Split]:
        """Initialize Spark, load data, persist to object store, and yield splits.

        Uses raydp's _save_spark_df_to_object_store to efficiently transfer
        Spark data to Ray object store as Arrow blocks.
        """
        from raydp.spark.dataset import _save_spark_df_to_object_store, get_raydp_master_owner

        # Initialize Spark
        self._init_spark()

        # Get DataFrame
        df = self._get_dataframe()

        # Repartition if parallelism is specified
        if self._config.parallelism is not None:
            num_partitions = df.rdd.getNumPartitions()
            if num_partitions != self._config.parallelism:
                df = df.repartition(self._config.parallelism)

        # Get the owner for object lifetime management
        owner = get_raydp_master_owner(self._spark)

        # Save DataFrame to object store, returns list of ObjectRefs and block sizes
        blocks, block_sizes = _save_spark_df_to_object_store(
            df,
            use_batch=False,  # Return Arrow tables, not batches
            owner=owner,
        )

        self.logger.info(
            f"Persisted Spark DataFrame to object store: "
            f"{len(blocks)} blocks, {sum(block_sizes)} total records"
        )

        # Yield splits containing ObjectRefs
        for idx, (block_ref, block_size) in enumerate(zip(blocks, block_sizes)):
            yield Split(
                split_id=f"{self.stage.stage_id}_split_{idx}",
                stage_id=self.stage.stage_id,
                data_range={
                    "object_ref": block_ref,
                    "block_size": block_size,
                    "block_index": idx,
                },
            )

    def stop(self) -> None:
        """Stop the source master and cleanup Spark.
        
        This is a synchronous method for compatibility with tests.
        For async usage, call stop_async().
        """
        self._stop_spark()
    
    async def stop_async(self) -> None:
        """Stop the source master and cleanup Spark (async version)."""
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


# Set master_class after class definition
SparkSourceConfig.master_class = SparkSourceMaster
