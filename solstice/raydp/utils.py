#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import atexit
import logging
import math
import glob
import re
import signal
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import ray

logger = logging.getLogger(__name__)

MEMORY_SIZE_UNITS = {"K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40}

# we use 4 bytes for block size, this means each block can contain
# 4294967296 records
BLOCK_SIZE_BIT = 32


def register_exit_handler(func):
    atexit.register(func)
    signal.signal(signal.SIGTERM, func)
    signal.signal(signal.SIGINT, func)


def random_split(df, weights, seed=None):
    """
    Random split the spark DataFrame or koalas DataFrame into given part
    :param df: the spark DataFrame or koalas DataFrame
    :param weights: list of doubles as weights with which to split the df.
                    Weights will be normalized if they don't sum up to 1.0.
    :param seed: The seed for sampling.
    """
    # convert to Spark DataFrame
    df, is_spark_df = convert_to_spark(df)
    splits = df.randomSplit(weights, seed)
    if is_spark_df:
        return splits
    else:
        # convert back to pandas on Spark DataFrame
        import pyspark.pandas as ps  # pylint: disable=C0415

        return [ps.DataFrame(split) for split in splits]


def _df_helper(df, spark_callback, spark_pandas_callback):
    try:
        import pyspark  # pylint: disable=C0415
    except Exception:
        pass
    else:
        if isinstance(df, pyspark.sql.DataFrame):
            return spark_callback(df)

    try:
        import pyspark.pandas as ps  # pylint: disable=C0415
    except Exception:
        pass
    else:
        if isinstance(df, ps.DataFrame):
            return spark_pandas_callback(df)

    raise Exception(
        f"The type: {type(df)} is not supported, only support "
        "pyspark.sql.DataFrame and pyspark.pandas.DataFrame"
    )


def df_type_check(df):
    """
    Check whether the df is spark DataFrame or koalas DataFrame.
    :return True for spark DataFrame or Koalas DataFrame.
    :raise Exception when it is neither spark DataFrame nor Koalas DataFrame.
    """
    return _df_helper(df, lambda d: True, lambda d: True)


def convert_to_spark(df):
    """
    Do nothing if the df is spark DataFrame, convert to spark DataFrame if it is
    koalas DataFrame. Raise Exception otherwise.
    :return: a pair of (converted df, whether it is spark DataFrame)
    """
    return _df_helper(df, lambda d: (d, True), lambda d: (d.to_spark(), False))


def parse_memory_size(memory_size: str) -> int:
    """
    Parse the human readable memory size into bytes.
    Adapt from: https://stackoverflow.com/a/60708339
    :param memory_size: human readable memory size
    :return: convert to int size
    """
    memory_size = memory_size.strip().upper()
    if re.search(r"B", memory_size):
        # discard "B"
        memory_size = re.sub(r"B", "", memory_size)

    try:
        return int(memory_size)
    except ValueError:
        pass

    global MEMORY_SIZE_UNITS
    if not re.search(r" ", memory_size):
        memory_size = re.sub(r"([KMGT]+)", r" \1", memory_size)
    number, unit_index = [item.strip() for item in memory_size.split()]
    return int(float(number) * MEMORY_SIZE_UNITS[unit_index])


def divide_blocks(
    blocks: List[int], world_size: int, shuffle: bool = False, shuffle_seed: int = None
) -> Dict[int, List[int]]:
    """
    Divide the blocks into world_size partitions, and return the divided block indexes for the
    given work_rank
    :param blocks: the blocks and each item is the given block size
    :param world_size: total world size
    :param shuffle: whether shuffle the blocks before divide
    :param shuffle_seed: the shuffle seed
    :return: a dict, the key is the world rank, and the value is a list of pair of block index
             and the samples selected in that block
    """
    import numpy as np

    if len(blocks) < world_size:
        raise Exception("do not have enough blocks to divide")

    results = {}

    # number of blocks per rank
    num_blocks_per_rank = int(math.ceil(len(blocks) * 1.0 / world_size))
    # number of samples per rank
    num_samples_per_rank = int(math.ceil(sum(blocks) * 1.0 / world_size))
    # total number of blocks
    total_num_blocks = num_blocks_per_rank * world_size
    # global block indexes
    global_indexes = list(range(len(blocks)))

    # add extra blocks to make it evenly divisible
    if len(global_indexes) != total_num_blocks:
        global_indexes += global_indexes[: (total_num_blocks - len(global_indexes))]

    assert len(global_indexes) == total_num_blocks

    if shuffle_seed:
        np.random.seed(shuffle_seed)
    else:
        np.random.seed(0)

    if shuffle:
        np.random.shuffle(global_indexes)

    def select(index: int, current_size: int, selected: List[Tuple[int, int]]) -> int:
        block_size = blocks[index]
        tmp = current_size + block_size
        if tmp < num_samples_per_rank:
            selected.append((index, block_size))
            current_size = tmp
        elif tmp >= num_samples_per_rank:
            selected.append((index, (num_samples_per_rank - current_size)))
            current_size = num_samples_per_rank
        return current_size

    for rank in range(world_size):
        indexes = global_indexes[rank:total_num_blocks:world_size]
        assert len(indexes) == num_blocks_per_rank

        samples_cur_rank = 0
        selected_indexes = []
        for i in indexes:
            samples_cur_rank = select(i, samples_cur_rank, selected_indexes)
            if samples_cur_rank == num_samples_per_rank:
                break

        while samples_cur_rank < num_samples_per_rank:
            index = np.random.choice(global_indexes, size=1)[0]
            samples_cur_rank = select(index, samples_cur_rank, selected_indexes)

        assert samples_cur_rank == num_samples_per_rank

        results[rank] = selected_indexes

    return results


def code_search_path() -> List[str]:
    import pyspark

    raydp_cp = os.path.abspath(os.path.join(os.path.abspath(__file__), "../jars/"))
    spark_home = os.environ.get("SPARK_HOME", os.path.dirname(pyspark.__file__))
    spark_jars_dir = os.path.abspath(os.path.join(spark_home, "jars/"))

    return [raydp_cp, spark_jars_dir]


def code_search_jars() -> List[str]:
    paths = code_search_path()
    jars = []
    for path in paths:
        jars.extend(glob.glob(os.path.join(path, "*.jar")))
    return jars


@dataclass
class ExecutorConfig:
    """Auto-inferred executor configuration based on Ray cluster resources."""

    num_executors: int
    executor_cores: int
    executor_memory_gb: int
    driver_memory_gb: int

    @property
    def executor_memory(self) -> str:
        """Return executor memory as a Spark-compatible string (e.g., '4g')."""
        return f"{self.executor_memory_gb}g"

    @property
    def driver_memory(self) -> str:
        """Return driver memory as a Spark-compatible string (e.g., '2g')."""
        return f"{self.driver_memory_gb}g"


def auto_infer_executor_config(
    cpu_overhead_per_executor: int = 1,
    memory_overhead_gb_per_executor: int = 2,
    min_executor_cores: int = 1,
    min_executor_memory_gb: int = 4,
    min_driver_memory_gb: int = 2,
) -> ExecutorConfig:
    """
    Automatically infer Spark executor configuration based on Ray cluster resources.

    This function analyzes the Ray cluster topology to determine optimal Spark executor
    settings. It distinguishes between head and worker nodes, using worker nodes for
    executors and the head node for the driver.

    Args:
        cpu_overhead_per_executor: Number of CPUs to reserve per executor for system overhead.
        memory_overhead_gb_per_executor: GB of memory to reserve per executor for overhead.
        min_executor_cores: Minimum number of cores per executor.
        min_executor_memory_gb: Minimum memory (GB) per executor.
        min_driver_memory_gb: Minimum memory (GB) for the driver.
        logger: Optional logger for debug output.

    Returns:
        ExecutorConfig with inferred settings.

    Example:
        >>> config = auto_infer_executor_config()
        >>> spark = init_spark(
        ...     app_name="my_app",
        ...     num_executors=config.num_executors,
        ...     executor_cores=config.executor_cores,
        ...     executor_memory=config.executor_memory,
        ... )
    """
    if not ray.is_initialized():
        raise RuntimeError(
            "Ray must be initialized before calling auto_infer_executor_config. "
            "Call ray.init() first."
        )

    # Get per-node resources using ray.nodes() for precise calculation
    nodes = ray.nodes()

    head_cpus = 0
    head_memory_gb = 0
    worker_nodes: List[Dict[str, int]] = []

    for node in nodes:
        if not node.get("Alive", False):
            continue
        node_resources = node.get("Resources", {})
        node_cpus = int(node_resources.get("CPU", 0))
        node_memory_gb = int(node_resources.get("memory", 0) / (1024 * 1024 * 1024))

        # Head node has 'node:__internal_head__' resource
        if "node:__internal_head__" in node_resources:
            head_cpus = node_cpus
            head_memory_gb = node_memory_gb
            logger.debug(f"Head node: {node_cpus} CPUs, {node_memory_gb}GB memory")
        else:
            worker_nodes.append({"cpus": node_cpus, "memory_gb": node_memory_gb})
            logger.debug(f"Worker node: {node_cpus} CPUs, {node_memory_gb}GB memory")

    num_workers = len(worker_nodes)
    if num_workers == 0:
        # Fallback: treat all resources as single executor (local mode)
        total_resources = ray.cluster_resources()
        executor_cores = max(
            int(total_resources.get("CPU", 4)) - cpu_overhead_per_executor,
            min_executor_cores,
        )
        executor_memory_gb = max(
            int(total_resources.get("memory", 8 * 1024**3) / 1024**3)
            - memory_overhead_gb_per_executor,
            min_executor_memory_gb,
        )
        num_executors = 1
        driver_memory_gb = min_driver_memory_gb
        logger.info(
            f"Local mode detected: 1 executor with {executor_cores} cores, "
            f"{executor_memory_gb}GB memory"
        )
    else:
        # Use minimum worker resources to ensure all executors can be scheduled
        min_worker_cpus = min(w["cpus"] for w in worker_nodes)
        min_worker_memory_gb = min(w["memory_gb"] for w in worker_nodes)

        # Executor config: leave overhead for system processes per worker
        executor_cores = max(
            min_worker_cpus - cpu_overhead_per_executor,
            min_executor_cores,
        )
        executor_memory_gb = max(
            min_worker_memory_gb - memory_overhead_gb_per_executor,
            min_executor_memory_gb,
        )
        num_executors = num_workers

        # Driver on head: use half of head memory
        driver_memory_gb = max(head_memory_gb // 2, min_driver_memory_gb)

        logger.info(
            f"Cluster: {num_workers} workers, head={head_cpus}CPU/{head_memory_gb}GB"
        )

    logger.info(
        f"Auto-configured: {num_executors} executors, {executor_cores} cores each, "
        f"{executor_memory_gb}g memory, driver {driver_memory_gb}g"
    )

    return ExecutorConfig(
        num_executors=num_executors,
        executor_cores=executor_cores,
        executor_memory_gb=executor_memory_gb,
        driver_memory_gb=driver_memory_gb,
    )
