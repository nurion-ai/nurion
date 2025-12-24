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

import atexit
import logging
from contextlib import ContextDecorator
from threading import RLock
from typing import Dict, Union, Optional

import ray
from pyspark.sql import SparkSession

from raydp.spark import SparkCluster
from raydp.utils import auto_infer_executor_config


class _SparkContext(ContextDecorator):
    """A class used to create the Spark cluster and get the Spark session.

    :param app_name the Spark application name
    :param configs the extra Spark configs need to set
    """

    def __init__(
        self,
        app_name: str,
        configs: Dict[str, str],
        logging_level: str = "warn",
    ):
        self._app_name = app_name
        self._logging_level = logging_level

        self._configs = configs

        self._spark_cluster: Optional[SparkCluster] = None
        self._spark_session: Optional[SparkSession] = None

    def _get_or_create_spark_cluster(self) -> SparkCluster:
        if self._spark_cluster is not None:
            return self._spark_cluster
        py4j_logger = logging.getLogger("py4j")
        py4j_logger.setLevel(logging.WARNING)
        self._spark_cluster = SparkCluster(
            self._app_name,
            self._configs,
            self._logging_level,
        )
        return self._spark_cluster

    def get_or_create_session(self):
        if self._spark_session is not None:
            return self._spark_session
        spark_cluster = self._get_or_create_spark_cluster()
        self._spark_session = spark_cluster.get_spark_session()

        return self._spark_session

    def start_connect_server(self) -> int:
        if self._spark_session is None:
            raise Exception(
                "The Spark cluster has not been created, please call get_or_create_session first."
            )
        return self._spark_session._jvm.org.apache.spark.sql.connect.ConnectServer.start()

    def stop(self, cleanup_data=True):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
        if self._spark_cluster is not None:
            self._spark_cluster.stop(cleanup_data)
            if cleanup_data:
                self._spark_cluster = None
        if self._configs is not None:
            self._configs = None

    def __enter__(self):
        self.get_or_create_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


_spark_context_lock = RLock()
_global_spark_context: _SparkContext = None


def init_spark(
    app_name: str,
    executor_cores: Optional[int] = None,
    executor_memory: Optional[Union[str, int]] = None,
    num_executors: Optional[int] = None,
    configs: Optional[Dict[str, str]] = None,
    log_to_driver: bool = False,
    logging_level: str = "warn",
    dynamic_allocation: bool = False,
    min_executors: Optional[int] = None,
    max_executors: Optional[int] = None,
    auto_configure: bool = False,
) -> SparkSession:
    """
    Init a Spark cluster with given requirements.

    :param app_name: The application name.
    :param executor_cores: the number of CPU cores for each executor. If None and
                           auto_configure=True, will be inferred from cluster resources.
    :param executor_memory: the memory size for each executor, both support bytes or human
                            readable string. If None and auto_configure=True, will be
                            inferred from cluster resources.
    :param num_executors: number of executor requests. If None and auto_configure=True,
                          will be inferred from cluster resources.
    :param configs: the extra Spark config need to set
    :param log_to_driver: whether to log the Spark logs to the driver, default is False,
                          set it to True when debugging
    :param dynamic_allocation: whether to enable Spark dynamic allocation
    :param min_executors: minimum number of executors for dynamic allocation
    :param max_executors: maximum number of executors for dynamic allocation
    :param auto_configure: if True and executor_cores/executor_memory/num_executors are not
                           provided, automatically infer from Ray cluster resources.
    :return: return the SparkSession
    """
    logger = logging.getLogger(__name__)

    if not ray.is_initialized():
        # ray has not initialized, init local
        ray.init(log_to_driver=log_to_driver, logging_level=logging_level)

    # Defensive copy to avoid mutating caller's dict
    _configs = {} if configs is None else configs.copy()

    # Auto-configure executor settings if requested and not explicitly provided
    if auto_configure:
        inferred_config = auto_infer_executor_config()

        if executor_cores is None:
            executor_cores = inferred_config.executor_cores
            logger.info(f"Auto-configured executor_cores: {executor_cores}")

        if executor_memory is None:
            executor_memory = inferred_config.executor_memory
            logger.info(f"Auto-configured executor_memory: {executor_memory}")

        if num_executors is None and not dynamic_allocation:
            num_executors = inferred_config.num_executors
            logger.info(f"Auto-configured num_executors: {num_executors}")

        # Also set driver memory if not already in configs
        if "spark.driver.memory" not in _configs:
            _configs["spark.driver.memory"] = inferred_config.driver_memory
            logger.info(f"Auto-configured driver_memory: {inferred_config.driver_memory}")

    # Validate required parameters
    if executor_cores is None:
        raise ValueError(
            "executor_cores is required. Either provide it explicitly or set auto_configure=True."
        )
    if executor_memory is None:
        raise ValueError(
            "executor_memory is required. Either provide it explicitly or set auto_configure=True."
        )

    with _spark_context_lock:
        global _global_spark_context
        if dynamic_allocation:
            _configs["spark.dynamicAllocation.enabled"] = "true"
            assert min_executors is not None, (
                "min_executors is required when dynamic_allocation is enabled"
            )
            assert max_executors is not None, (
                "max_executors is required when dynamic_allocation is enabled"
            )
            _configs["spark.dynamicAllocation.minExecutors"] = str(min_executors)
            _configs["spark.dynamicAllocation.maxExecutors"] = str(max_executors)
            _configs["spark.executor.instances"] = str(min_executors)
        else:
            if num_executors is None:
                raise ValueError(
                    "num_executors is required when dynamic_allocation is disabled. "
                    "Either provide it explicitly or set auto_configure=True."
                )
            _configs["spark.dynamicAllocation.enabled"] = "false"
            _configs["spark.executor.instances"] = str(num_executors)
        _configs["spark.executor.cores"] = str(executor_cores)
        _configs["spark.executor.memory"] = str(executor_memory)

        if _global_spark_context is None:
            try:
                _global_spark_context = _SparkContext(
                    app_name,
                    _configs,
                    logging_level,
                )
                return _global_spark_context.get_or_create_session()
            except:
                if _global_spark_context is not None:
                    _global_spark_context.stop()
                _global_spark_context = None
                raise
        else:
            raise Exception("The spark environment has inited.")


def start_connect_server() -> int:
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is not None:
            port = _global_spark_context.start_connect_server()
            if port < 0:
                raise Exception(
                    "The spark connect server start failed, can not find available port, please check the spark logs."
                )
            return port
        raise Exception("The spark environment has not inited, please call init_spark first.")


def stop_spark(cleanup_data=True):
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is not None:
            _global_spark_context.stop(cleanup_data)
            if cleanup_data:
                _global_spark_context = None


atexit.register(stop_spark)
