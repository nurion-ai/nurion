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

import json
import logging

import ray
import ray.cross_language
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
)

from raydp.utils import code_search_path

from .ray_pyworker import PyWorker

RAYDP_SPARK_MASTER_SUFFIX = "_SPARK_MASTER"


@ray.remote
class RayDPSparkMaster:
    def __init__(self, app_name, configs, logging_level: str):
        self._logger = logging.getLogger(__file__)

        self._app_name = app_name
        self._ray_java_master = None
        self._started_up = False
        self._configs = configs
        self._logging_level = logging_level
        self._objects = {}

    def start_up(self, resources=None):
        if self._started_up:
            self._logger.warning(
                "The RayClusterMaster has started already. Do not call it twice"
            )
            return
        ray_app_master_class = ray.cross_language.java_actor_class(
            "org.apache.spark.deploy.raydp.RayAppMaster",
            # {
            #     "runtime_env": {
            #         "java_executable": f"java -Dray.logging.level={self._logging_level} -cp {':'.join([p + '/*' for p in code_search_path()])}",
            #     },
            # },
        )
        self._logger.info(f"Start the RayClusterMaster with configs: {self._configs}")
        self._ray_java_master = ray_app_master_class.options(
            resources=resources
        ).remote()
        self._logger.info("The RayClusterMaster has started")
        self._started_up = True

    def get_app_id(self) -> str:
        assert self._started_up
        return f"raydp-{self._ray_java_master._actor_id.hex()}"

    def get_master_url(self) -> str:
        assert self._started_up
        url = ray.get(self._ray_java_master.getMasterUrl.remote())
        self._logger.info(f"The master url is {url}")
        return url

    def create_pyworker(self, worker_id: str, node_id: str, env_vars: str) -> str:
        self._logger.info(
            f"Create a PyWorker with node_id: {node_id}, env_vars: {env_vars}, runtime_env: {ray.get_runtime_context().namespace}"
        )
        envs = json.loads(env_vars)
        pg_name = f"raydp-executor-{self._app_name}-{worker_id}-pg"
        # get placement group by name
        pg = ray.util.get_placement_group(pg_name)
        worker = PyWorker.options(
            runtime_env={
                "env_vars": envs,
            },
            max_concurrency=2,
            scheduling_strategy=PlacementGroupSchedulingStrategy(pg),
        ).remote()
        ray.get(worker.heartbeat.remote())
        return worker

    def add_objects(self, timestamp, objects):
        self._objects[timestamp] = objects

    def get_object(self, timestamp, idx):
        return self._objects[timestamp][idx]

    def get_ray_address(self):
        return ray.worker.global_worker.node.address

    def stop(self, cleanup_data):
        self._started_up = False
        if cleanup_data:
            ray.actor.exit_actor()
