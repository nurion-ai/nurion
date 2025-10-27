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

import glob
import os
import platform
import pyspark
from typing import Dict

import ray
from fusionflowkit.datahub_client import (
    create_spark_subtask,
    get_ray_job_id,
    get_task_id,
)
import ray.serve
from pyspark.sql.session import SparkSession

from .ray_cluster_master import RAYDP_SPARK_MASTER_SUFFIX, RayDPSparkMaster

DRIVER_CP_KEY = "spark.driver.extraClassPath"
DRIVER_JAVA_OPTIONS_KEY = "spark.driver.extraJavaOptions"


class SparkCluster:
    def __init__(
        self,
        app_name,
        configs,
        logging_level,
    ):
        self._app_name = app_name
        self._configs = configs
        self._logging_level = logging_level
        # self._logger = logging.getLogger(__file__)
        self._prepare_spark_configs()
        self._setup_master(self._get_master_resources(self._configs))
        self._spark_session: SparkSession = None

    def _setup_master(self, resources: Dict[str, float]):
        spark_master_name = self._app_name + RAYDP_SPARK_MASTER_SUFFIX

        if resources:
            num_cpu = 1
            if "CPU" in resources:
                num_cpu = resources["CPU"]
                resources.pop("CPU", None)
            self._spark_master_handle = RayDPSparkMaster.options(
                name=spark_master_name,
                num_cpus=num_cpu,
                resources=resources,
            ).remote(self._app_name, self._configs, logging_level=self._logging_level)
        else:
            self._spark_master_handle = RayDPSparkMaster.options(
                name=spark_master_name,
            ).remote(self._app_name, self._configs, logging_level=self._logging_level)

        ray.get(self._spark_master_handle.start_up.remote(resources))

    def _get_master_resources(self, configs: Dict[str, str]) -> Dict[str, float]:
        resources = {}
        spark_master_actor_resource_prefix = "spark.ray.master.actor.resource."

        def get_master_actor_resource(
            key_prefix: str, resource: Dict[str, float]
        ) -> Dict[str, float]:
            for key in configs:
                if key.startswith(key_prefix):
                    resource_name = key[len(key_prefix) :]
                    resource[resource_name] = float(configs[key])
            return resource

        resources = get_master_actor_resource(
            spark_master_actor_resource_prefix, resources
        )

        return resources

    def get_cluster_url(self) -> str:
        return ray.get(self._spark_master_handle.get_master_url.remote())

    def _prepare_spark_configs(self):
        if self._configs is None:
            self._configs = {}
        if platform.system() != "Darwin":
            driver_node_ip = ray.util.get_node_ip_address()
            if "spark.driver.host" not in self._configs:
                self._configs["spark.driver.host"] = str(driver_node_ip)
                self._configs["spark.driver.bindAddress"] = str(driver_node_ip)

        raydp_cp = os.path.abspath(
            os.path.join(os.path.abspath(__file__), "../../jars/*")
        )
        ray_cp = os.path.abspath(os.path.join(os.path.dirname(ray.__file__), "jars/*"))
        spark_home = os.environ.get("SPARK_HOME", os.path.dirname(pyspark.__file__))
        spark_jars_dir = os.path.abspath(os.path.join(spark_home, "jars/*"))

        raydp_jars = glob.glob(raydp_cp)
        driver_cp = ":".join(raydp_jars + [spark_jars_dir] + glob.glob(ray_cp))
        if DRIVER_CP_KEY in self._configs:
            self._configs[DRIVER_CP_KEY] += (
                self._configs[DRIVER_CP_KEY] + ":" + driver_cp
            )
        else:
            self._configs[DRIVER_CP_KEY] = driver_cp

        extra_driver_options = f"-Dray.job.id={get_ray_job_id()}"
        if DRIVER_JAVA_OPTIONS_KEY in self._configs:
            self._configs[DRIVER_JAVA_OPTIONS_KEY] += " " + extra_driver_options
        else:
            self._configs[DRIVER_JAVA_OPTIONS_KEY] = extra_driver_options

        python_path_candidates = self._configs.get(
            "spark.executorEnv.PYTHONPATH", ""
        ).split(":")
        for k, v in os.environ.items():
            if k == "PYTHONPATH":
                python_path_candidates.append(v)
            if k == "VIRTUAL_ENV":
                python_path_candidates += glob.glob(f"{v}/lib/python*/site-packages")
                self._configs["spark.pyspark.python"] = f"{v}/bin/python"
        self._configs["spark.executorEnv.PYTHONPATH"] = ":".join(
            [x for x in python_path_candidates if len(x) > 0]
        )

    def get_spark_session(self) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session
        spark_builder = SparkSession.builder
        for k, v in self._configs.items():
            spark_builder.config(k, v)
        spark_builder.enableHiveSupport()
        app_id = ray.get(self._spark_master_handle.get_app_id.remote())
        task_id = get_task_id()
        if task_id:
            spark_builder.config("spark.ui.proxyRedirectUri", "/")
            spark_builder.config("spark.ui.proxyBase", f"/spark/{task_id}/{app_id}")
        self._spark_session = (
            spark_builder.appName(self._app_name)
            .master(self.get_cluster_url())
            .getOrCreate()
        )

        # self._logger.info(f"Spark UI: {self._spark_session.sparkContext.uiWebUrl}")
        print(f"Spark UI: {self._spark_session.sparkContext.uiWebUrl}")
        self._spark_session.sparkContext.setLogLevel(self._logging_level)
        if task_id:
            try:
                create_spark_subtask(
                    task_id=task_id,
                    app_name=self._app_name,
                    app_id=app_id,
                    webui_url=self._spark_session.sparkContext.uiWebUrl,
                )
            except Exception as e:
                print(f"Failed to create spark subtask: {e}")
                # self._logger.warning(f"Failed to create spark subtask: {e}")
        return self._spark_session

    def stop(self, cleanup_data):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
        if self._spark_master_handle is not None:
            self._spark_master_handle.stop.remote(cleanup_data)
            if cleanup_data:
                self._spark_master_handle = None
