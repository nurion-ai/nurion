/*
 * Copyright 2025 nurion team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

object RayDPConfigs {
  val SPARK_EXECUTOR_ACTOR_RESOURCE_PREFIX = "spark.ray.executor.actor.resource"
  val SPARK_EXECUTOR_SCHEDULING_STRATEGY = "spark.ray.executor.scheduling.strategy"
  val SPARK_EXECUTOR_SCHEDULING_STRATEGY_PARAMS_PREFIX = "spark.ray.executor.scheduling.strategy.params"
  val SPARK_EXECUTOR_WORKING_DIR = "spark.ray.executor.working-dir"
  val SPARK_LOGGER_PREFIX = "spark.ray.logger."
}
