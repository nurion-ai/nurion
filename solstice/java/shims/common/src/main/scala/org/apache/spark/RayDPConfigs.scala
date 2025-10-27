package org.apache.spark

object RayDPConfigs {
  val SPARK_EXECUTOR_ACTOR_RESOURCE_PREFIX = "spark.ray.executor.actor.resource"
  val SPARK_EXECUTOR_SCHEDULING_STRATEGY = "spark.ray.executor.scheduling.strategy"
  val SPARK_EXECUTOR_SCHEDULING_STRATEGY_PARAMS_PREFIX = "spark.ray.executor.scheduling.strategy.params"
  val SPARK_EXECUTOR_WORKING_DIR = "spark.ray.executor.working-dir"
  val SPARK_LOGGER_PREFIX = "spark.ray.logger."
}
