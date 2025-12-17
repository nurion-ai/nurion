/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.raydp

import io.ray.api.Ray
import io.ray.api.placementgroup.PlacementGroup
import org.apache.spark.{SparkConf, RayDPConfigs}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Python.PYTHON_USE_DAEMON
import org.apache.spark.raydp.RayExecutorUtils
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable

class ExecutorLifecycle(
                         appInfo: ApplicationInfo,
                         masterUrl: () => String,
                       ) extends Logging {

  private val executorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor("Executor-lifecycle")

  def schedule(): Unit = {
    val desc = appInfo.desc
    for (_ <- 0 until desc.numExecutors) {
      requestNewExecutor()
    }
    executorService.scheduleWithFixedDelay(() => {
      logDebug(s"send heartbeat to alive executors ${appInfo.hashCode()} ${appInfo.availableExecutors().toList}")
      var stoppedCount = 0
      for ((executorId, desc) <- appInfo.availableExecutors()) {
        try {
          val rst = Ray.get(RayExecutorUtils.heartbeat(desc.handler))
          assert(rst == "alive", s"Wrong heartbeat response $rst")
        } catch {
          case _: Throwable =>
            logWarning(s"$executorId heartbeat failed")
            appInfo.kill(executorId, shutdownActor = true)
            stoppedCount += 1
        }
      }
      if (stoppedCount > 0) {
        logInfo(s"$stoppedCount executor dead unexpected, request new executors")
        for (_ <- 0 until stoppedCount) {
          requestNewExecutor()
        }
      }
    }, 5L, 10L, TimeUnit.SECONDS)
  }

  def request(requestedTotal: Int, callback: Boolean => Unit): Unit = {
    logInfo(s"Request new Executors, target is $requestedTotal, current expected ${appInfo.expectedCount()}, current available ${appInfo.availableCount()}")
    executorService.execute(() => {
      if (requestedTotal > appInfo.expectedCount()) {
        (0 until (requestedTotal - appInfo.expectedCount())).foreach { _ =>
          requestNewExecutor()
        }
      }
      callback(true)
    })
  }

  def kill(executorIds: Seq[String], callback: Boolean => Unit): Unit = {
    logInfo(s"Try to kill executors $executorIds")
    executorService.execute(() => {
      var success = true
      for (executorId <- executorIds) {
        if (!appInfo.kill(executorId, shutdownActor = true)) {
          success = false
        }
      }
      callback(success)
    })
  }

  def register(executorId: String, sender: RpcAddress, callback: Boolean => Unit): Unit = {
    logInfo(s"Register executor $executorId $sender")
    executorService.execute(() => {
      val success = appInfo.registerExecutor(executorId, sender)
      callback(success)
    })
  }

  private def requestNewExecutor(): Unit = {
    val cpu = appInfo.desc.coresPerExecutor
    val memory = appInfo.desc.memoryPerExecutorMB
    val executorId = s"${appInfo.getNextExecutorId}"

    logInfo(s"Requesting Spark executor with Ray logical resource { CPU: $cpu, Memory: ${memory}MB " +
      s"${
        appInfo.desc.resourcePerExecutor
          .map { case (name, amount) => s"$name: $amount" }.mkString(", ")
      } }.., use pg ${appInfo.desc.usePythonDaemon}, executorId $executorId")

    val handler = RayExecutorUtils.createExecutorActor(
      appInfo.desc.name, executorId, masterUrl(),
      appInfo.desc.resourcePerExecutor.getOrElse("cpu", cpu.toDouble),
      memory,
      appInfo.desc.resourcePerExecutor
        .filterNot { case (name, _) => name == "cpu" }
        .map { case (name, amount) => (name, Double.box(amount)) }
        .asJava,
      seqAsJavaList(appInfo.desc.command.javaOpts),
      !appInfo.desc.usePythonDaemon,
      appInfo.desc.schedulingStrategy
    )
    appInfo.pendingExecutor(executorId, handler)
  }

}
