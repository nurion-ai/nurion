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

import io.ray.api.ActorHandle
import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.RayExecutorUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class ExecutorDesc(executorId: String,
                        handler: ActorHandle[RayDPExecutor],
                        var address: Option[RpcAddress] = None) {
  var registered: Boolean = false
}

private[spark] class ApplicationInfo(val startTime: Long,
                                     val desc: ApplicationDescription,
                                     val driver: RpcEndpointRef) extends Logging {

  private var state: ApplicationState.Value = _
  private var executors: mutable.HashMap[String, ExecutorDesc] = _
  private var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  private var endTime: Long = _
  private var nextExecutorId: Int = _

  init()

  private def init(): Unit = {
    state = ApplicationState.WAITING
    executors = new mutable.HashMap[String, ExecutorDesc]
    endTime = -1L
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
  }

  def pendingExecutor(executorId: String,
                      handler: ActorHandle[RayDPExecutor]): Unit = {
    val desc = ExecutorDesc(executorId, handler)
    executors(executorId) = desc
    logDebug(s"Add pending executor ${hashCode()} $executors $desc")
  }

  def registerExecutor(executorId: String, address: RpcAddress): Boolean = {
    if (executors.contains(executorId)) {
      if (executors(executorId).registered) {
        logWarning(s"Try to register executor: ${executorId} twice")
        false
      } else {
        executors(executorId).registered = true
        executors(executorId).address = Some(address)
        true
      }
    } else {
      logWarning(s"Try to register executor: $executorId which is not existed")
      false
    }
  }

  def kill(address: RpcAddress, shutdownActor: Boolean): Boolean = {
    executors.find(_._2.address.contains(address)) match {
      case None => false
      case Some((id, _)) => kill(id, shutdownActor)
    }
  }

  def kill(executorId: String, shutdownActor: Boolean): Boolean = {
    logInfo(s"Do kill $executorId ${executors.contains(executorId)}")
    if (executors.contains(executorId)) {
      val exec = executors(executorId)
      removedExecutors += executors(executorId)
      executors -= executorId
      if (shutdownActor) {
        RayExecutorUtils.exitExecutor(exec.handler, desc.name, executorId)
      }
      true
    } else {
      false
    }
  }

  def getExecutorHandler(executorId: String): Option[ActorHandle[RayDPExecutor]] = {
    executors.get(executorId).map(_.handler)
  }

  def pendingExecutors(): Iterator[(String, ExecutorDesc)] = {
    executors.iterator.filter(!_._2.registered)
  }

  def availableExecutors(): Iterator[(String, ExecutorDesc)] = {
    executors.iterator.filter(_._2.registered)
  }

  def availableCount(): Int = {
    executors.count(_._2.registered)
  }

  def expectedCount(): Int = {
    executors.size
  }

  def getNextExecutorId: Int = {
    val previous = nextExecutorId
    nextExecutorId += 1
    previous
  }

  def markFinished(endState: ApplicationState.Value): Unit = {
    state = endState
    endTime = System.currentTimeMillis()
  }

}
