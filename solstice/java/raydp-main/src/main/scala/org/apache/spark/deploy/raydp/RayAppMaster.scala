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
import io.ray.runtime.config.RayConfig
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.RayExecutorUtils
import org.apache.spark.rpc._
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

class RayAppMaster(host: String,
                   port: Int) extends Serializable with Logging {
  private var endpoint: RpcEndpointRef = _
  private var rpcEnv: RpcEnv = _
  private val conf: SparkConf = new SparkConf()

  init()

  def this() = {
    this(RayConfig.create().nodeIp, 0)
  }

  def init(): Unit = {
    Utils.loadDefaultSparkProperties(conf)
    val securityMgr = new SecurityManager(conf)
    rpcEnv = RpcEnv.create(
      RayAppMaster.ENV_NAME,
      host,
      host,
      port,
      conf,
      securityMgr,
      numUsableCores = 0,
      clientMode = false)
    // register endpoint
    endpoint = rpcEnv.setupEndpoint(RayAppMaster.ENDPOINT_NAME, new RayAppMasterEndpoint(rpcEnv))
  }

  /**
   * Get the app master endpoint URL. The executor will connect to AppMaster by this URL and
   * tell the AppMaster that it has started up successful.
   */
  private def getAppMasterEndpointUrl(): String = {
    RpcEndpointAddress(rpcEnv.address, RayAppMaster.ENDPOINT_NAME).toString
  }

  /**
   * used by Python Actor, be careful
   */
  def getMasterUrl(): String = {
    val url = RpcEndpointAddress(rpcEnv.address, RayAppMaster.ENDPOINT_NAME).toString
    url.replace("spark", "ray")
  }

  def stop(): Int = {
    logInfo("Stopping RayAppMaster")
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      endpoint = null
      rpcEnv = null
    }
    0
  }

  class RayAppMasterEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

    private var driverEndpoint: RpcEndpointRef = null
    private var driverAddress: RpcAddress = null
    private var appInfo: ApplicationInfo = null
    private var executorLifecycle: ExecutorLifecycle = null;

    override def receive: PartialFunction[Any, Unit] = {
      case RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef) =>
        logInfo(s"Registering app ${appDescription.name}, app command ${appDescription.command}")
        val app = createApplication(appDescription, driver)
        registerApplication(app)
        driver.send(RegisteredApplication(self, s"raydp-${Ray.getRuntimeContext.getCurrentActorId.toString}"))
        executorLifecycle = new ExecutorLifecycle(appInfo, getAppMasterEndpointUrl)
        executorLifecycle.schedule()

      case UnregisterApplication() =>
        appInfo.markFinished(ApplicationState.FINISHED)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorIp) =>
        executorLifecycle.register(executorId, context.senderAddress, result => {
          if (result) {
            setUpExecutor(executorId)
          }
          context.reply(result)
        })

      case RequestExecutors(requestedTotal) =>
        executorLifecycle.request(requestedTotal, context.reply(_))

      case KillExecutors(executorIds) =>
        executorLifecycle.kill(executorIds, context.reply(_));

      case MasterHeartbeat(ts) =>
        context.reply(MasterHeartbeatResponse("alive", System.currentTimeMillis()))
    }

    override def onStop(): Unit = {
    }

    private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef): ApplicationInfo = {
      val now = System.currentTimeMillis()
      new ApplicationInfo(now, desc, driver)
    }

    private def registerApplication(app: ApplicationInfo): Unit = {
      val appAddress = app.driver.address
      if (appAddress == driverAddress) {
        logInfo("Attempted to re-register application at same address: " + appAddress)
        return
      }

      appInfo = app
      driverEndpoint = app.driver
      driverAddress = appAddress
    }

    private def setUpExecutor(executorId: String): Unit = {
      val handlerOpt = appInfo.getExecutorHandler(executorId)
      if (handlerOpt.isEmpty) {
        logWarning(s"Trying to setup executor: ${executorId} which has been removed")
      }
      val driverUrl = appInfo.desc.command.driverUrl
      val cores = appInfo.desc.coresPerExecutor
      val classPathEntries = appInfo.desc.command.classPathEntries.mkString(";")
      RayExecutorUtils.setUpExecutor(handlerOpt.get, driverUrl, cores, classPathEntries)
    }
  }
}

object RayAppMaster extends Serializable {
  val ENV_NAME = "RAY_RPC_ENV"
  val ENDPOINT_NAME = "RAY_APP_MASTER"
  val ACTOR_NAME = "RAY_APP_MASTER"

}
