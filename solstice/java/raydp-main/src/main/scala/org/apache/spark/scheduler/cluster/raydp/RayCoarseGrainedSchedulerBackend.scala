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

package org.apache.spark.scheduler.cluster.raydp

import org.apache.spark.deploy.raydp._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.config.{Python, SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.{ResourceProfile, ResourceRequirement, ResourceUtils}
import org.apache.spark.rpc.{RpcEndpointAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}
import org.apache.spark.{RayDPConfigs, RayDPException, SparkConf, SparkContext}

import java.net.URI
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Semaphore, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * A SchedulerBackend that request executor from Ray.
 */
class RayCoarseGrainedSchedulerBackend(
                                        sc: SparkContext,
                                        scheduler: TaskSchedulerImpl,
                                        masterURL: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging {

  private val masterSparkUrl = transferOrCreateRPCEndpoint(masterURL).toString

  private val appMasterRef = new AtomicReference[RpcEndpointRef]()
  private val stopped = new AtomicBoolean()
  private val appIdRef = new AtomicReference[String]()

  private val registrationBarrier = new Semaphore(0)
  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("Master-Heartbeater")
  private val heartbeatFailedCount = new AtomicReference[Int]()

  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf

    override protected def onStopRequest(): Unit = stopWithState(SparkAppHandle.State.KILLED)
  }

  override def applicationId(): String = appIdRef.get()

  protected override val minRegisteredRatio: Double =
    if (conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private def transferOrCreateRPCEndpoint(sparkUrl: String): RpcEndpointAddress = {
    try {
      var uri: URI = null
      logInfo(s"Creating RPC endpoint for $sparkUrl")
      uri = new URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "ray" ||
        host == null ||
        port < 0 ||
        name == null ||
        (uri.getPath != null && uri.getPath.nonEmpty) ||
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new RayDPException("Invalid Ray Master URL: " + sparkUrl)
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new RayDPException("Invalid Ray Master URL: " + sparkUrl, e)
    }
  }

  override def createTokenManager(): Option[HadoopDelegationTokenManager] = {
    Some(new HadoopDelegationTokenManager(sc.conf, sc.hadoopConfiguration, driverEndpoint))
  }

  override def start(): Unit = {
    super.start()

    val conf = sc.conf

    if (sc.deployMode != "client") {
      throw new RayDPException("We only support client mode currently")
    }

    launcherBackend.connect()

    val driverUrl = RpcEndpointAddress(
      conf.get(config.DRIVER_HOST_ADDRESS),
      conf.get(config.DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME
    ).toString
    val extraJavaOpts = sc.conf.get(config.EXECUTOR_JAVA_OPTIONS)
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.get(config.EXECUTOR_CLASS_PATH)
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.get(config.EXECUTOR_LIBRARY_PATH)
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains(config.Tests.IS_TESTING.key)) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    // add Xmx, it should not be set in java opts, because Spark is not allowed.
    // We also add Xms to ensure the Xmx >= Xms
    val memoryLimit = Seq(s"-Xms${sc.executorMemory}M", s"-Xmx${sc.executorMemory}M")

    val javaOpts = sparkJavaOpts ++ extraJavaOpts ++ memoryLimit

    val command = Command(driverUrl, sc.executorEnvs,
      classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val coresPerExecutor = conf.get(config.EXECUTOR_CORES.key, "1").toInt

    val executorResourceReqs = ResourceUtils.parseResourceRequirements(
      conf, config.SPARK_EXECUTOR_PREFIX)
    val raydpExecutorCustomResources = parseRayDPResourceRequirements(conf)

    val resourcesInMap = transferResourceRequirements(executorResourceReqs) ++
      raydpExecutorCustomResources
    val numExecutors = initialExecutors

    val appDesc = ApplicationDescription(name = sc.appName, numExecutors = numExecutors,
      coresPerExecutor = coresPerExecutor,
      memoryPerExecutorMB = sc.executorMemory,
      command = command,
      resourcePerExecutor = resourcesInMap,
      usePythonDaemon = conf.get(Python.PYTHON_USE_DAEMON),
      schedulingStrategy = conf.get(RayDPConfigs.SPARK_EXECUTOR_SCHEDULING_STRATEGY, "DEFAULT"))

    val rpcEnv = sc.env.rpcEnv
    appMasterRef.set(rpcEnv.setupEndpoint("AppMasterClient", new AppMasterClient(appDesc, rpcEnv)))
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = {
    stopWithState(SparkAppHandle.State.FINISHED)
  }


  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  private def parseRayDPResourceRequirements(sparkConf: SparkConf): Map[String, Double] = {
    sparkConf.getAllWithPrefix(
        s"${RayDPConfigs.SPARK_EXECUTOR_ACTOR_RESOURCE_PREFIX}.")
      .map { case (key, _) => key.toLowerCase }
      .distinct
      .map(name => {
        val amountDouble = sparkConf.get(
          s"${RayDPConfigs.SPARK_EXECUTOR_ACTOR_RESOURCE_PREFIX}.${name}",
          0d.toString).toDouble
        name -> amountDouble
      })
      .toMap
  }

  private def transferResourceRequirements(requirements: Seq[ResourceRequirement]): mutable.HashMap[String, Double] = {
    val results = mutable.HashMap[String, Double]()
    requirements.foreach { r =>
      val value = 1.0 * r.amount / r.numParts
      if (results.contains(r.resourceName)) {
        results(r.resourceName) = results(r.resourceName) + value
      } else {
        results += ((r.resourceName, value))
      }
    }
    results
  }

  private def waitForRegistration(): Unit = {
    try {
      val d = RpcUtils.lookupRpcTimeout(conf).duration
      registrationBarrier.tryAcquire(d.length, d.unit)
    } catch {
      case _: Exception =>
        logWarning("waiting for registration timeout")
        stop()
    }
  }

  private class AppMasterClient(
                                 appDesc: ApplicationDescription,
                                 override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

    override def onStart(): Unit = {
      try {
        registerToAppMaster()
        logInfo(s"Driver Registered to app master $appDesc")
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to app master", e)
          stop()
          RayCoarseGrainedSchedulerBackend.this.stop()
      }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredApplication(ref, appId) =>
        appIdRef.set(appId)
        appMasterRef.set(ref)
        registrationBarrier.release()
        heartbeater.scheduleWithFixedDelay(() => {
          val f = appMasterRef.get.ask[MasterHeartbeatResponse](MasterHeartbeat(System.currentTimeMillis()))
          f.onComplete {
            case Success(_) => heartbeatFailedCount.set(0)
            case Failure(e) =>
              logWarning("Heartbeat with master failed", e)
              heartbeatFailedCount.set(heartbeatFailedCount.get() + 1)
              if (heartbeatFailedCount.get() >= 3) {
                logError(s"Heartbeat failed too many times ${heartbeatFailedCount.get()}")
                stopWithState(SparkAppHandle.State.FAILED)
                System.exit(1)
              }
          }
        }, 5, 5, TimeUnit.SECONDS)
    }

    private def registerToAppMaster(): Unit = {
      val appMasterRef = rpcEnv.setupEndpointRefByURI(masterSparkUrl)
      appMasterRef.send(RegisterApplication(appDesc, self))
    }
  }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  override protected def doRequestTotalExecutors(
                                                  resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    if (appMasterRef.get != null) {
      val defaultProf = sc.resourceProfileManager.defaultResourceProfile
      val numExecs = resourceProfileToTotalExecs.getOrElse(defaultProf, 0)
      appMasterRef.get.ask[Boolean](RequestExecutors(numExecs))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   *
   * @return whether the kill request is acknowledged.
   */
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (appMasterRef.get != null) {
      appMasterRef.get.ask[Boolean](KillExecutors(executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  private def stopWithState(finalState: SparkAppHandle.State): Unit = {
    if (stopped.compareAndSet(false, true)) {
      try {
        super.stop() // this will stop all executors
        if (appMasterRef.get != null)
          appMasterRef.get.send(UnregisterApplication())
      } finally {
        appMasterRef.set(null)
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }
}
