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

package org.apache.spark.executor

import ai.nurion.solstice.raydp.shims.SparkShimLoader
import io.ray.api.Ray
import io.ray.runtime.config.RayConfig
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.commons.io.FileUtils
import org.apache.spark.{RayDPConfigs, _}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.raydp._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.Utils

import java.io.{ByteArrayOutputStream, File}
import java.nio.channels.Channels
import scala.reflect.classTag

class RayDPExecutor(val appName: String,
                    var executorId: String,
                    val appMasterURL: String) extends Logging {

  val nodeIp: String = RayConfig.create().nodeIp
  val conf = new SparkConf()

  private val temporaryRpcEnvName = "ExecutorTemporaryRpcEnv"
  private var temporaryRpcEnv: Option[RpcEnv] = None
  private var workingDir: File = null
  private var backend: CoarseGrainedExecutorBackend = null;

  init()

  def init(): Unit = {
    createTemporaryRpcEnv(temporaryRpcEnvName, conf)
    assert(temporaryRpcEnv.nonEmpty)
    registerToAppMaster()
  }

  def registerToAppMaster(): Unit = {
    var appMaster: RpcEndpointRef = null
    val nTries = 3
    for (i <- 0 until nTries if appMaster == null) {
      try {
        appMaster = temporaryRpcEnv.get.setupEndpointRefByURI(appMasterURL)
      } catch {
        case e: Throwable =>
          if (i == nTries - 1) {
            throw e
          } else {
            logWarning(
              s"Executor: ${executorId} register to app master failed(${i + 1}/${nTries}) ")
          }
      }
    }
    val registeredResult = appMaster.askSync[Boolean](RegisterExecutor(executorId, nodeIp))
    if (registeredResult) {
      logInfo(s"Executor: ${executorId} register to app master success")
    } else {
      throw new RuntimeException(s"Executor: ${executorId} register to app master failed")
    }
  }

  def startUp(
               driverUrl: String,
               cores: Int,
               classPathEntries: String): Unit = {
    createWorkingDir()
    setUserDir()

    val userClassPath = classPathEntries.split(java.io.File.pathSeparator)
      .filter(_.nonEmpty).map(new File(_).toURI.toURL)
    val createFn: (RpcEnv, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, env, resourceProfile) =>
        SparkShimLoader.getSparkShims
          .getExecutorBackendFactory()
          .createExecutorBackend(rpcEnv, driverUrl, executorId,
            nodeIp, nodeIp, cores, userClassPath, env, None, resourceProfile)
    }
    try {
      serveAsExecutor(driverUrl, cores, createFn)
    } catch {
      case e: Exception =>
        logError("Got exception while running, exit actor", e)
        stop()
    }
  }

  def heartbeat(): String = "alive"

  def numRunningTasks(): Int = Option(backend).map(_.executor.numRunningTasks).getOrElse(-1)

  private def createWorkingDir(): Unit = {
    // create the application dir
    import scala.collection.JavaConverters._
    for (elem <- System.getenv().asScala) {
      log.debug(s"env ${elem._1} => ${elem._2}")
    }
    val rayConfig = RayConfig.create()
    val jobId = Ray.getRuntimeContext.getCurrentJobId.toString
    val appDir = new File(rayConfig.sessionDir, jobId)
    var remainingTimes = 3
    var continue = true
    while (continue && remainingTimes > 0) {
      try {
        appDir.mkdir()
        continue = !appDir.exists()
        remainingTimes -= 1
        if (remainingTimes > 0) {
          logInfo(s"Create application dir: ${appDir.getAbsolutePath} failed, " +
            s"remaining times: ${remainingTimes}")
        }
      } catch {
        case e: SecurityException =>
          throw e
      }
    }

    if (appDir.exists()) {
      if (appDir.isFile) {
        throw new RayDPException(
          s"Expect ${appDir.getAbsolutePath} is a directory, however it is a file")
      }
    } else {
      throw new RayDPException(s"Create application dir: ${appDir.getAbsolutePath} failed " +
        s"after 3 times trying")
    }

    val executorDir = new File(appDir.getCanonicalPath, s"$appName-$executorId")
    if (executorDir.exists()) {
      throw new RayDPException(
        s"Create $executorId working dir: ${executorDir.getAbsolutePath} failed because " +
          s"it existed already")
    }
    executorDir.mkdir()
    if (!executorDir.exists()) {
      throw new RayDPException(s"Create $executorId working dir: " +
        s"${executorDir.getAbsolutePath} failed")
    }
    workingDir = executorDir.getCanonicalFile
    logInfo(s"create appDir $appDir, executorDir $executorDir, workingDir is $workingDir")
    FileUtils.forceDeleteOnExit(workingDir)
  }

  private def setUserDir(): Unit = {
    assert(workingDir != null && workingDir.isDirectory)
    System.setProperty("user.dir", workingDir.getAbsolutePath)
    System.setProperty("java.io.tmpdir", workingDir.getAbsolutePath)
    logInfo(s"Set user.dir to ${workingDir.getAbsolutePath}")
  }

  private def serveAsExecutor(driverUrl: String,
                              cores: Int,
                              backendCreateFn: (RpcEnv, SparkEnv, ResourceProfile) => CoarseGrainedExecutorBackend
                             ): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      var driver: RpcEndpointRef = null
      val nTries = 3
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = temporaryRpcEnv.get.setupEndpointRefByURI(driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }

      val cfg = driver.askSync[SparkAppConfig](
        RetrieveSparkAppConfig(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
      val props = cfg.sparkProperties
      destroyTemporaryRpcEnv()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      driverConf.set(EXECUTOR_ID, executorId)
      val env = SparkEnv.createExecutorEnv(driverConf, executorId, nodeIp,
        nodeIp, cores, cfg.ioEncryptionKey, isLocal = false)

      // set the tmp dir for the executor, it will be deleted when executor stop
      val workerTmpDir = new File(workingDir, "_tmp")
      workerTmpDir.mkdir()
      assert(workerTmpDir.exists() && workerTmpDir.isDirectory)
      SparkEnv.get.driverTmpDir = Some(workerTmpDir.getAbsolutePath)

      env.conf.set(RayDPConfigs.SPARK_EXECUTOR_WORKING_DIR, workingDir.getAbsolutePath)
      backend = backendCreateFn(env.rpcEnv, env, cfg.resourceProfile)
      env.rpcEnv.setupEndpoint("Executor", backend)
      env.rpcEnv.awaitTermination()
    }
  }

  private def createTemporaryRpcEnv(
                                     name: String,
                                     conf: SparkConf): Unit = {
    val env = RpcEnv.create(name, nodeIp, nodeIp, -1, conf,
      new SecurityManager(conf),
      numUsableCores = 0, clientMode = true)
    temporaryRpcEnv = Some(env)
  }

  private def destroyTemporaryRpcEnv(): Unit = {
    if (temporaryRpcEnv.nonEmpty) {
      temporaryRpcEnv.get.shutdown()
      temporaryRpcEnv = None
    }
  }

  def stop(): Unit = {
    Ray.exitActor()
  }

  def getBlockLocations(rddId: Int, numPartitions: Int): Array[String] = {
    val env = SparkEnv.get
    val blockIds = (0 until numPartitions).map(i =>
      BlockId.apply("rdd_" + rddId + "_" + i)
    ).toArray
    val locations = BlockManager.blockIdsToLocations(blockIds, env)
    val result = new Array[String](numPartitions)
    for ((key, value) <- locations) {
      val partitionId = key.name.substring(key.name.lastIndexOf('_') + 1).toInt
      result(partitionId) = value.head.substring(value.head.lastIndexOf('_') + 1)
    }
    result
  }

  private def requestRecacheRDD(rddId: Int, driverAgentUrl: String): Unit = {
    val env = RpcEnv.create("TEMP_EXECUTOR_" + executorId, nodeIp, nodeIp, -1, conf,
      new SecurityManager(conf),
      numUsableCores = 0, clientMode = true)
    var driverAgent: RpcEndpointRef = null
    val nTries = 3
    for (i <- 0 until nTries if driverAgent == null) {
      try {
        driverAgent = env.setupEndpointRefByURI(driverAgentUrl)
      } catch {
        case e: Throwable =>
          if (i == nTries - 1) {
            throw e
          } else {
            logWarning(
              s"Executor: ${executorId} register to driver Agent failed(${i + 1}/${nTries}) ")
          }
      }
    }
    val success = driverAgent.askSync[Boolean](RecacheRDD(rddId))
    env.shutdown
  }

  def getRDDPartition(rddId: Int,
                      partitionId: Int,
                      schemaStr: String,
                      driverAgentUrl: String): Array[Byte] = {
    val env = SparkEnv.get
    val context = SparkShimLoader.getSparkShims.getDummyTaskContext(partitionId, env)
    TaskContext.setTaskContext(context)
    val schema = Schema.fromJSON(schemaStr)
    val blockId = BlockId.apply("rdd_" + rddId + "_" + partitionId)
    val iterator = env.blockManager.get(blockId)(classTag[Array[Byte]]) match {
      case Some(blockResult) =>
        blockResult.data.asInstanceOf[Iterator[Array[Byte]]]
      case None =>
        logWarning("The cached block has been lost. Cache it again via driver agent")
        requestRecacheRDD(rddId, driverAgentUrl)
        env.blockManager.get(blockId)(classTag[Array[Byte]]) match {
          case Some(blockResult) =>
            blockResult.data.asInstanceOf[Iterator[Array[Byte]]]
          case None =>
            throw new RayDPException("Still cannot get the block after recache!")
        }
    }
    val byteOut = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(byteOut))
    MessageSerializer.serialize(writeChannel, schema)
    iterator.foreach(writeChannel.write)
    ArrowStreamWriter.writeEndOfStream(writeChannel, new IpcOption)
    val result = byteOut.toByteArray
    writeChannel.close()
    byteOut.close()
    result
  }
}
