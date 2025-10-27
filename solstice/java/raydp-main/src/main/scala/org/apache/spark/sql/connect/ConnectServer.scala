package org.apache.spark.sql.connect

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SparkConnectService

object ConnectServer {
  def start(): Int = {
    try {
      val sc = SparkSession.getActiveSession.get
      SparkConnectService.start(sc.sparkContext)
      sc.conf.get(Connect.CONNECT_GRPC_BINDING_PORT.key, "15002").toInt
    } catch {
      case e: InterruptedException =>
        throw new RuntimeException(e)
    }
  }

}
