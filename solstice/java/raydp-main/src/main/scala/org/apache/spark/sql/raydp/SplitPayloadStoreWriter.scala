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

package org.apache.spark.sql.raydp

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Base64, HashMap => JHashMap, Properties}

/**
 * Writes Arrow data directly to Tansu Queue.
 *
 * This is the V2 implementation that:
 * 1. Embeds Arrow IPC data directly in Kafka message (base64 encoded)
 * 2. Writes directly to output_queue (bypasses source_queue + operator)
 * 3. No ObjectRef serialization - data is inline in message
 *
 * Flow:
 * 1. Encode Arrow bytes as base64
 * 2. Create payload_key = "_v2arrow:{base64_data}"
 * 3. Send message to output_queue
 * 4. Downstream: payload_store.get(payload_key) → decode and convert to SplitPayload
 *
 * @param queueBootstrapServers Kafka bootstrap servers for Tansu
 * @param queueTopic Topic name (output_queue topic)
 * @param stageId Stage identifier for message IDs
 */
class SplitPayloadStoreWriter(
    queueBootstrapServers: String,
    queueTopic: String,
    stageId: String
) extends Serializable {

  @transient private var kafkaProducer: KafkaProducer[String, Array[Byte]] = _
  @transient private lazy val gson = new Gson()

  private var messageCounter = 0
  private var totalRecords = 0

  /**
   * Initialize the writer.
   * Must be called once before storeAndSend().
   */
  def start(): Unit = {
    // Initialize Kafka producer for Tansu
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, queueBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    // Increase max request size for large Arrow batches (default 1MB -> 16MB)
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "16777216")

    kafkaProducer = new KafkaProducer[String, Array[Byte]](props)
  }

  /**
   * Store Arrow data and send message to output queue.
   *
   * V2 Direct approach: embeds Arrow data directly in Kafka message.
   * This avoids ObjectRef serialization issues between JVM and Python
   * while maintaining simplicity. For large datasets, data is chunked
   * into manageable partition sizes.
   *
   * @param arrowBytes Arrow IPC format bytes
   * @param splitId Unique split identifier
   * @param numRecords Number of records in this batch
   * @return Queue offset
   */
  def storeAndSend(
      arrowBytes: Array[Byte],
      splitId: String,
      numRecords: Int
  ): Long = {

    // 1. Encode Arrow bytes as base64 for JSON embedding
    val arrowBase64 = Base64.getEncoder.encodeToString(arrowBytes)

    // 2. Create payload_key with JVM Arrow prefix for direct data
    // Format: _jvm_arrow:{base64_encoded_arrow_ipc}
    val payloadKey = s"_jvm_arrow:${arrowBase64}"

    // 3. Send message to output_queue
    val metadata = new JHashMap[String, Any]()
    metadata.put("source_stage", stageId)
    metadata.put("num_records", Integer.valueOf(numRecords))
    metadata.put("arrow_bytes_len", Integer.valueOf(arrowBytes.length))

    val message = new JHashMap[String, Any]()
    message.put("message_id", s"${stageId}_${messageCounter}")
    message.put("split_id", splitId)
    message.put("payload_key", payloadKey)  // Contains Arrow data directly
    message.put("metadata", metadata)
    message.put("timestamp", java.lang.Double.valueOf(System.currentTimeMillis() / 1000.0))

    val jsonBytes = gson.toJson(message).getBytes("UTF-8")
    val record = new ProducerRecord[String, Array[Byte]](queueTopic, splitId, jsonBytes)

    val future = kafkaProducer.send(record)
    val result = future.get()

    messageCounter += 1
    totalRecords += numRecords
    result.offset()
  }

  /**
   * Flush any pending messages.
   */
  def flush(): Unit = {
    if (kafkaProducer != null) {
      kafkaProducer.flush()
    }
  }

  /**
   * Close the writer and release resources.
   */
  def close(): Unit = {
    if (kafkaProducer != null) {
      kafkaProducer.flush()
      kafkaProducer.close()
      kafkaProducer = null
    }
  }

  /**
   * Get the number of messages sent.
   */
  def getMessageCount: Int = messageCounter

  /**
   * Get the total number of records sent.
   */
  def getTotalRecords: Int = totalRecords
}

object SplitPayloadStoreWriter {
  /**
   * Create a new writer instance.
   *
   * @param queueBootstrapServers Kafka bootstrap servers for Tansu
   * @param queueTopic Topic name (output_queue topic)
   * @param stageId Stage identifier
   * @return A new SplitPayloadStoreWriter instance
   */
  def create(
      queueBootstrapServers: String,
      queueTopic: String,
      stageId: String
  ): SplitPayloadStoreWriter = {
    new SplitPayloadStoreWriter(queueBootstrapServers, queueTopic, stageId)
  }
}
