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

// package org.apache.spark.metrics.sink
//
// import com.codahale.metrics._
// import com.google.common.collect.Maps
// import org.apache.spark.internal.Logging
// import org.apache.spark.metrics.MetricsSystem
// import org.apache.spark.util.ThreadUtils
//
// import java.util
// import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
// import java.util.{Locale, Properties}
// import scala.collection.JavaConverters._
//
// class CanoeReporter(registry: MetricRegistry, stub: MetricsServiceBlockingStub, scheduler: ScheduledExecutorService)
//   extends ScheduledReporter(registry, "canoe-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, scheduler)
//     with Logging {
//
//   override def report(gauges: util.SortedMap[String, Gauge[_]],
//                       counters: util.SortedMap[String, Counter],
//                       histograms: util.SortedMap[String, Histogram],
//                       meters: util.SortedMap[String, Meter],
//                       timers: util.SortedMap[String, Timer]): Unit = {
//     val requestBuilder = BatchMetricsRequest.newBuilder()
//     gauges.asScala.foreach {
//       case (name, gauge) =>
//         val gaugeValue = gauge.getValue match {
//           case l: Long => l.toFloat
//           case f: Float => f
//           case d: Double => d.toFloat
//           case _ => gauge.getValue.toString.toFloat
//         }
//         val (newName, tags) = formatName(name)
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(newName)
//           .setCanoeValue(gaugeValue)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//     }
//
//     counters.asScala.foreach {
//       case (name, counter) =>
//         val (newName, tags) = formatName(name)
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(newName)
//           .setCanoeValue(counter.getCount)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//     }
//     histograms.asScala.foreach {
//       case (name, hist) =>
//         val snapshot = hist.getSnapshot
//         val (newName, tags) = formatName(name)
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}Count")
//           .setCanoeValue(hist.getCount)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}Max")
//           .setCanoeValue(snapshot.getMax)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}Mean")
//           .setCanoeValue(snapshot.getMean.toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}50thPercentile")
//           .setCanoeValue(snapshot.getMedian.toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}75thPercentile")
//           .setCanoeValue(snapshot.get75thPercentile().toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}95thPercentile")
//           .setCanoeValue(snapshot.get95thPercentile().toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}98thPercentile")
//           .setCanoeValue(snapshot.get98thPercentile().toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}99thPercentile")
//           .setCanoeValue(snapshot.get99thPercentile().toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}999thPercentile")
//           .setCanoeValue(snapshot.get999thPercentile().toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}StdDev")
//           .setCanoeValue(snapshot.getStdDev.toFloat)
//           .setMetricType("scalar")
//           .build())
//     }
//
//     meters.asScala.foreach {
//       case (name, meter) =>
//         val (newName, tags) = formatName(name)
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}Count")
//           .setCanoeValue(meter.getCount)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}MeanRate")
//           .setCanoeValue(meter.getMeanRate.toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}OneMinuteRate")
//           .setCanoeValue(meter.getOneMinuteRate.toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}FiveMinuteRate")
//           .setCanoeValue(meter.getFiveMinuteRate.toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//         requestBuilder.addMetrics(MetricsRequest.newBuilder()
//           .setCanoeMetric(s"${newName}FifteenMinuteRate")
//           .setCanoeValue(meter.getFifteenMinuteRate.toFloat)
//           .putAllTags(tags)
//           .setMetricType("scalar")
//           .build())
//     }
//     stub.batchLogMetrics(requestBuilder.build())
//   }
//
//   private def formatName(name: String): (String, util.Map[String, String]) = {
//     val nameParts = name.split("\\.")
//     val tags = Maps.newHashMap[String, String]()
//     tags.put("spark.app.id", nameParts(0))
//     tags.put("spark.executor.id", nameParts(1))
//     val newName = nameParts.drop(2).mkString("_")
//     (newName, tags)
//   }
// }
//
// class CanoeSink(val property: Properties, val registry: MetricRegistry) extends Sink with Logging {
//
//   val CANOE_KEY_PERIOD = "period"
//   val CANOE_KEY_UNIT = "unit"
//
//   val CANOE_DEFAULT_PERIOD = 10
//   val CANOE_DEFAULT_UNIT = "SECONDS"
//
//   private val pollPeriod = Option(property.getProperty(CANOE_KEY_PERIOD)) match {
//     case Some(s) => s.toInt
//     case None => CANOE_DEFAULT_PERIOD
//   }
//
//   private val pollUnit: TimeUnit = Option(property.getProperty(CANOE_KEY_UNIT)) match {
//     case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
//     case None => TimeUnit.valueOf(CANOE_DEFAULT_UNIT)
//   }
//
//   MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)
//   private val stub = MetricsClient.getBlockingStub
//   private val reporter = new CanoeReporter(registry, stub, ThreadUtils.newDaemonSingleThreadScheduledExecutor("Canoe-Reporter"))
//
//   override def start(): Unit = {
//     reporter.start(pollPeriod, pollUnit)
//   }
//
//   override def stop(): Unit = {
//     reporter.stop()
//   }
//
//   override def report(): Unit = {
//     reporter.report()
//   }
// }
