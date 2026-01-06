# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prometheus metrics exporter for Solstice jobs."""

import os
from typing import Any, Dict, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from solstice.utils.logging import create_ray_logger


class PrometheusMetricsExporter:
    """Export Solstice metrics to Prometheus.

    Integrates with Ray's Prometheus support and adds Solstice-specific metrics.

    Metrics exported:
    - Stage-level: throughput, queue lag, backpressure, skew ratio, worker count
    - Partition-level: per-partition lag
    - Worker-level: processing time histogram

    Usage:
        exporter = PrometheusMetricsExporter(job_id="my_job")
        exporter.update_stage_metrics("stage_1", metrics)
        exporter.update_partition_metrics("stage_1", partition_metrics)
    """

    def __init__(self, job_id: str, registry: Optional[CollectorRegistry] = None):
        """Initialize metrics exporter.

        Args:
            job_id: Job identifier for metric labels
            registry: Optional Prometheus registry (creates new if None)
        """
        self.job_id = job_id
        self.registry = registry or CollectorRegistry()
        self.logger = create_ray_logger(f"PrometheusExporter-{job_id}")

        # Stage-level metrics
        self.records_processed = Counter(
            "solstice_records_processed_total",
            "Total number of records processed",
            ["job_id", "stage_id", "direction"],
            registry=self.registry,
        )

        self.throughput = Gauge(
            "solstice_throughput_records_per_second",
            "Current throughput in records per second",
            ["job_id", "stage_id", "direction"],
            registry=self.registry,
        )

        self.queue_lag = Gauge(
            "solstice_queue_lag",
            "Number of pending messages in input queue",
            ["job_id", "stage_id"],
            registry=self.registry,
        )

        self.queue_size = Gauge(
            "solstice_queue_size",
            "Current size of output queue",
            ["job_id", "stage_id"],
            registry=self.registry,
        )

        self.partition_lag = Gauge(
            "solstice_partition_lag",
            "Per-partition lag in messages",
            ["job_id", "stage_id", "partition_id"],
            registry=self.registry,
        )

        self.partition_offset = Gauge(
            "solstice_partition_offset",
            "Latest offset for partition",
            ["job_id", "stage_id", "partition_id", "offset_type"],
            registry=self.registry,
        )

        self.backpressure = Gauge(
            "solstice_backpressure_active",
            "Whether backpressure is active (1=yes, 0=no)",
            ["job_id", "stage_id"],
            registry=self.registry,
        )

        self.skew_ratio = Gauge(
            "solstice_skew_ratio",
            "Data skew ratio (max_lag / avg_lag)",
            ["job_id", "stage_id"],
            registry=self.registry,
        )

        self.worker_count = Gauge(
            "solstice_worker_count",
            "Number of active workers",
            ["job_id", "stage_id"],
            registry=self.registry,
        )

        self.processing_time = Histogram(
            "solstice_processing_time_seconds",
            "Split processing time in seconds",
            ["job_id", "stage_id"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
            registry=self.registry,
        )

        self.stage_uptime = Gauge(
            "solstice_stage_uptime_seconds",
            "Stage uptime in seconds",
            ["job_id", "stage_id"],
            registry=self.registry,
        )

        self.logger.info(f"Initialized Prometheus metrics exporter for job {job_id}")

    def update_stage_metrics(self, stage_id: str, metrics: Dict[str, Any]) -> None:
        """Update stage-level metrics.

        Args:
            stage_id: Stage identifier
            metrics: Metrics dictionary from StageMetrics
        """
        labels = {"job_id": self.job_id, "stage_id": stage_id}

        try:
            # Throughput
            if "input_throughput" in metrics:
                self.throughput.labels(**labels, direction="input").set(metrics["input_throughput"])
            if "output_throughput" in metrics:
                self.throughput.labels(**labels, direction="output").set(
                    metrics["output_throughput"]
                )

            # Queue metrics
            if "input_queue_lag" in metrics:
                self.queue_lag.labels(**labels).set(metrics["input_queue_lag"])
            if "output_queue_size" in metrics:
                self.queue_size.labels(**labels).set(metrics["output_queue_size"])

            # Backpressure
            if "backpressure_active" in metrics:
                self.backpressure.labels(**labels).set(1 if metrics["backpressure_active"] else 0)

            # Skew
            if "skew_ratio" in metrics:
                self.skew_ratio.labels(**labels).set(metrics["skew_ratio"])

            # Worker count
            if "worker_count" in metrics:
                self.worker_count.labels(**labels).set(metrics["worker_count"])

            # Uptime
            if "uptime_secs" in metrics:
                self.stage_uptime.labels(**labels).set(metrics["uptime_secs"])

            # Records processed (cumulative)
            if "input_records" in metrics:
                self.records_processed.labels(**labels, direction="input").inc(
                    metrics["input_records"]
                )
            if "output_records" in metrics:
                self.records_processed.labels(**labels, direction="output").inc(
                    metrics["output_records"]
                )

        except Exception as e:
            self.logger.warning(f"Failed to update stage metrics: {e}")

    def update_partition_metrics(
        self,
        stage_id: str,
        partition_metrics: Dict[int, Dict[str, Any]],
    ) -> None:
        """Update partition-level metrics.

        Args:
            stage_id: Stage identifier
            partition_metrics: Dict mapping partition_id to PartitionMetrics
        """
        labels = {"job_id": self.job_id, "stage_id": stage_id}

        try:
            for partition_id, pm in partition_metrics.items():
                part_labels = {**labels, "partition_id": str(partition_id)}

                # Lag
                if "lag" in pm:
                    self.partition_lag.labels(**part_labels).set(pm["lag"])

                # Offsets
                if "latest_offset" in pm:
                    self.partition_offset.labels(**part_labels, offset_type="latest").set(
                        pm["latest_offset"]
                    )

                if "committed_offset" in pm:
                    self.partition_offset.labels(**part_labels, offset_type="committed").set(
                        pm["committed_offset"]
                    )

        except Exception as e:
            self.logger.warning(f"Failed to update partition metrics: {e}")

    def observe_processing_time(self, stage_id: str, duration_seconds: float) -> None:
        """Record a processing time observation.

        Args:
            stage_id: Stage identifier
            duration_seconds: Processing duration in seconds
        """
        try:
            self.processing_time.labels(job_id=self.job_id, stage_id=stage_id).observe(
                duration_seconds
            )
        except Exception as e:
            self.logger.warning(f"Failed to observe processing time: {e}")


def get_ray_prometheus_url() -> Optional[str]:
    """Get Ray's Prometheus metrics endpoint URL.

    Ray exports metrics on each node at :8080/metrics by default.
    Can be configured via RAY_PROMETHEUS_HOST environment variable.

    Returns:
        Prometheus URL or None if not configured
    """
    return os.getenv("RAY_PROMETHEUS_HOST", "http://localhost:8080")


def get_grafana_url() -> Optional[str]:
    """Get Grafana dashboard URL if configured.

    Users need to deploy their own Grafana + Prometheus.
    We provide pre-built dashboard JSON files.

    Returns:
        Grafana URL or None if not configured
    """
    return os.getenv("SOLSTICE_GRAFANA_URL")


def get_prometheus_pushgateway_url() -> Optional[str]:
    """Get Prometheus Pushgateway URL if configured.

    Used for short-lived jobs that need to push metrics.

    Returns:
        Pushgateway URL or None if not configured
    """
    return os.getenv("SOLSTICE_PROMETHEUS_PUSHGATEWAY")
