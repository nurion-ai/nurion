"""Debug artifact collection for E2E tests."""

from __future__ import annotations

import json
import os
import subprocess
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class DebugCollector:
    """Collects debug artifacts from E2E test runs.
    
    Collects:
    - Kubernetes pod logs
    - Kubernetes events
    - Ray job logs
    - Test output samples
    - Pytest reports
    """
    
    def __init__(
        self,
        output_dir: str = "debug-artifacts",
        namespace: str = "nurion-nightly",
        kubeconfig: Optional[str] = None,
    ):
        """Initialize debug collector.
        
        Args:
            output_dir: Directory to store artifacts
            namespace: Kubernetes namespace
            kubeconfig: Path to kubeconfig file
        """
        self.output_dir = Path(output_dir)
        self.namespace = namespace
        self.kubeconfig = kubeconfig
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Timestamp for this collection
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def _run_kubectl(self, args: List[str], output_file: Optional[Path] = None) -> str:
        """Run kubectl command.
        
        Args:
            args: kubectl arguments
            output_file: Optional file to write output to
            
        Returns:
            Command output
        """
        cmd = ["kubectl"]
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", self.kubeconfig])
        cmd.extend(["-n", self.namespace])
        cmd.extend(args)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )
            output = result.stdout
            
            if output_file:
                output_file.write_text(output)
            
            return output
        except subprocess.TimeoutExpired:
            return "Command timed out"
        except Exception as e:
            return f"Error: {e}"
    
    def collect_pod_logs(self, label_selector: str = "app=aether", tail: int = 1000) -> None:
        """Collect logs from pods matching selector.
        
        Args:
            label_selector: Kubernetes label selector
            tail: Number of log lines to collect
        """
        logs_dir = self.output_dir / "pod-logs"
        logs_dir.mkdir(exist_ok=True)
        
        # Get pod names
        pods_output = self._run_kubectl(["get", "pods", "-l", label_selector, "-o", "name"])
        pod_names = [p.strip().replace("pod/", "") for p in pods_output.strip().split("\n") if p]
        
        for pod_name in pod_names:
            if not pod_name:
                continue
            
            output_file = logs_dir / f"{pod_name}.log"
            self._run_kubectl(
                ["logs", pod_name, "--tail", str(tail), "--all-containers"],
                output_file=output_file,
            )
    
    def collect_events(self) -> None:
        """Collect Kubernetes events."""
        output_file = self.output_dir / "events.log"
        self._run_kubectl(
            ["get", "events", "--sort-by=.lastTimestamp"],
            output_file=output_file,
        )
    
    def collect_pod_status(self) -> None:
        """Collect pod status information."""
        output_file = self.output_dir / "pod-status.log"
        self._run_kubectl(
            ["get", "pods", "-o", "wide"],
            output_file=output_file,
        )
        
        # Also collect pod descriptions
        describe_file = self.output_dir / "pod-describe.log"
        self._run_kubectl(
            ["describe", "pods"],
            output_file=describe_file,
        )
    
    def collect_ray_job_logs(self, job_ids: List[str]) -> None:
        """Collect Ray job logs.
        
        Args:
            job_ids: List of Ray job IDs to collect logs for
        """
        ray_logs_dir = self.output_dir / "ray-logs"
        ray_logs_dir.mkdir(exist_ok=True)
        
        for job_id in job_ids:
            output_file = ray_logs_dir / f"{job_id}.log"
            
            # Try to get logs via ray job logs command
            try:
                result = subprocess.run(
                    ["ray", "job", "logs", job_id],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                output_file.write_text(result.stdout + "\n" + result.stderr)
            except Exception as e:
                output_file.write_text(f"Failed to get Ray job logs: {e}")
    
    def collect_configmaps_secrets(self) -> None:
        """Collect ConfigMaps and Secrets (names only, not values)."""
        output_file = self.output_dir / "configmaps.log"
        self._run_kubectl(["get", "configmaps", "-o", "wide"], output_file=output_file)
        
        # Get secret names only (not values)
        secrets_file = self.output_dir / "secrets.log"
        self._run_kubectl(["get", "secrets", "-o", "name"], output_file=secrets_file)
    
    def save_test_metadata(self, metadata: Dict[str, Any]) -> None:
        """Save test metadata.
        
        Args:
            metadata: Test metadata dictionary
        """
        metadata_file = self.output_dir / "test-metadata.json"
        metadata["collection_timestamp"] = self.timestamp
        metadata_file.write_text(json.dumps(metadata, indent=2, default=str))
    
    def save_test_output_sample(
        self,
        name: str,
        data: Any,
        max_size: int = 10000,
    ) -> None:
        """Save a sample of test output data.
        
        Args:
            name: Name for the sample file
            data: Data to save (will be JSON serialized)
            max_size: Maximum size in bytes
        """
        samples_dir = self.output_dir / "samples"
        samples_dir.mkdir(exist_ok=True)
        
        output_file = samples_dir / f"{name}.json"
        
        json_str = json.dumps(data, indent=2, default=str)
        if len(json_str) > max_size:
            json_str = json_str[:max_size] + "\n... (truncated)"
        
        output_file.write_text(json_str)
    
    def collect_all(self, job_ids: Optional[List[str]] = None) -> None:
        """Collect all debug artifacts.
        
        Args:
            job_ids: Optional list of Ray job IDs
        """
        print(f"Collecting debug artifacts to {self.output_dir}")
        
        # Kubernetes artifacts
        print("  - Collecting pod logs...")
        self.collect_pod_logs("app=aether")
        self.collect_pod_logs("app=postgresql")
        self.collect_pod_logs("app=github-runner")
        
        print("  - Collecting events...")
        self.collect_events()
        
        print("  - Collecting pod status...")
        self.collect_pod_status()
        
        print("  - Collecting configmaps/secrets...")
        self.collect_configmaps_secrets()
        
        # Ray job logs
        if job_ids:
            print("  - Collecting Ray job logs...")
            self.collect_ray_job_logs(job_ids)
        
        print("Done collecting debug artifacts")
    
    def create_archive(self) -> Path:
        """Create a tarball of all collected artifacts.
        
        Returns:
            Path to the created archive
        """
        archive_name = f"debug-artifacts-{self.timestamp}.tar.gz"
        archive_path = self.output_dir.parent / archive_name
        
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(self.output_dir, arcname="debug-artifacts")
        
        return archive_path


def collect_debug_on_failure(
    namespace: str = "nurion-nightly",
    output_dir: str = "debug-artifacts",
) -> Path:
    """Convenience function to collect debug artifacts on test failure.
    
    Args:
        namespace: Kubernetes namespace
        output_dir: Output directory
        
    Returns:
        Path to the debug archive
    """
    collector = DebugCollector(
        output_dir=output_dir,
        namespace=namespace,
    )
    collector.collect_all()
    return collector.create_archive()
