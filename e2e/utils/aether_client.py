"""Aether API client for E2E tests."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass
class RayJobStatus:
    """Status of a Ray job."""
    
    job_id: str
    status: str
    message: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    
    @property
    def is_terminal(self) -> bool:
        """Check if job is in terminal state."""
        return self.status in ("SUCCEEDED", "FAILED", "STOPPED")
    
    @property
    def is_success(self) -> bool:
        """Check if job succeeded."""
        return self.status == "SUCCEEDED"


class AetherClient:
    """HTTP client for Aether API.
    
    Provides methods for:
    - Health checks
    - Lance table management
    - Iceberg catalog management
    - K8s cluster registration
    - Ray job submission and monitoring
    """
    
    def __init__(self, base_url: str, timeout: float = 30.0):
        """Initialize Aether client.
        
        Args:
            base_url: Aether API base URL (e.g., http://aether:8000)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout)
    
    def close(self):
        """Close the HTTP client."""
        self._client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    # Health check
    
    def health_check(self) -> bool:
        """Check if Aether is healthy."""
        try:
            response = self._client.get("/api/health")
            return response.status_code == 200
        except Exception:
            return False
    
    def wait_for_health(self, timeout: float = 120.0, interval: float = 5.0) -> bool:
        """Wait for Aether to become healthy.
        
        Args:
            timeout: Maximum time to wait in seconds
            interval: Check interval in seconds
            
        Returns:
            True if healthy, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.health_check():
                return True
            time.sleep(interval)
        return False
    
    # Lance namespace/table management
    
    def create_lance_namespace(self, name: str, location: str) -> Dict[str, Any]:
        """Create a Lance namespace."""
        response = self._client.post(
            "/api/lance/namespaces",
            json={"name": name, "location": location},
        )
        response.raise_for_status()
        return response.json()
    
    def register_lance_table(
        self,
        namespace: str,
        table_name: str,
        location: str,
        schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register a Lance table."""
        payload = {
            "name": table_name,
            "location": location,
        }
        if schema:
            payload["schema"] = schema
        
        response = self._client.post(
            f"/api/lance/namespaces/{namespace}/tables",
            json=payload,
        )
        response.raise_for_status()
        return response.json()
    
    def get_lance_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get Lance table details."""
        response = self._client.get(
            f"/api/lance/namespaces/{namespace}/tables/{table_name}"
        )
        response.raise_for_status()
        return response.json()
    
    def list_lance_tables(self, namespace: str) -> List[Dict[str, Any]]:
        """List tables in a Lance namespace."""
        response = self._client.get(f"/api/lance/namespaces/{namespace}/tables")
        response.raise_for_status()
        return response.json()
    
    # Iceberg catalog management
    
    def create_iceberg_catalog(
        self,
        name: str,
        catalog_type: str,
        uri: str,
        warehouse: str,
    ) -> Dict[str, Any]:
        """Create an Iceberg catalog."""
        response = self._client.post(
            "/api/iceberg/catalogs",
            json={
                "name": name,
                "catalog_type": catalog_type,
                "uri": uri,
                "warehouse": warehouse,
            },
        )
        response.raise_for_status()
        return response.json()
    
    def create_iceberg_namespace(
        self,
        catalog: str,
        namespace: str,
        properties: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create an Iceberg namespace."""
        response = self._client.post(
            f"/api/iceberg/catalogs/{catalog}/namespaces",
            json={
                "namespace": namespace,
                "properties": properties or {},
            },
        )
        response.raise_for_status()
        return response.json()
    
    def register_iceberg_table(
        self,
        catalog: str,
        namespace: str,
        table_name: str,
        location: str,
        schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register an Iceberg table."""
        payload = {
            "name": table_name,
            "location": location,
        }
        if schema:
            payload["schema"] = schema
        
        response = self._client.post(
            f"/api/iceberg/catalogs/{catalog}/namespaces/{namespace}/tables",
            json=payload,
        )
        response.raise_for_status()
        return response.json()
    
    def get_iceberg_table(
        self,
        catalog: str,
        namespace: str,
        table_name: str,
    ) -> Dict[str, Any]:
        """Get Iceberg table details."""
        response = self._client.get(
            f"/api/iceberg/catalogs/{catalog}/namespaces/{namespace}/tables/{table_name}"
        )
        response.raise_for_status()
        return response.json()
    
    # K8s cluster management
    
    def register_k8s_cluster(
        self,
        name: str,
        kubeconfig: Optional[str] = None,
        context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register a Kubernetes cluster."""
        payload = {"name": name}
        if kubeconfig:
            payload["kubeconfig"] = kubeconfig
        if context:
            payload["context"] = context
        
        response = self._client.post("/api/k8s/clusters", json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_k8s_cluster(self, cluster_id: int) -> Dict[str, Any]:
        """Get K8s cluster details."""
        response = self._client.get(f"/api/k8s/clusters/{cluster_id}")
        response.raise_for_status()
        return response.json()
    
    def list_k8s_clusters(self) -> List[Dict[str, Any]]:
        """List registered K8s clusters."""
        response = self._client.get("/api/k8s/clusters")
        response.raise_for_status()
        return response.json()
    
    def test_k8s_connection(self, cluster_id: int) -> Dict[str, Any]:
        """Test connection to a K8s cluster."""
        response = self._client.post(f"/api/k8s/clusters/{cluster_id}/test-connection")
        response.raise_for_status()
        return response.json()
    
    # Ray job management
    
    def submit_rayjob(
        self,
        cluster_id: int,
        name: str,
        entrypoint: str,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Submit a Ray job."""
        payload = {
            "cluster_id": cluster_id,
            "name": name,
            "entrypoint": entrypoint,
        }
        if runtime_env:
            payload["runtime_env"] = runtime_env
        if metadata:
            payload["metadata"] = metadata
        
        response = self._client.post("/api/k8s/rayjobs", json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_rayjob_status(self, job_id: str) -> RayJobStatus:
        """Get Ray job status."""
        response = self._client.get(f"/api/k8s/rayjobs/{job_id}")
        response.raise_for_status()
        data = response.json()
        return RayJobStatus(
            job_id=data["job_id"],
            status=data["status"],
            message=data.get("message"),
            start_time=data.get("start_time"),
            end_time=data.get("end_time"),
        )
    
    def wait_for_rayjob(
        self,
        job_id: str,
        timeout: float = 1800.0,  # 30 minutes
        interval: float = 10.0,
    ) -> RayJobStatus:
        """Wait for Ray job to complete.
        
        Args:
            job_id: Ray job ID
            timeout: Maximum wait time in seconds
            interval: Poll interval in seconds
            
        Returns:
            Final job status
            
        Raises:
            TimeoutError: If job doesn't complete within timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.get_rayjob_status(job_id)
            if status.is_terminal:
                return status
            time.sleep(interval)
        
        raise TimeoutError(f"Ray job {job_id} did not complete within {timeout} seconds")
    
    def get_rayjob_logs(self, job_id: str) -> str:
        """Get Ray job logs."""
        response = self._client.get(f"/api/k8s/rayjobs/{job_id}/logs")
        response.raise_for_status()
        return response.text
    
    def stop_rayjob(self, job_id: str) -> Dict[str, Any]:
        """Stop a Ray job."""
        response = self._client.post(f"/api/k8s/rayjobs/{job_id}/stop")
        response.raise_for_status()
        return response.json()
