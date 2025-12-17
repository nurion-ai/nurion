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

"""E2E tests for Aether service setup and registration.

Tests:
1. Aether health check
2. Lance table registration
3. Iceberg table registration
4. K8s cluster registration
"""

from __future__ import annotations

import pytest

from utils.aether_client import AetherClient
from utils.test_data import TestDataManager

pytestmark = [pytest.mark.e2e, pytest.mark.nightly]


class TestAetherHealth:
    """Tests for Aether service health."""
    
    def test_aether_health_check(self, aether_client: AetherClient):
        """Test that Aether service is healthy."""
        assert aether_client.health_check(), "Aether health check failed"
    
    def test_aether_health_endpoint_response(self, aether_client: AetherClient):
        """Test health endpoint returns expected response."""
        # The health check should pass after initialization
        is_healthy = aether_client.health_check()
        assert is_healthy is True


class TestLanceTableRegistration:
    """Tests for Lance table registration."""
    
    def test_create_lance_namespace(self, aether_client: AetherClient):
        """Test creating a Lance namespace."""
        result = aether_client.create_lance_namespace(
            name="nurion_test",
            location="s3://nurion/lance",
        )
        assert result is not None
        assert result.get("name") == "nurion_test"
    
    def test_register_videos_lance_table(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test registering videos Lance table."""
        table_info = test_data_manager.get_table_info("videos_lance")
        
        result = aether_client.register_lance_table(
            namespace=table_info.namespace,
            table_name=table_info.name,
            location=table_info.location,
            schema=table_info.schema,
        )
        
        assert result is not None
        assert result.get("name") == table_info.name
    
    def test_register_images_lance_table(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test registering images Lance table."""
        table_info = test_data_manager.get_table_info("images_lance")
        
        result = aether_client.register_lance_table(
            namespace=table_info.namespace,
            table_name=table_info.name,
            location=table_info.location,
            schema=table_info.schema,
        )
        
        assert result is not None
        assert result.get("name") == table_info.name
    
    def test_get_lance_table(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test getting Lance table details."""
        table_info = test_data_manager.get_table_info("videos_lance")
        
        result = aether_client.get_lance_table(
            namespace=table_info.namespace,
            table_name=table_info.name,
        )
        
        assert result is not None
        assert result.get("location") == table_info.location
    
    def test_list_lance_tables(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test listing Lance tables in namespace."""
        table_info = test_data_manager.get_table_info("videos_lance")
        
        tables = aether_client.list_lance_tables(namespace=table_info.namespace)
        
        assert len(tables) >= 2  # videos and images
        table_names = [t.get("name") for t in tables]
        assert "videos_lance" in table_names
        assert "images_lance" in table_names


class TestIcebergTableRegistration:
    """Tests for Iceberg table registration."""
    
    def test_create_iceberg_catalog(self, aether_client: AetherClient):
        """Test creating an Iceberg catalog."""
        result = aether_client.create_iceberg_catalog(
            name="nurion_catalog",
            catalog_type="rest",
            uri="http://iceberg-rest:8181",
            warehouse="s3://nurion/iceberg/warehouse",
        )
        
        assert result is not None
        assert result.get("name") == "nurion_catalog"
    
    def test_create_iceberg_namespace(self, aether_client: AetherClient):
        """Test creating an Iceberg namespace."""
        result = aether_client.create_iceberg_namespace(
            catalog="nurion_catalog",
            namespace="nurion_test",
            properties={"location": "s3://nurion/iceberg"},
        )
        
        assert result is not None
    
    def test_register_videos_iceberg_table(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test registering videos Iceberg table."""
        table_info = test_data_manager.get_table_info("videos_iceberg")
        
        result = aether_client.register_iceberg_table(
            catalog="nurion_catalog",
            namespace=table_info.namespace,
            table_name=table_info.name,
            location=table_info.location,
            schema=table_info.schema,
        )
        
        assert result is not None
        assert result.get("name") == table_info.name
    
    def test_register_images_iceberg_table(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test registering images Iceberg table."""
        table_info = test_data_manager.get_table_info("images_iceberg")
        
        result = aether_client.register_iceberg_table(
            catalog="nurion_catalog",
            namespace=table_info.namespace,
            table_name=table_info.name,
            location=table_info.location,
            schema=table_info.schema,
        )
        
        assert result is not None
        assert result.get("name") == table_info.name
    
    def test_get_iceberg_table(
        self,
        aether_client: AetherClient,
        test_data_manager: TestDataManager,
    ):
        """Test getting Iceberg table details."""
        table_info = test_data_manager.get_table_info("videos_iceberg")
        
        result = aether_client.get_iceberg_table(
            catalog="nurion_catalog",
            namespace=table_info.namespace,
            table_name=table_info.name,
        )
        
        assert result is not None


class TestK8sClusterRegistration:
    """Tests for K8s cluster registration."""
    
    def test_register_k8s_cluster(self, aether_client: AetherClient):
        """Test registering K8s cluster."""
        result = aether_client.register_k8s_cluster(
            name="nurion-sh",
            context="nurion-sh",
        )
        
        assert result is not None
        assert result.get("name") == "nurion-sh"
        assert "id" in result
    
    def test_list_k8s_clusters(self, aether_client: AetherClient):
        """Test listing K8s clusters."""
        clusters = aether_client.list_k8s_clusters()
        
        assert len(clusters) >= 1
        cluster_names = [c.get("name") for c in clusters]
        assert "nurion-sh" in cluster_names
    
    def test_k8s_cluster_connection(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
    ):
        """Test K8s cluster connection."""
        result = aether_client.test_k8s_connection(k8s_cluster_id)
        
        assert result is not None
        assert result.get("connected") is True
        assert "server_version" in result


class TestDataVerification:
    """Tests to verify test data is accessible."""
    
    def test_videos_lance_data_exists(self, test_data_manager: TestDataManager):
        """Verify videos Lance table data exists."""
        exists = test_data_manager.verify_table_exists("videos_lance")
        assert exists, "Videos Lance table data not found in S3"
    
    def test_images_lance_data_exists(self, test_data_manager: TestDataManager):
        """Verify images Lance table data exists."""
        exists = test_data_manager.verify_table_exists("images_lance")
        assert exists, "Images Lance table data not found in S3"
    
    def test_can_read_videos_lance_sample(self, test_data_manager: TestDataManager):
        """Verify can read sample from videos Lance table."""
        table = test_data_manager.read_lance_table("videos_lance", limit=10)
        
        assert len(table) > 0
        assert "video_path" in table.column_names
        assert "duration_seconds" in table.column_names
    
    def test_can_read_images_lance_sample(self, test_data_manager: TestDataManager):
        """Verify can read sample from images Lance table."""
        table = test_data_manager.read_lance_table("images_lance", limit=10)
        
        assert len(table) > 0
        assert "image" in table.column_names
        assert "width" in table.column_names
