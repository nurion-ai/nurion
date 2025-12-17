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

"""Test data management for E2E tests."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
import lance
import pyarrow as pa


@dataclass
class TableInfo:
    """Information about a test table."""
    
    name: str
    namespace: str
    location: str
    format: str  # "lance" or "iceberg"
    record_count: int
    schema: Dict[str, Any]


class TestDataManager:
    """Manages test data for E2E tests.
    
    Provides methods for:
    - Accessing pre-created test tables (videos/images in Lance/Iceberg)
    - Creating temporary test data
    - Validating output data
    """
    
    # Pre-defined test table locations
    TEST_TABLES = {
        "videos_lance": TableInfo(
            name="videos_lance",
            namespace="nurion_test",
            location="s3://nurion/lance/videos_lance",
            format="lance",
            record_count=1000,
            schema={
                "id": "string",
                "video_path": "string",
                "duration_seconds": "float",
                "width": "int32",
                "height": "int32",
                "fps": "float",
                "category": "string",
                "metadata": "string",
            },
        ),
        "videos_iceberg": TableInfo(
            name="videos_iceberg",
            namespace="nurion_test",
            location="s3://nurion/iceberg/videos_iceberg",
            format="iceberg",
            record_count=1000,
            schema={
                "id": "string",
                "video_path": "string",
                "duration_seconds": "float",
                "width": "int32",
                "height": "int32",
                "fps": "float",
                "category": "string",
                "metadata": "string",
            },
        ),
        "images_lance": TableInfo(
            name="images_lance",
            namespace="nurion_test",
            location="s3://nurion/lance/images_lance",
            format="lance",
            record_count=10000,
            schema={
                "id": "string",
                "image": "binary",
                "format": "string",
                "width": "int32",
                "height": "int32",
                "size_bytes": "int64",
                "metadata": "string",
            },
        ),
        "images_iceberg": TableInfo(
            name="images_iceberg",
            namespace="nurion_test",
            location="s3://nurion/iceberg/images_iceberg",
            format="iceberg",
            record_count=10000,
            schema={
                "id": "string",
                "image": "binary",
                "format": "string",
                "width": "int32",
                "height": "int32",
                "size_bytes": "int64",
                "metadata": "string",
            },
        ),
    }
    
    def __init__(
        self,
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_region: Optional[str] = None,
    ):
        """Initialize test data manager.
        
        Args:
            s3_endpoint: S3 endpoint URL (from AWS_ENDPOINT_URL env)
            s3_access_key: S3 access key
            s3_secret_key: S3 secret key
            s3_region: S3 region (from AWS_DEFAULT_REGION env)
        """
        self.s3_endpoint = s3_endpoint or os.environ.get("AWS_ENDPOINT_URL")
        self.s3_access_key = s3_access_key or os.environ.get("AWS_ACCESS_KEY_ID")
        self.s3_secret_key = s3_secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.s3_region = s3_region or os.environ.get("AWS_DEFAULT_REGION", "")
        
        self._s3_client = None
    
    @property
    def s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            # Use virtual addressing style for Volcengine TOS compatibility
            s3_config = Config(s3={"addressing_style": "virtual"})
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                region_name=self.s3_region,
                config=s3_config,
            )
        return self._s3_client
    
    @property
    def storage_options(self) -> Dict[str, str]:
        """Get storage options for Lance/PyIceberg."""
        return {
            "aws_access_key_id": self.s3_access_key,
            "aws_secret_access_key": self.s3_secret_key,
            "aws_endpoint": self.s3_endpoint,
            "aws_region": self.s3_region,
        }
    
    def get_table_info(self, table_name: str) -> TableInfo:
        """Get information about a test table."""
        if table_name not in self.TEST_TABLES:
            raise ValueError(f"Unknown test table: {table_name}")
        return self.TEST_TABLES[table_name]
    
    def list_tables(self) -> List[str]:
        """List available test tables."""
        return list(self.TEST_TABLES.keys())
    
    def verify_table_exists(self, table_name: str) -> bool:
        """Verify that a test table exists in S3."""
        info = self.get_table_info(table_name)
        
        # Parse S3 location
        if not info.location.startswith("s3://"):
            return False
        
        path = info.location[5:]  # Remove "s3://"
        bucket, key = path.split("/", 1)
        
        try:
            # Check if the table directory exists
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=key,
                MaxKeys=1,
            )
            return response.get("KeyCount", 0) > 0
        except Exception:
            return False
    
    def read_lance_table(self, table_name: str, limit: Optional[int] = None) -> pa.Table:
        """Read a Lance table.
        
        Args:
            table_name: Name of the test table
            limit: Maximum number of rows to read
            
        Returns:
            PyArrow table with the data
        """
        info = self.get_table_info(table_name)
        if info.format != "lance":
            raise ValueError(f"Table {table_name} is not a Lance table")
        
        dataset = lance.dataset(info.location, storage_options=self.storage_options)
        
        if limit:
            return dataset.head(limit)
        return dataset.to_table()
    
    def get_output_location(self, test_name: str) -> str:
        """Get S3 location for test output.
        
        Args:
            test_name: Name of the test
            
        Returns:
            S3 URI for output data
        """
        return f"s3://nurion/test_outputs/{test_name}"
    
    def verify_output_table(
        self,
        location: str,
        expected_columns: List[str],
        min_records: int = 1,
    ) -> bool:
        """Verify that an output table was created correctly.
        
        Args:
            location: S3 location of the output table
            expected_columns: List of expected column names
            min_records: Minimum number of expected records
            
        Returns:
            True if verification passes
        """
        try:
            dataset = lance.dataset(location, storage_options=self.storage_options)
            schema = dataset.schema
            
            # Check columns
            actual_columns = set(schema.names)
            for col in expected_columns:
                if col not in actual_columns:
                    return False
            
            # Check record count
            count = dataset.count_rows()
            if count < min_records:
                return False
            
            return True
        except Exception:
            return False
    
    def cleanup_output(self, location: str) -> None:
        """Clean up test output data.
        
        Args:
            location: S3 location to clean up
        """
        if not location.startswith("s3://"):
            return
        
        path = location[5:]
        bucket, prefix = path.split("/", 1)
        
        try:
            # List and delete all objects with the prefix
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    self.s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": objects},
                    )
        except Exception:
            pass  # Ignore cleanup errors


def create_sample_video_records(count: int = 10) -> List[Dict[str, Any]]:
    """Create sample video records for testing.
    
    Args:
        count: Number of records to create
        
    Returns:
        List of video record dictionaries
    """
    records = []
    for i in range(count):
        records.append({
            "id": f"video_{i:04d}",
            "video_path": f"s3://nurion/raw/videos/video_{i:04d}.mp4",
            "duration_seconds": 600.0 + (i % 120),  # 10-12 minutes
            "width": 1920,
            "height": 1080,
            "fps": 30.0,
            "category": ["education", "entertainment", "tech"][i % 3],
            "metadata": json.dumps({"source": "finevideo", "index": i}),
        })
    return records


def create_sample_image_records(count: int = 100) -> List[Dict[str, Any]]:
    """Create sample image records for testing.
    
    Note: This creates metadata only, not actual image bytes.
    
    Args:
        count: Number of records to create
        
    Returns:
        List of image record dictionaries
    """
    records = []
    for i in range(count):
        # Create a small placeholder image (1x1 pixel JPEG)
        placeholder = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
        
        records.append({
            "id": f"image_{i:05d}",
            "image": placeholder,  # Placeholder bytes
            "format": "jpeg",
            "width": 1024,
            "height": 1024,
            "size_bytes": 1024 * 1024,  # ~1MB
            "metadata": json.dumps({"source": "laion-hr", "index": i}),
        })
    return records
