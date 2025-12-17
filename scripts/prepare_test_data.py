#!/usr/bin/env python3

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

"""Prepare test data for E2E tests.

Downloads videos from FineVideo and images from LAION-HR,
then uploads to S3-compatible storage and creates Lance/Iceberg tables.

Usage:
    # Set environment variables or use command line args
    export AWS_ENDPOINT_URL="host"
    export AWS_ACCESS_KEY_ID="your-access-key"
    export AWS_SECRET_ACCESS_KEY="your-secret-key"
    
    python scripts/prepare_test_data.py \
        --s3-bucket nurion \
        --video-count 1000 \
        --image-count 10000
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List

import boto3
import pyarrow as pa


def get_s3_client(endpoint: str) -> boto3.client:
    """Create S3 client for S3-compatible storage."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", ""),
    )


def get_storage_options(endpoint: str) -> Dict[str, str]:
    """Get storage options for Lance/PyIceberg."""
    return {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_endpoint": endpoint,
        "aws_region": os.environ.get("AWS_DEFAULT_REGION", ""),
    }


def download_finevideo_sample(count: int) -> Iterator[Dict[str, Any]]:
    """Download sample videos from FineVideo dataset.
    
    Args:
        count: Number of videos to download
        
    Yields:
        Video records with binary data and metadata
    """
    try:
        from datasets import load_dataset
    except ImportError:
        print("Installing datasets library...")
        os.system("pip install datasets")
        from datasets import load_dataset
    
    print(f"Loading FineVideo dataset (streaming mode)...")
    dataset = load_dataset(
        "HuggingFaceFV/finevideo",
        split="train",
        streaming=True,
    )
    
    # Filter videos by duration (8-12 minutes = 480-720 seconds)
    def duration_filter(sample):
        duration = sample.get("json", {}).get("duration_seconds", 0)
        return 480 <= duration <= 720
    
    filtered = filter(duration_filter, dataset)
    
    downloaded = 0
    for sample in filtered:
        if downloaded >= count:
            break
        
        try:
            video_bytes = sample.get("mp4")
            metadata = sample.get("json", {})
            
            if video_bytes:
                yield {
                    "id": f"video_{downloaded:04d}",
                    "video_bytes": video_bytes,
                    "duration_seconds": metadata.get("duration_seconds", 0),
                    "width": metadata.get("width", 1920),
                    "height": metadata.get("height", 1080),
                    "fps": metadata.get("fps", 30.0),
                    "category": metadata.get("category", "unknown"),
                    "metadata": json.dumps(metadata),
                }
                downloaded += 1
                
                if downloaded % 10 == 0:
                    print(f"  Downloaded {downloaded}/{count} videos...")
                    
        except Exception as e:
            print(f"  Error downloading video: {e}")
            continue
    
    print(f"Downloaded {downloaded} videos")


def download_laion_images(count: int) -> Iterator[Dict[str, Any]]:
    """Download sample images from LAION-HR dataset.
    
    Args:
        count: Number of images to download
        
    Yields:
        Image records with binary data and metadata
    """
    try:
        from PIL import Image
        import requests
    except ImportError:
        os.system("pip install Pillow requests")
        from PIL import Image
        import requests
    
    try:
        from datasets import load_dataset
    except ImportError:
        os.system("pip install datasets")
        from datasets import load_dataset
    
    print(f"Loading LAION-HR dataset (streaming mode)...")
    
    # Load parquet with URLs
    dataset = load_dataset(
        "laion/laion-high-resolution",
        split="train",
        streaming=True,
    )
    
    downloaded = 0
    for sample in dataset:
        if downloaded >= count:
            break
        
        url = sample.get("url")
        if not url:
            continue
        
        try:
            # Download image
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                continue
            
            image_bytes = response.content
            
            # Check size (800KB - 1.2MB)
            size = len(image_bytes)
            if not (800 * 1024 <= size <= 1200 * 1024):
                continue
            
            # Get dimensions
            with Image.open(io.BytesIO(image_bytes)) as img:
                width, height = img.size
                format_str = img.format.lower() if img.format else "jpeg"
            
            # Check resolution
            if width < 1024 or height < 1024:
                continue
            
            yield {
                "id": f"image_{downloaded:05d}",
                "image": image_bytes,
                "format": format_str,
                "width": width,
                "height": height,
                "size_bytes": size,
                "metadata": json.dumps({
                    "url": url,
                    "caption": sample.get("caption", ""),
                }),
            }
            downloaded += 1
            
            if downloaded % 100 == 0:
                print(f"  Downloaded {downloaded}/{count} images...")
                
        except Exception as e:
            continue
    
    print(f"Downloaded {downloaded} images")


def upload_videos_to_s3(
    s3_client,
    bucket: str,
    videos: Iterator[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Upload videos to S3 and return metadata records.
    
    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        videos: Iterator of video records
        
    Returns:
        List of video metadata records with S3 paths
    """
    records = []
    
    for video in videos:
        video_id = video["id"]
        video_bytes = video.pop("video_bytes")
        
        # Upload to S3
        s3_key = f"raw/videos/{video_id}.mp4"
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=video_bytes,
            ContentType="video/mp4",
        )
        
        # Create metadata record
        record = {
            "id": video_id,
            "video_path": f"s3://{bucket}/{s3_key}",
            "duration_seconds": video["duration_seconds"],
            "width": video["width"],
            "height": video["height"],
            "fps": video["fps"],
            "category": video["category"],
            "metadata": video["metadata"],
        }
        records.append(record)
    
    return records


def create_lance_table(
    records: List[Dict[str, Any]],
    table_uri: str,
    storage_options: Dict[str, str],
) -> None:
    """Create Lance table from records.
    
    Args:
        records: List of records
        table_uri: S3 URI for Lance table
        storage_options: S3 storage options
    """
    try:
        import lance
    except ImportError:
        os.system("pip install lance")
        import lance
    
    table = pa.Table.from_pylist(records)
    
    print(f"Creating Lance table at {table_uri}...")
    lance.write_dataset(table, table_uri, storage_options=storage_options)
    print(f"  Created with {len(records)} records")


def create_iceberg_table(
    records: List[Dict[str, Any]],
    catalog_uri: str,
    namespace: str,
    table_name: str,
    location: str,
) -> None:
    """Create Iceberg table from records.
    
    Args:
        records: List of records
        catalog_uri: Iceberg REST catalog URI
        namespace: Iceberg namespace
        table_name: Table name
        location: S3 location for table data
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        os.system("pip install pyiceberg")
        from pyiceberg.catalog import load_catalog
    
    print(f"Creating Iceberg table {namespace}.{table_name}...")
    
    catalog = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": catalog_uri,
            "warehouse": location,
        },
    )
    
    table_data = pa.Table.from_pylist(records)
    
    # Create namespace if not exists
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass
    
    # Create table
    catalog.create_table(
        f"{namespace}.{table_name}",
        schema=table_data.schema,
        location=f"{location}/{table_name}",
    )
    
    # Append data
    table = catalog.load_table(f"{namespace}.{table_name}")
    table.append(table_data)
    
    print(f"  Created with {len(records)} records")


def main():
    parser = argparse.ArgumentParser(description="Prepare test data for E2E tests")
    parser.add_argument("--s3-endpoint", default=os.environ.get("AWS_ENDPOINT_URL", ""))
    parser.add_argument("--s3-bucket", default="nurion")
    parser.add_argument("--video-count", type=int, default=1000)
    parser.add_argument("--image-count", type=int, default=10000)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--iceberg-catalog", default="http://localhost:8181")
    
    args = parser.parse_args()
    
    # Check environment
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        print("Error: AWS_ACCESS_KEY_ID not set")
        sys.exit(1)
    
    s3_client = get_s3_client(args.s3_endpoint)
    storage_options = get_storage_options(args.s3_endpoint)
    
    print("=" * 60)
    print("Nurion E2E Test Data Preparation")
    print("=" * 60)
    print(f"S3 Endpoint: {args.s3_endpoint}")
    print(f"S3 Bucket: {args.s3_bucket}")
    print(f"Videos: {args.video_count}")
    print(f"Images: {args.image_count}")
    print()
    
    if not args.skip_download:
        # Download and upload videos
        print("Step 1: Downloading and uploading videos...")
        videos = download_finevideo_sample(args.video_count)
        video_records = upload_videos_to_s3(s3_client, args.s3_bucket, videos)
        
        # Create videos Lance table
        print("\nStep 2: Creating videos Lance table...")
        create_lance_table(
            video_records,
            f"s3://{args.s3_bucket}/lance/videos_lance",
            storage_options,
        )
        
        # Download and upload images
        print("\nStep 3: Downloading images...")
        images = list(download_laion_images(args.image_count))
        
        # Create images Lance table (with binary blobs)
        print("\nStep 4: Creating images Lance table...")
        create_lance_table(
            images,
            f"s3://{args.s3_bucket}/lance/images_lance",
            storage_options,
        )
        
        # Create video records with paths for Iceberg
        print("\nStep 5: Creating Iceberg tables...")
        try:
            create_iceberg_table(
                video_records,
                args.iceberg_catalog,
                "nurion_test",
                "videos_iceberg",
                f"s3://{args.s3_bucket}/iceberg",
            )
            
            create_iceberg_table(
                images,
                args.iceberg_catalog,
                "nurion_test",
                "images_iceberg",
                f"s3://{args.s3_bucket}/iceberg",
            )
        except Exception as e:
            print(f"  Iceberg table creation failed (may need REST catalog): {e}")
    
    print("\n" + "=" * 60)
    print("Test data preparation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
