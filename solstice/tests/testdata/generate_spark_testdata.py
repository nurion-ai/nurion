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

"""Generate test data for Spark source testing (JSONL and Parquet formats)."""

import json
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Categories for generating realistic data
DEPARTMENTS = ["engineering", "sales", "marketing", "hr", "finance", "operations", "research"]
LOCATIONS = ["new_york", "san_francisco", "london", "tokyo", "berlin", "sydney", "toronto"]
SKILLS = [
    "python",
    "java",
    "scala",
    "spark",
    "sql",
    "kubernetes",
    "docker",
    "machine_learning",
    "data_engineering",
    "frontend",
    "backend",
    "devops",
    "analytics",
    "visualization",
    "cloud",
    "aws",
    "gcp",
    "azure",
]
STATUSES = ["active", "inactive", "pending", "archived"]


def generate_record(idx: int) -> dict:
    """Generate a single test record with realistic data."""
    return {
        "id": idx,
        "name": f"user_{idx}",
        "email": f"user_{idx}@company.com",
        "age": random.randint(22, 65),
        "salary": round(random.uniform(50000, 200000), 2),
        "department": random.choice(DEPARTMENTS),
        "location": random.choice(LOCATIONS),
        "years_experience": random.randint(0, 40),
        "skills": random.sample(SKILLS, k=random.randint(1, 5)),
        "performance_score": round(random.uniform(1.0, 5.0), 2),
        "projects_completed": random.randint(0, 100),
        "is_manager": random.random() > 0.8,
        "status": random.choice(STATUSES),
        "hire_year": random.randint(2000, 2024),
        "team_size": random.randint(0, 20) if random.random() > 0.8 else 0,
    }


def generate_records(count: int) -> list:
    """Generate a list of records."""
    return [generate_record(i) for i in range(count)]


def save_jsonl(records: list, output_file: Path) -> None:
    """Save records to JSONL format."""
    with open(output_file, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def save_parquet(records: list, output_file: Path) -> None:
    """Save records to Parquet format."""
    # Convert skills list to string for parquet (list types are tricky)
    for record in records:
        record["skills"] = ",".join(record["skills"])

    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_file)


def ensure_spark_testdata() -> Path:
    """Ensure Spark test data exists, generating if needed.

    Returns the path to the spark testdata directory.
    """
    output_dir = Path(__file__).parent / "resources" / "spark"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_100 = output_dir / "test_data_100.parquet"
    parquet_1000 = output_dir / "test_data_1000.parquet"

    # Only regenerate if files don't exist
    if not parquet_100.exists() or not parquet_1000.exists():
        random.seed(42)  # For reproducibility

        # Generate 1000 records
        records_1000 = generate_records(1000)
        save_jsonl(records_1000, output_dir / "test_data_1000.jsonl")
        save_parquet(records_1000.copy(), output_dir / "test_data_1000.parquet")

        # Generate 100 records (subset)
        random.seed(42)
        records_100 = generate_records(100)
        save_jsonl(records_100, output_dir / "test_data_100.jsonl")
        save_parquet(records_100.copy(), output_dir / "test_data_100.parquet")

        print(f"Generated Spark test data in {output_dir}")

    return output_dir


def main():
    """Generate test data files."""
    random.seed(42)  # For reproducibility

    output_dir = Path(__file__).parent / "resources" / "spark"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate 1000 records
    records_1000 = generate_records(1000)
    save_jsonl(records_1000, output_dir / "test_data_1000.jsonl")
    save_parquet(records_1000.copy(), output_dir / "test_data_1000.parquet")
    print("Generated 1000 records (JSONL + Parquet)")

    # Generate 100 records
    random.seed(42)
    records_100 = generate_records(100)
    save_jsonl(records_100, output_dir / "test_data_100.jsonl")
    save_parquet(records_100.copy(), output_dir / "test_data_100.parquet")
    print("Generated 100 records (JSONL + Parquet)")

    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
