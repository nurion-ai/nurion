#!/usr/bin/env python3
"""Generate 1000 JSONL test records for Spark source testing."""

import json
import random
from pathlib import Path

# Categories for generating realistic data
DEPARTMENTS = ["engineering", "sales", "marketing", "hr", "finance", "operations", "research"]
LOCATIONS = ["new_york", "san_francisco", "london", "tokyo", "berlin", "sydney", "toronto"]
SKILLS = [
    "python", "java", "scala", "spark", "sql", "kubernetes", "docker",
    "machine_learning", "data_engineering", "frontend", "backend", "devops",
    "analytics", "visualization", "cloud", "aws", "gcp", "azure"
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


def main():
    """Generate 1000 JSONL records."""
    random.seed(42)  # For reproducibility

    output_dir = Path(__file__).parent / "resources" / "spark"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "test_data_1000.jsonl"

    with open(output_file, "w") as f:
        for i in range(1000):
            record = generate_record(i)
            f.write(json.dumps(record) + "\n")

    print(f"Generated 1000 records to {output_file}")

    # Also generate a smaller sample for quick tests
    sample_file = output_dir / "test_data_100.jsonl"
    with open(sample_file, "w") as f:
        for i in range(100):
            record = generate_record(i)
            f.write(json.dumps(record) + "\n")

    print(f"Generated 100 records to {sample_file}")


if __name__ == "__main__":
    main()

