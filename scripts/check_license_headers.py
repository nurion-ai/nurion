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

"""Script to check if source files have proper Apache 2.0 license headers."""

import re
import sys
from pathlib import Path

# Expected license header patterns
PYTHON_LICENSE_PATTERN = re.compile(
    r"#\s*Copyright\s+2025\s+nurion\s+team.*?"
    r"#\s*Licensed\s+under\s+the\s+Apache\s+License.*?"
    r"#\s*http://www\.apache\.org/licenses/LICENSE-2\.0",
    re.DOTALL | re.IGNORECASE,
)

SCALA_JAVA_LICENSE_PATTERN = re.compile(
    r"/\*\s*\n\s*\*\s*Copyright\s+2025\s+nurion\s+team.*?"
    r"\*\s*Licensed\s+under\s+the\s+Apache\s+License.*?"
    r"\*\s*http://www\.apache\.org/licenses/LICENSE-2\.0",
    re.DOTALL | re.IGNORECASE,
)

# Alternative: ASF license (for raydp files)
# Match both Python (#) and Scala/Java (/* */) formats
ASF_LICENSE_PATTERN = re.compile(
    r"(?:#|/\*)\s*\n\s*(?:\*|#)?\s*Licensed\s+to\s+the\s+Apache\s+Software\s+Foundation.*?"
    r"(?:Apache\s+License,\s+Version\s+2\.0|http://www\.apache\.org/licenses/LICENSE-2\.0)",
    re.DOTALL | re.IGNORECASE,
)

# Directories to exclude from checking
EXCLUDE_DIRS = {
    "__pycache__",
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "target",
    "build",
    "dist",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "solstice.egg-info",
    "aether.egg-info",
}

# Files to exclude from checking
EXCLUDE_FILES = {
    "__init__.py",  # Usually very short, optional
}

# Directories that may have ASF license (raydp)
ASF_LICENSE_DIRS = {
    "raydp",
}


def should_check_file(file_path: Path) -> bool:
    """Determine if a file should be checked."""
    # Check if in excluded directory
    parts = file_path.parts
    if any(excluded in parts for excluded in EXCLUDE_DIRS):
        return False

    # Check if file is excluded
    if file_path.name in EXCLUDE_FILES:
        return False

    return True


def is_asf_licensed_file(file_path: Path) -> bool:
    """Check if file is in a directory that uses ASF license."""
    parts = file_path.parts
    return any(asf_dir in parts for asf_dir in ASF_LICENSE_DIRS)


def check_python_file(file_path: Path) -> tuple[bool, str]:
    """Check Python file for license header. Returns (is_valid, message)."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        return False, f"Error reading file: {e}"

    # Check first 30 lines
    lines = content.split("\n")[:30]
    header_text = "\n".join(lines)

    # First check if file has any valid license header (ASF or nurion)
    if ASF_LICENSE_PATTERN.search(header_text):
        return True, "Has ASF license header"
    
    if PYTHON_LICENSE_PATTERN.search(header_text):
        return True, "Has nurion license header"

    # For ASF-licensed files (raydp), expect ASF pattern
    if is_asf_licensed_file(file_path):
        return False, "Missing ASF license header (expected for raydp files)"

    return False, "Missing Apache 2.0 license header"


def check_scala_java_file(file_path: Path) -> tuple[bool, str]:
    """Check Scala/Java file for license header. Returns (is_valid, message)."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        return False, f"Error reading file: {e}"

    # Check first 50 lines
    lines = content.split("\n")[:50]
    header_text = "\n".join(lines)

    # First check if file has any valid license header (ASF or nurion)
    if ASF_LICENSE_PATTERN.search(header_text):
        return True, "Has ASF license header"
    
    if SCALA_JAVA_LICENSE_PATTERN.search(header_text):
        return True, "Has nurion license header"

    # For ASF-licensed files (raydp), expect ASF pattern
    if is_asf_licensed_file(file_path):
        return False, "Missing ASF license header (expected for raydp files)"

    return False, "Missing Apache 2.0 license header"


def check_directory(root_dir: Path) -> int:
    """Check all files in directory tree. Returns number of violations."""
    violations = []
    checked_count = 0

    for file_path in root_dir.rglob("*"):
        if not file_path.is_file():
            continue

        if not should_check_file(file_path):
            continue

        checked_count += 1

        if file_path.suffix == ".py":
            is_valid, message = check_python_file(file_path)
            if not is_valid:
                violations.append((file_path, message))
        elif file_path.suffix in (".scala", ".java"):
            is_valid, message = check_scala_java_file(file_path)
            if not is_valid:
                violations.append((file_path, message))

    if violations:
        print("❌ License header violations found:\n")
        for file_path, message in violations:
            print(f"  {file_path}: {message}")
        print(f"\nTotal: {len(violations)} violations out of {checked_count} files checked")
        return len(violations)
    else:
        print(f"✅ All {checked_count} files have proper license headers")
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        root_dir = Path(sys.argv[1])
    else:
        root_dir = Path(__file__).parent.parent

    if not root_dir.exists():
        print(f"Error: Directory {root_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    print(f"Checking license headers in: {root_dir}\n")
    violations = check_directory(root_dir)
    sys.exit(violations)

