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

"""Script to add Apache 2.0 license headers to source files."""

import os
import sys
from pathlib import Path

# License header templates
PYTHON_LICENSE_HEADER = """# Copyright 2025 nurion team
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
"""

SCALA_JAVA_LICENSE_HEADER = """/*
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
"""

# Directories to exclude (they may have their own license headers)
EXCLUDE_DIRS = {
    "raydp",  # Has ASF license headers
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
}

# Files to exclude
EXCLUDE_FILES = {
    "__init__.py",  # Usually very short, may skip
}

# Patterns that indicate existing license headers
LICENSE_INDICATORS = [
    "Copyright",
    "Licensed under the Apache License",
    "Apache License, Version 2.0",
]


def has_license_header(content: str) -> bool:
    """Check if file already has a license header."""
    # Check first 30 lines for license indicators
    lines = content.split("\n")[:30]
    text = "\n".join(lines).lower()
    return any(indicator.lower() in text for indicator in LICENSE_INDICATORS)


def should_process_file(file_path: Path) -> bool:
    """Determine if a file should be processed."""
    # Check if in excluded directory
    parts = file_path.parts
    if any(excluded in parts for excluded in EXCLUDE_DIRS):
        return False

    # Check if file is excluded
    if file_path.name in EXCLUDE_FILES:
        return False

    return True


def add_license_to_python(file_path: Path) -> bool:
    """Add license header to Python file. Returns True if modified."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return False

    # Skip if already has license
    if has_license_header(content):
        return False

    # Handle shebang and encoding
    lines = content.split("\n")
    new_lines = []
    shebang = None
    encoding = None

    # Extract shebang if present
    if lines and lines[0].startswith("#!"):
        shebang = lines[0]
        lines = lines[1:]

    # Extract encoding if present
    if lines and lines[0].startswith("# -*- coding:") or lines[0].startswith("# coding:"):
        encoding = lines[0]
        lines = lines[1:]

    # Skip empty lines at start
    while lines and not lines[0].strip():
        lines = lines[1:]

    # Build new content
    if shebang:
        new_lines.append(shebang)
        new_lines.append("")
    if encoding:
        new_lines.append(encoding)
        new_lines.append("")

    new_lines.append(PYTHON_LICENSE_HEADER.rstrip())
    new_lines.append("")
    new_lines.extend(lines)

    new_content = "\n".join(new_lines)
    if new_content != content:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_content)
            return True
        except Exception as e:
            print(f"Error writing {file_path}: {e}", file=sys.stderr)
            return False

    return False


def add_license_to_scala_java(file_path: Path) -> bool:
    """Add license header to Scala/Java file. Returns True if modified."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return False

    # Skip if already has license
    if has_license_header(content):
        return False

    # Add license at the beginning
    new_content = SCALA_JAVA_LICENSE_HEADER + "\n" + content

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        return True
    except Exception as e:
        print(f"Error writing {file_path}: {e}", file=sys.stderr)
        return False


def process_directory(root_dir: Path):
    """Process all files in directory tree."""
    modified_count = 0
    skipped_count = 0

    for file_path in root_dir.rglob("*"):
        if not file_path.is_file():
            continue

        if not should_process_file(file_path):
            continue

        if file_path.suffix == ".py":
            if add_license_to_python(file_path):
                print(f"Added license to: {file_path}")
                modified_count += 1
            else:
                skipped_count += 1
        elif file_path.suffix in (".scala", ".java"):
            if add_license_to_scala_java(file_path):
                print(f"Added license to: {file_path}")
                modified_count += 1
            else:
                skipped_count += 1

    print(f"\n✅ Processed: {modified_count} files modified, {skipped_count} files skipped")
    return modified_count


if __name__ == "__main__":
    if len(sys.argv) > 1:
        root_dir = Path(sys.argv[1])
    else:
        root_dir = Path(__file__).parent.parent

    if not root_dir.exists():
        print(f"Error: Directory {root_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    print(f"Adding license headers to files in: {root_dir}")
    modified = process_directory(root_dir)
    sys.exit(0 if modified >= 0 else 1)

