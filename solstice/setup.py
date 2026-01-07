#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Setup script for solstice package.
Uses pyproject.toml for metadata but provides custom build hooks for JAR files.
"""

import importlib.util
import os

from setuptools import setup

# Load _build_hooks directly without triggering raydp/__init__.py
_build_hooks_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "raydp", "_build_hooks.py"
)
spec = importlib.util.spec_from_file_location("_build_hooks", _build_hooks_path)
_build_hooks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_build_hooks)

BuildWithJars = _build_hooks.BuildWithJars
SdistWithJars = _build_hooks.SdistWithJars

setup(
    cmdclass={
        "build_py": BuildWithJars,
        "sdist": SdistWithJars,
    },
)



