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

"""Storage backends for WebUI data persistence.

Protocols:
- JobStorageWriter: Per-job writing (path contains job_id, no job_id in methods)
- JobStorageReader: Cross-job reading (needs job_id to locate data)

Implementations:
- JobStorage: Per-job write storage (implements JobStorageWriter)
- PortalStorage: Read-only storage for Portal (implements JobStorageReader)
"""

from solstice.webui.storage.base import JobStorageReader, JobStorageWriter
from solstice.webui.storage.portal_storage import PortalStorage
from solstice.webui.storage.slatedb_storage import JobStorage

# Backward compatibility alias
SlateDBStorage = JobStorage

__all__ = [
    "JobStorageWriter",
    "JobStorageReader",
    "PortalStorage",
    "JobStorage",
    "SlateDBStorage",
]
