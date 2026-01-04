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

"""Tansu Python bindings - embedded Kafka-compatible broker."""

from typing import Optional

# Import Rust implementations
from tansu_py.tansu_py import (  # type: ignore
    BrokerConfig as _BrokerConfig,
    BrokerError as _BrokerError,
    BrokerErrorKind as _BrokerErrorKind,
    TansuBroker as _TansuBroker,
)

# Re-export for better IDE support
BrokerConfig = _BrokerConfig
BrokerError = _BrokerError
BrokerErrorKind = _BrokerErrorKind
TansuBroker = _TansuBroker


class BrokerEventHandler:
    """Base class for broker event callbacks.
    
    Users should subclass this and override the methods they need.
    """
    
    def on_started(self, port: int) -> None:
        """Called when broker successfully starts.
        
        Args:
            port: The actual port the broker is listening on
        """
        pass
    
    def on_stopped(self) -> None:
        """Called when broker stops normally."""
        pass
    
    def on_error(self, error: BrokerError) -> None:
        """Called when a recoverable error occurs.
        
        Args:
            error: The error information
        """
        pass
    
    def on_fatal(self, error: BrokerError) -> None:
        """Called when a fatal error occurs (broker will crash).
        
        Args:
            error: The error information
        """
        pass


__all__ = [
    "BrokerConfig",
    "BrokerError",
    "BrokerErrorKind",
    "TansuBroker",
    "BrokerEventHandler",
]

