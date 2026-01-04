// Copyright 2025 nurion team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pyo3::prelude::*;

mod broker;

use broker::{BrokerConfig, BrokerError, BrokerErrorKind, TansuBroker};

/// Tansu Python bindings - embedded Kafka-compatible broker
#[pymodule]
fn tansu_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BrokerConfig>()?;
    m.add_class::<BrokerError>()?;
    m.add_class::<BrokerErrorKind>()?;
    m.add_class::<TansuBroker>()?;
    Ok(())
}
