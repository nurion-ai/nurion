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
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread::JoinHandle;
use tokio::runtime::Runtime;
use url::Url;
use uuid::Uuid;

use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_storage::StorageContainer;

/// Broker error kinds
#[pyclass]
#[derive(Clone)]
pub struct BrokerErrorKind;

#[pymethods]
impl BrokerErrorKind {
    #[classattr]
    const BIND_FAILED: &'static str = "bind_failed";
    
    #[classattr]
    const STORAGE_ERROR: &'static str = "storage_error";
    
    #[classattr]
    const PROTOCOL_ERROR: &'static str = "protocol_error";
    
    #[classattr]
    const INTERNAL_ERROR: &'static str = "internal_error";
    
    #[classattr]
    const SHUTDOWN_TIMEOUT: &'static str = "shutdown_timeout";
}

/// Broker error information
#[pyclass]
#[derive(Clone)]
pub struct BrokerError {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub is_recoverable: bool,
}

#[pymethods]
impl BrokerError {
    #[new]
    fn new(kind: String, message: String, is_recoverable: bool) -> Self {
        BrokerError {
            kind,
            message,
            is_recoverable,
        }
    }
    
    fn __repr__(&self) -> String {
        format!(
            "BrokerError(kind='{}', message='{}', is_recoverable={})",
            self.kind, self.message, self.is_recoverable
        )
    }
}

/// Broker configuration
#[pyclass]
#[derive(Clone)]
pub struct BrokerConfig {
    #[pyo3(get, set)]
    pub storage_url: String,
    #[pyo3(get, set)]
    pub listener_port: u16,
    #[pyo3(get, set)]
    pub advertised_host: String,
}

#[pymethods]
impl BrokerConfig {
    #[new]
    #[pyo3(signature = (storage_url, listener_port, advertised_host))]
    fn new(storage_url: String, listener_port: u16, advertised_host: String) -> Self {
        BrokerConfig {
            storage_url,
            listener_port,
            advertised_host,
        }
    }
}

/// Tansu Broker - embedded Kafka-compatible broker
#[pyclass]
pub struct TansuBroker {
    config: BrokerConfig,
    handle: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
    event_handler: Option<PyObject>,
    actual_port: Arc<std::sync::Mutex<Option<u16>>>,
}

#[pymethods]
impl TansuBroker {
    #[new]
    #[pyo3(signature = (config, event_handler=None))]
    fn new(config: BrokerConfig, event_handler: Option<PyObject>) -> Self {
        TansuBroker {
            config,
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
            event_handler,
            actual_port: Arc::new(std::sync::Mutex::new(None)),
        }
    }
    
    /// Run broker in blocking mode (blocks current thread)
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Broker is already running"
            ));
        }
        
        self.running.store(true, Ordering::SeqCst);
        let config = self.config.clone();
        let handler = self.event_handler.as_ref().map(|h| h.clone_ref(py));
        let running = self.running.clone();
        let actual_port = self.actual_port.clone();
        
        // Release GIL and run broker in current thread
        py.allow_threads(|| {
            let rt = Runtime::new().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
            })?;
            
            rt.block_on(async {
                match Self::run_real_broker(&config, running.clone()).await {
                    Ok(port) => {
                        *actual_port.lock().unwrap() = Some(port);
                        
                        // Trigger on_started callback
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let _ = h.call_method1(py, "on_started", (port,));
                            });
                        }
                        
                        // Keep running until stopped
                        while running.load(Ordering::SeqCst) {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        
                        // Trigger on_stopped callback
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let _ = h.call_method0(py, "on_stopped");
                            });
                        }
                    }
                    Err(e) => {
                        running.store(false, Ordering::SeqCst);
                        
                        // Trigger on_fatal callback
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let error = BrokerError::new(
                                    BrokerErrorKind::BIND_FAILED.to_string(),
                                    e,
                                    false,
                                );
                                let _ = h.call_method1(py, "on_fatal", (error,));
                            });
                        }
                    }
                }
            });
            
            Ok(())
        })
    }
    
    /// Start broker in non-blocking mode (background thread)
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Broker is already running"
            ));
        }
        
        self.running.store(true, Ordering::SeqCst);
        let config = self.config.clone();
        let handler = self.event_handler.as_ref().map(|h| h.clone_ref(py));
        let running = self.running.clone();
        let actual_port = self.actual_port.clone();
        
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new().expect("Failed to create runtime");
            
            rt.block_on(async {
                match Self::run_real_broker(&config, running.clone()).await {
                    Ok(port) => {
                        *actual_port.lock().unwrap() = Some(port);
                        
                        // Trigger on_started callback
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let _ = h.call_method1(py, "on_started", (port,));
                            });
                        }
                        
                        // Keep running until stopped
                        while running.load(Ordering::SeqCst) {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        
                        // Trigger on_stopped callback
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let _ = h.call_method0(py, "on_stopped");
                            });
                        }
                    }
                    Err(e) => {
                        running.store(false, Ordering::SeqCst);
                        
                        // Trigger on_fatal callback
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let error = BrokerError::new(
                                    BrokerErrorKind::BIND_FAILED.to_string(),
                                    e,
                                    false,
                                );
                                let _ = h.call_method1(py, "on_fatal", (error,));
                            });
                        }
                    }
                }
            });
        });
        
        self.handle = Some(handle);
        Ok(())
    }
    
    /// Stop the broker
    fn stop(&mut self) -> PyResult<()> {
        // Signal the broker to stop
        self.running.store(false, Ordering::SeqCst);
        
        // Drop the handle without joining - the thread will clean up when it detects running=false
        // This avoids blocking and allows the Python caller to return immediately
        // The OS will reclaim resources (including the port) when the thread terminates
        self.handle.take();
        
        Ok(())
    }
    
    /// Check if broker is running
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    
    /// Get the actual port the broker is listening on
    fn get_port(&self) -> Option<u16> {
        *self.actual_port.lock().unwrap()
    }
    
    /// Wait for broker thread to finish (non-blocking mode only)
    fn wait(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            py.allow_threads(|| {
                handle.join().map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err("Failed to join broker thread")
                })
            })?;
        }
        Ok(())
    }
    
    /// Set or update event handler
    fn set_event_handler(&mut self, handler: PyObject) {
        self.event_handler = Some(handler);
    }
}

impl TansuBroker {
    /// Real Tansu broker implementation using tansu-broker crate
    async fn run_real_broker(config: &BrokerConfig, _running: Arc<AtomicBool>) -> Result<u16, String> {
        // Parse URLs
        let storage_url = Url::parse(&config.storage_url)
            .map_err(|e| format!("Invalid storage URL: {}", e))?;
        
        let listener_url = Url::parse(&format!("tcp://0.0.0.0:{}", config.listener_port))
            .map_err(|e| format!("Invalid listener URL: {}", e))?;
        
        let advertised_url = Url::parse(&format!("tcp://{}:{}", config.advertised_host, config.listener_port))
            .map_err(|e| format!("Invalid advertised URL: {}", e))?;
        
        // Create broker instance
        let cluster_id = "tansu_cluster".to_string();
        let incarnation_id = Uuid::now_v7();
        
        let broker = Broker::<Controller<StorageContainer>, StorageContainer>::builder()
            .cluster_id(cluster_id)
            .node_id(NODE_ID)
            .incarnation_id(incarnation_id)
            .listener(listener_url.clone())
            .advertised_listener(advertised_url)
            .storage(storage_url)
            .build()
            .await
            .map_err(|e| format!("Failed to build broker: {:?}", e))?;
        
        // Start broker in a separate task
        let _broker_handle = tokio::spawn(async move {
            broker.main().await
        });
        
        // Wait a bit for broker to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
        // Return the actual port
        Ok(config.listener_port)
    }
}
