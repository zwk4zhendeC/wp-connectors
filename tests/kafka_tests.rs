#![cfg(feature = "kafka")]

#[path = "common/mod.rs"]
mod common;

#[path = "kafka/common.rs"]
mod kafka_common;

#[path = "kafka/integration_tests.rs"]
mod integration_tests;

#[path = "kafka/performance_tests.rs"]
mod performance_tests;
