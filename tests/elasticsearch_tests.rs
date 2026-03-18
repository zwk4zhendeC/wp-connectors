#![cfg(feature = "elasticsearch")]

#[path = "common/mod.rs"]
mod common;

#[path = "elasticsearch/common.rs"]
mod elasticsearch_common;

#[path = "elasticsearch/integration_tests.rs"]
mod integration_tests;

#[path = "elasticsearch/performance_tests.rs"]
mod performance_tests;
