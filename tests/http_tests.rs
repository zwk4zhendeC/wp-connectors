#![cfg(feature = "http")]

#[path = "common/mod.rs"]
mod common;

#[path = "http/common.rs"]
mod http_common;

#[path = "http/integration_tests.rs"]
mod integration_tests;

#[path = "http/performance_tests.rs"]
mod performance_tests;
