#![cfg(feature = "postgres")]

#[path = "common/mod.rs"]
mod common;

#[path = "postgresql/common.rs"]
mod postgresql_common;

#[path = "postgresql/integration_tests.rs"]
mod integration_tests;

#[path = "postgresql/performance_tests.rs"]
mod performance_tests;
