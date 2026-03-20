#![cfg(feature = "mysql")]

#[path = "common/mod.rs"]
mod common;

#[path = "mysql/common.rs"]
mod mysql_common;

#[path = "mysql/integration_tests.rs"]
mod integration_tests;

#[path = "mysql/performance_tests.rs"]
mod performance_tests;
