#![cfg(feature = "clickhouse")]

#[path = "common/mod.rs"]
mod common;

#[path = "clickhouse/common.rs"]
mod clickhouse_common;

#[path = "clickhouse/integration_tests.rs"]
mod integration_tests;

#[path = "clickhouse/performance_tests.rs"]
mod performance_tests;
