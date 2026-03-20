#![cfg(feature = "victorialogs")]

#[path = "common/mod.rs"]
mod common;

#[path = "victorialogs/common.rs"]
mod victorialogs_common;

#[path = "victorialogs/integration_tests.rs"]
mod integration_tests;

#[path = "victorialogs/performance_tests.rs"]
mod performance_tests;
