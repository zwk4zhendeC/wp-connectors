#![cfg(feature = "doris")]
// Wrapper test to include tests under tests/kafka/ and shared helpers in tests/common.rs

#[path = "common/mod.rs"]
mod common;

#[path = "doris/common.rs"]
mod doris_common;

#[path = "doris/integration_tests.rs"]
mod integration_tests;

#[path = "doris/performance_tests.rs"]
mod performance_tests;

// #[path = "doris/test_1.rs"]
// mod t1;
