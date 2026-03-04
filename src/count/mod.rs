//! Count sink implementation for wp-connectors
//!
//! 该sink用作调试

mod factory;
mod sink;

pub use factory::CountSinkFactory;
pub use sink::CountSink;
