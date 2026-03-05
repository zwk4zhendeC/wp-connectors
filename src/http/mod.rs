//! HTTP sink implementation for wp-connectors.

mod config;
mod factory;
mod sink;

pub use config::HTTPSinkConfig;
pub use factory::HTTPSinkFactory;
pub use sink::HTTPSink;
