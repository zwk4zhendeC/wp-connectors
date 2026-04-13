use educe::Educe;
use serde::Deserialize;
use serde::Serialize;

/// Prometheus configuration for metrics
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct Prometheus {
    #[educe(Default = "0.0.0.0:9090")]
    pub endpoint: String,
}
