use educe::Educe;
use serde::Deserialize;
use serde::Serialize;
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct VictoriaMetric {
    #[educe(Default = "http://127.0.0.1:8428/api/v1/import/prometheus")]
    pub insert_url: String,
    #[educe(Default = 1.0)]
    pub flush_interval_secs: f64,
}
