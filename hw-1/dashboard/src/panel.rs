use serde_json::{Value, json};

use crate::application::Endpoint;
use crate::dashboard::Datasource;

pub const REQUESTS_TOTAL: &str = "http_requests_total";
const REQUEST_DURATION_BUCKET: &str = "http_request_duration_seconds_bucket";
const PERCENTILES: [(f64, &str); 3] = [(0.50, "p50"), (0.95, "p95"), (0.99, "p99")];
const ERROR_STATUS: &str = r#"status=~"4..|5..""#;

pub struct GridPosition {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

impl GridPosition {
    pub const fn new(x: u32, y: u32, width: u32, height: u32) -> Self {
        Self {
            x,
            y,
            width,
            height,
        }
    }

    pub fn to_json(&self) -> Value {
        json!({ "x": self.x, "y": self.y, "w": self.width, "h": self.height })
    }
}

pub fn row(id: Option<u32>, title: &str, position: GridPosition) -> Value {
    json!({
        "id": id,
        "type": "row",
        "title": title,
        "collapsed": false,
        "gridPos": position.to_json(),
        "panels": [],
    })
}

pub fn latency(endpoint: &Endpoint, datasource: &Datasource) -> Value {
    let targets = (0..)
        .zip(PERCENTILES)
        .map(|(index, (quantile, legend))| {
            target(
                index,
                &format!(
                    "histogram_quantile({quantile}, sum by (le) (rate({REQUEST_DURATION_BUCKET}{}[$__rate_interval])))",
                    selector(endpoint, None),
                ),
                legend,
            )
        })
        .collect();
    timeseries("Latency", "s", targets, datasource)
}

pub fn rps(endpoint: &Endpoint, datasource: &Datasource) -> Value {
    let query = format!(
        "sum(rate({REQUESTS_TOTAL}{}[$__rate_interval]))",
        selector(endpoint, None)
    );
    timeseries("RPS", "reqps", vec![target(0, &query, "RPS")], datasource)
}

pub fn errors(endpoint: &Endpoint, datasource: &Datasource) -> Value {
    let query = format!(
        "sum by (status) (rate({REQUESTS_TOTAL}{}[$__rate_interval]))",
        selector(endpoint, Some(ERROR_STATUS)),
    );
    timeseries(
        "Errors by status code",
        "reqps",
        vec![target(0, &query, "{{status}}")],
        datasource,
    )
}

fn selector(endpoint: &Endpoint, extra: Option<&str>) -> String {
    let extra = extra.map(|matcher| format!(", {matcher}")).unwrap_or_default();
    format!(
        r#"{{method="{}", endpoint="{}", datacenter=~"$datacenter", pod=~"$pod"{extra}}}"#,
        escape(&endpoint.method),
        escape(&endpoint.path),
    )
}

fn escape(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', r#"\""#)
}

fn timeseries(title: &str, unit: &str, targets: Vec<Value>, datasource: &Datasource) -> Value {
    json!({
        "type": "timeseries",
        "title": title,
        "datasource": datasource.to_json(),
        "targets": targets,
        "fieldConfig": { "defaults": { "unit": unit }, "overrides": [] },
        "options": {
            "legend": { "displayMode": "list", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" },
        },
    })
}

fn target(index: u8, expr: &str, legend: &str) -> Value {
    json!({
        "refId": char::from(b'A' + index).to_string(),
        "expr": expr,
        "legendFormat": legend,
        "range": true,
    })
}
