use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use metrics::Unit;
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};

pub const REQUESTS_TOTAL: &str = "http_requests_total";
pub const REQUEST_DURATION_SECONDS: &str = "http_request_duration_seconds";

const DURATION_BUCKETS: &[f64] = &[
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.5, 5.0, 10.0,
];

const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct Instance {
    pub pod: String,
    pub datacenter: String,
}

pub fn install(instance: Instance) -> Result<PrometheusHandle, BuildError> {
    let handle = PrometheusBuilder::new()
        .add_global_label("pod", instance.pod)
        .add_global_label("datacenter", instance.datacenter)
        .set_buckets_for_metric(
            Matcher::Full(REQUEST_DURATION_SECONDS.to_owned()),
            DURATION_BUCKETS,
        )?
        .install_recorder()?;

    metrics::describe_counter!(
        REQUESTS_TOTAL,
        "Total number of HTTP requests handled, by endpoint and response status."
    );
    metrics::describe_histogram!(
        REQUEST_DURATION_SECONDS,
        Unit::Seconds,
        "HTTP request latency, by endpoint and response status."
    );

    let upkeep = handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(UPKEEP_INTERVAL);
        loop {
            interval.tick().await;
            upkeep.run_upkeep();
        }
    });

    Ok(handle)
}

pub async fn track(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = request.method().to_string();
    let endpoint = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |path| path.as_str().to_owned());

    let response = next.run(request).await;

    let labels = [
        ("method", method),
        ("endpoint", endpoint),
        ("status", response.status().as_u16().to_string()),
    ];
    metrics::counter!(REQUESTS_TOTAL, &labels).increment(1);
    metrics::histogram!(REQUEST_DURATION_SECONDS, &labels).record(start.elapsed().as_secs_f64());

    response
}
