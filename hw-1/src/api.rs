mod registry;

use std::sync::Arc;

use axum::extract::{FromRef, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router, middleware};
use metrics_exporter_prometheus::PrometheusHandle;

use crate::db::Database;
use crate::failure::{self, FailureRate};
use crate::{item, telemetry};

pub use registry::{HandlerInfo, Registry};

#[derive(Clone)]
struct AppState {
    database: Database,
    metrics: PrometheusHandle,
    handlers: Arc<[HandlerInfo]>,
}

impl FromRef<AppState> for Database {
    fn from_ref(state: &AppState) -> Self {
        state.database.clone()
    }
}

impl FromRef<AppState> for PrometheusHandle {
    fn from_ref(state: &AppState) -> Self {
        state.metrics.clone()
    }
}

impl FromRef<AppState> for Arc<[HandlerInfo]> {
    fn from_ref(state: &AppState) -> Self {
        state.handlers.clone()
    }
}

pub fn router(database: Database, metrics: PrometheusHandle, failure_rate: FailureRate) -> Router {
    let (router, handlers) = item::register(Registry::default()).into_parts();

    let state = AppState {
        database,
        metrics,
        handlers: handlers.into(),
    };

    router
        .route_layer(middleware::from_fn_with_state(failure_rate, failure::inject))
        .route_layer(middleware::from_fn(telemetry::track))
        .route("/health", get(get_healthcheck))
        .route("/metrics", get(get_metrics))
        .route("/handlers", get(get_handlers))
        .with_state(state)
}

async fn get_healthcheck() -> StatusCode {
    StatusCode::OK
}

async fn get_metrics(State(metrics): State<PrometheusHandle>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics.render(),
    )
}

async fn get_handlers(State(handlers): State<Arc<[HandlerInfo]>>) -> Json<Arc<[HandlerInfo]>> {
    Json(handlers)
}
