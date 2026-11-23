use std::fmt;
use std::str::FromStr;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct FailureRate(f64);

#[derive(Debug)]
pub struct InvalidFailureRate(String);

impl FromStr for FailureRate {
    type Err = InvalidFailureRate;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.parse::<f64>() {
            Ok(rate) if (0.0..=1.0).contains(&rate) => Ok(Self(rate)),
            _ => Err(InvalidFailureRate(value.to_owned())),
        }
    }
}

impl fmt::Display for InvalidFailureRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failure rate must be a number in [0, 1], got {:?}", self.0)
    }
}

impl std::error::Error for InvalidFailureRate {}

pub async fn inject(State(rate): State<FailureRate>, request: Request, next: Next) -> Response {
    if rand::random_bool(rate.0) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    next.run(request).await
}
