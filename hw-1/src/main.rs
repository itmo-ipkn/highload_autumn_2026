use std::env;

use simple_crud::api;
use simple_crud::db::Database;
use simple_crud::failure::FailureRate;
use simple_crud::telemetry::{self, Instance};
use tokio::net::TcpListener;

fn env_or_unknown(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| {
        eprintln!("{name} is not set, using \"unknown\"");
        "unknown".to_owned()
    })
}

#[tokio::main]
async fn main() {
    let instance = Instance {
        pod: env_or_unknown("POD_ID"),
        datacenter: env_or_unknown("DATACENTER"),
    };
    let failure_rate = env::var("FAILURE_RATE")
        .map_or(Ok(FailureRate::default()), |rate| rate.parse())
        .expect("FAILURE_RATE");

    let metrics = telemetry::install(instance).unwrap();
    let database = Database::in_memory().await.unwrap();

    let app = api::router(database, metrics, failure_rate);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
