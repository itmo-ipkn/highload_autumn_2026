mod handlers;
mod model;
mod repository;

use axum::extract::FromRef;
use axum::http::Method;

use crate::api::Registry;
use crate::db::Database;

pub use model::{Item, ItemId, NewItem, Price};

pub fn register<S>(registry: Registry<S>) -> Registry<S>
where
    S: Clone + Send + Sync + 'static,
    Database: FromRef<S>,
{
    registry
        .route(Method::POST, "/items", handlers::post_item)
        .route(Method::GET, "/items/{id}", handlers::get_item)
        .route(Method::PUT, "/items/{id}", handlers::put_item)
}
