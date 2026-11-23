use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use super::model::{Item, ItemId, NewItem};
use crate::db::{Database, Error};

pub async fn post_item(
    State(database): State<Database>,
    Json(item): Json<NewItem>,
) -> Result<(StatusCode, Json<Item>), Error> {
    let item = Item::create(database.pool(), &item).await?;
    Ok((StatusCode::CREATED, Json(item)))
}

pub async fn get_item(
    State(database): State<Database>,
    Path(id): Path<ItemId>,
) -> Result<Response, Error> {
    Ok(match Item::get(database.pool(), id).await? {
        Some(item) => Json(item).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    })
}

pub async fn put_item(
    State(database): State<Database>,
    Path(id): Path<ItemId>,
    Json(item): Json<NewItem>,
) -> Result<Response, Error> {
    Ok(match Item::update(database.pool(), id, &item).await? {
        Some(item) => Json(item).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    })
}
