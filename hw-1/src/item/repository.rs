use sqlx::SqliteExecutor;

use super::model::{Item, ItemId, NewItem, Price};
use crate::db::Error;

impl Item {
    pub async fn create(executor: impl SqliteExecutor<'_>, item: &NewItem) -> Result<Self, Error> {
        let item = sqlx::query_as!(
            Item,
            r#"INSERT INTO item (name, price, sold) VALUES (?, ?, ?)
               RETURNING id AS "id: ItemId", name, price AS "price: Price", sold AS "sold: u32""#,
            item.name,
            item.price,
            item.sold,
        )
        .fetch_one(executor)
        .await?;
        Ok(item)
    }

    pub async fn get(executor: impl SqliteExecutor<'_>, id: ItemId) -> Result<Option<Self>, Error> {
        let item = sqlx::query_as!(
            Item,
            r#"SELECT id AS "id: ItemId", name, price AS "price: Price", sold AS "sold: u32"
               FROM item WHERE id = ?"#,
            id,
        )
        .fetch_optional(executor)
        .await?;
        Ok(item)
    }

    pub async fn update(
        executor: impl SqliteExecutor<'_>,
        id: ItemId,
        item: &NewItem,
    ) -> Result<Option<Self>, Error> {
        let item = sqlx::query_as!(
            Item,
            r#"UPDATE item SET name = ?, price = ?, sold = ? WHERE id = ?
               RETURNING id AS "id: ItemId", name, price AS "price: Price", sold AS "sold: u32""#,
            item.name,
            item.price,
            item.sold,
            id,
        )
        .fetch_optional(executor)
        .await?;
        Ok(item)
    }
}
