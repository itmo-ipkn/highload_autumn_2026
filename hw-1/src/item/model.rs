use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct ItemId(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct Price(u32);

#[derive(Debug, Clone, Serialize)]
pub struct Item {
    pub id: ItemId,
    pub name: String,
    pub price: Price,
    pub sold: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NewItem {
    pub name: String,
    pub price: Price,
    #[serde(default)]
    pub sold: u32,
}
