use rand::RngExt;
use serde::Serialize;

const MISSING_ID_BASE: u32 = 1_000_000_000;

#[derive(Debug, Clone, Serialize)]
pub struct NewItem {
    name: String,
    price: i64,
    sold: u32,
}

impl NewItem {
    pub fn random() -> Self {
        let mut rng = rand::rng();
        Self {
            name: format!("item-{}", rng.random_range(1..=1_000_000)),
            price: rng.random_range(1..=10_000),
            sold: rng.random_range(0..=50),
        }
    }

    pub fn invalid() -> Self {
        Self {
            price: -1,
            ..Self::random()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ItemIds {
    seeded: u32,
}

impl ItemIds {
    pub const fn new(seeded: u32) -> Self {
        Self { seeded }
    }

    pub fn existing(self) -> u32 {
        rand::rng().random_range(1..=self.seeded)
    }

    pub fn missing() -> u32 {
        MISSING_ID_BASE + rand::rng().random_range(0..1_000_000)
    }
}
