use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sqlx::{
    SqlitePool, migrate,
    migrate::MigrateError,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

#[derive(Clone)]
pub struct Database(SqlitePool);

#[derive(Debug)]
#[allow(dead_code)]
pub struct Error(sqlx::Error);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something wrong with database",
        )
            .into_response()
    }
}

impl Database {
    async fn initialize(pool: SqlitePool) -> Result<Self, Error> {
        migrate!().run(&pool).await?;
        Ok(Self(pool))
    }

    pub async fn in_memory() -> Result<Self, Error> {
        let options = SqliteConnectOptions::new().in_memory(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Self::initialize(pool).await
    }

    pub const fn pool(&self) -> &SqlitePool {
        &self.0
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        eprintln!("{error}");
        Self(error)
    }
}

impl From<MigrateError> for Error {
    fn from(error: MigrateError) -> Self {
        Self::from(sqlx::Error::from(error))
    }
}
