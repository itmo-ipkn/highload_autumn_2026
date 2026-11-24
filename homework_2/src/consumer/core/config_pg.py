from pydantic import Field
from pydantic_settings import BaseSettings

DATABASE_URL_TEMPLATE = (
    "{dialect}://{user}:{password}@{host}:{port}/{database}"
)


class PostgresSettings(BaseSettings):
    dialect: str = Field(..., alias="POSTGRES_DIALECT")
    user: str = Field(..., alias="POSTGRES_USER")
    password: str = Field(..., alias="POSTGRES_PASSWORD")
    host: str = Field(..., alias="POSTGRES_HOST")
    port: str = Field(..., alias="POSTGRES_PORT")
    database: str = Field(..., alias="POSTGRES_DB")
