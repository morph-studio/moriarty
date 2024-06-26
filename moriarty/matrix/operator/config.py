from __future__ import annotations

import os
from typing import Any, Callable

from pydantic import BaseModel

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache


def get_config() -> Config:
    default = {
        "DB_HOST": "localhost",
        "DB_PORT": 5432,
        "DB_USER": "postgres",
        "DB_PASSWORD": "Th3PAssW0rd!1.",
        "DB_DATABASE": "moriarty-matrix",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": 6379,
        "REDIS_DB": 6,
        "REDIS_TLS": False,
    }

    return Config.from_env(default)


class DBConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls, default: dict[str, str]) -> "DBConfig":
        host = os.getenv("DB_HOST", default.get("DB_HOST", "localhost"))
        port = int(os.getenv("DB_PORT", default.get("DB_PORT", 5432)))
        user = os.getenv("DB_USER", default.get("DB_USER", "postgres"))
        password = os.getenv("DB_PASSWORD", default.get("DB_PASSWORD", "postgres"))
        database = os.getenv("DB_DATABASE", default.get("DB_DATABASE", "postgres"))
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

    def get_db_url(self):
        return f"{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_log_db_url(self):
        return self.get_db_url().replace(self.password, "********")


class RedisConfig(BaseModel):
    host: str
    port: int
    password: str
    db: int
    tls: bool
    cluster: bool

    @classmethod
    def from_env(cls, default: dict[str, str]) -> "RedisConfig":
        host = os.getenv("REDIS_HOST", default.get("REDIS_HOST", "localhost"))
        port = int(os.getenv("REDIS_PORT", default.get("REDIS_PORT", 6379)))
        password = os.getenv("REDIS_PASSWORD", default.get("REDIS_PASSWORD", ""))
        db = int(os.getenv("REDIS_DB", default.get("REDIS_DB", 6)))
        tls = (
            os.getenv("REDIS_TLS") in ["True", "true"]
            if os.getenv("REDIS_TLS")
            else default.get("REDIS_TLS", False)
        )
        cluster = (
            os.getenv("REDIS_CLUSTER") in ["True", "true"]
            if os.getenv("REDIS_CLUSTER")
            else default.get("REDIS_CLUSTER", False)
        )
        return cls(
            host=host,
            port=port,
            password=password,
            db=db,
            tls=tls,
            cluster=cluster,
        )

    def get_redis_url(self):
        if not self.cluster:
            url = f"{self.host}:{self.port}/{self.db}"
        else:
            # Ignore db
            url = f"{self.host}:{self.port}"

        if self.tls:
            return f"rediss://{url}"
        else:
            return f"redis://{url}"


class Config(BaseModel):
    db: DBConfig
    redis: RedisConfig

    """
    Cache for latest initialized object.
    """

    @classmethod
    def from_env(cls, default: None | dict[str, Any]) -> "Config":
        if default is None:
            default = {}
        return cls(
            db=DBConfig.from_env(default=default),
            redis=RedisConfig.from_env(default=default),
        )
