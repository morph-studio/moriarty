from __future__ import annotations

import os
from typing import Any, Callable

from pydantic import BaseModel

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from moriarty.envs import *


def get_config() -> Config:
    default = {
        DB_HOST_ENV: "localhost",
        DB_PORT_ENV: 5432,
        DB_USER_ENV: "postgres",
        DB_PASSWORD_ENV: "Th3PAssW0rd!1.",
        DB_DATABASE_ENV: "moriarty-matrix",
        REDIS_HOST_ENV: "localhost",
        REDIS_PORT_ENV: 6379,
        REDIS_DB_ENV: 0,
        REDIS_TLS_ENV: False,
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
        host = os.getenv(DB_HOST_ENV, default.get(DB_HOST_ENV, "localhost"))
        port = int(os.getenv(DB_PORT_ENV, default.get(DB_PORT_ENV, 5432)))
        user = os.getenv(DB_USER_ENV, default.get(DB_USER_ENV, "postgres"))
        password = os.getenv(DB_PASSWORD_ENV, default.get(DB_PASSWORD_ENV, "postgres"))
        database = os.getenv(DB_DATABASE_ENV, default.get(DB_DATABASE_ENV, "postgres"))
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
        host = os.getenv(REDIS_HOST_ENV, default.get(REDIS_HOST_ENV, "localhost"))
        port = int(os.getenv(REDIS_PORT_ENV, default.get(REDIS_PORT_ENV, 6379)))
        password = os.getenv(REDIS_PASSWORD_ENV, default.get(REDIS_PASSWORD_ENV, ""))
        db = int(os.getenv(REDIS_DB_ENV, default.get(REDIS_DB_ENV, 6)))
        tls = (
            os.getenv(REDIS_TLS_ENV) in ["True", "true"]
            if os.getenv(REDIS_TLS_ENV)
            else default.get(REDIS_TLS_ENV, False)
        )
        cluster = (
            os.getenv(REDIS_CLUSTER_ENV) in ["True", "true"]
            if os.getenv(REDIS_CLUSTER_ENV)
            else default.get(REDIS_CLUSTER_ENV, False)
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
