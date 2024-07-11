import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from moriarty.envs import MORIARTY_MATRIX_API_URL_ENV, MORIARTY_MATRIX_TOKEN_ENV
from moriarty.log import logger


class Config(BaseModel):
    api_url: Optional[str] = None
    api_token: Optional[str] = None


class ConfigLoader:
    def __init__(self, config_path: str = "~/.config/moriarty/config.json") -> None:
        self.config_path = Path(config_path).expanduser()
        if not self.config_path.exists():
            self.config = Config()
            self.init_config_file()
        else:
            logger.info(f"Load config from: {self.config_path}")
            self.config = Config.model_validate_json(self.config_path.read_text())

    def get_api_url(self) -> str | None:
        return self.config.api_url or os.getenv(MORIARTY_MATRIX_API_URL_ENV)

    def get_api_token(self) -> str | None:
        return self.config.api_token or os.getenv(MORIARTY_MATRIX_TOKEN_ENV)

    def init_config_file(self) -> None:
        if self.config_path.exists():
            return

        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(Config().model_dump_json())
