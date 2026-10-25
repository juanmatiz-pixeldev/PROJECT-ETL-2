"""Configuracion centralizada via variables de entorno (nunca credenciales en el codigo)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


@dataclass(frozen=True)
class Settings:
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("DATA_DIR", REPO_ROOT / "data")))
    rules_catalog: Path = field(
        default_factory=lambda: Path(os.getenv("RULES_CATALOG", REPO_ROOT / "gx" / "rules_catalog.yaml"))
    )
    sql_dir: Path = field(default_factory=lambda: Path(os.getenv("SQL_DIR", REPO_ROOT / "sql")))

    # Fuente primaria
    primary_source_path: Path = field(
        default_factory=lambda: Path(os.getenv("PRIMARY_SOURCE_PATH", REPO_ROOT / "data" / "source" / "rnt.csv"))
    )

    # API (Socrata SODA)
    api_base_url: str = field(default_factory=lambda: os.getenv("API_BASE_URL", "https://www.datos.gov.co"))
    api_dataset_id: str = field(default_factory=lambda: os.getenv("API_DATASET_ID", ""))
    api_app_token: str = field(default_factory=lambda: os.getenv("API_APP_TOKEN", ""))
    api_page_size: int = field(default_factory=lambda: _int("API_PAGE_SIZE", 5000))
    api_max_pages: int = field(default_factory=lambda: _int("API_MAX_PAGES", 500))
    api_connect_timeout_s: int = field(default_factory=lambda: _int("API_CONNECT_TIMEOUT_S", 10))
    api_read_timeout_s: int = field(default_factory=lambda: _int("API_READ_TIMEOUT_S", 60))
    api_where: str = field(default_factory=lambda: os.getenv("API_WHERE", ""))
    api_field_map: dict = field(default_factory=lambda: json.loads(os.getenv("API_FIELD_MAP", "{}") or "{}"))
    api_value_fields: tuple = field(
        default_factory=lambda: tuple(f.strip() for f in os.getenv("API_VALUE_FIELDS", "").split(",") if f.strip())
    )

    # Data Warehouse
    dw_url_override: str = field(default_factory=lambda: os.getenv("DW_URL", ""))
    dw_host: str = field(default_factory=lambda: os.getenv("DW_HOST", "localhost"))
    dw_port: int = field(default_factory=lambda: _int("DW_PORT", 5432))
    dw_user: str = field(default_factory=lambda: os.getenv("DW_USER", "dw_user"))
    dw_password: str = field(default_factory=lambda: os.getenv("DW_PASSWORD", ""))
    dw_name: str = field(default_factory=lambda: os.getenv("DW_NAME", "dw_turismo_ods8"))

    @property
    def dw_url(self) -> str:
        if self.dw_url_override:
            return self.dw_url_override
        return (
            f"postgresql+psycopg2://{self.dw_user}:{self.dw_password}"
            f"@{self.dw_host}:{self.dw_port}/{self.dw_name}"
        )

    # Rutas por lote (batch identity, Seccion 17)
    def raw_dir(self, source: str, batch_id: str) -> Path:
        return self.data_dir / "raw" / source / batch_id

    def prepared_dir(self, batch_id: str) -> Path:
        return self.data_dir / "prepared" / batch_id

    def validation_dir(self, batch_id: str) -> Path:
        return self.data_dir / "validation" / batch_id

    def log_dir(self, batch_id: str) -> Path:
        return self.data_dir / "logs" / batch_id


def get_settings() -> Settings:
    return Settings()
