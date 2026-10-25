"""Smoke E2E sin Docker: Postgres embebido (pgserver) + API mock. Ejecuta las MISMAS funciones que usa el DAG.
Uso:  PYTHONPATH=src python tests/e2e_local.py [none|api_null_keys|primary_drop_column] [batch_id]
"""
import os
import sys
import tempfile
from pathlib import Path

import pgserver

sys.path.insert(0, str(Path(__file__).parent))
import mock_api  # noqa: E402

fault = sys.argv[1] if len(sys.argv) > 1 else "none"
batch = sys.argv[2] if len(sys.argv) > 2 else "e2etest1"
pgdir = Path(os.getenv("PGDIR", tempfile.mkdtemp(prefix="pgdata_")))
pg = pgserver.get_server(pgdir)
uri = pg.get_uri().replace("postgresql://", "postgresql+psycopg2://")

srv, state, url = mock_api.start()
os.environ.update({
    "DW_URL": uri, "API_BASE_URL": url, "API_DATASET_ID": "abcd-1234", "API_PAGE_SIZE": "20",
    "API_FIELD_MAP": '{"cod_municipio":"codigo_municipio","ano":"anio"}', "API_VALUE_FIELDS": "poblacion_total",
    "PRIMARY_SOURCE_PATH": "data/source/rnt_sample.csv", "DATA_DIR": os.getenv("DATA_DIR", "data"),
})

from etl.config import get_settings  # noqa: E402
from etl.errors import CriticalValidationError  # noqa: E402
from etl.extract_api import extract_api  # noqa: E402
from etl.extract_primary import extract_primary  # noqa: E402
from etl.load import ensure_schema, load_dimensional  # noqa: E402
from etl.prepare_validate import validate_prepared  # noqa: E402
from etl.raw_validate import validate_raw  # noqa: E402
from etl.transform import transform_and_integrate  # noqa: E402

s = get_settings()
ensure_schema(s)
print("== extract"); print(extract_primary(s, batch, fault)); print(extract_api(s, batch, fault))
try:
    print("== validate_primary_raw"); print(validate_raw(s, "primary", batch, "run1", "validate_primary_raw"))
    print("== validate_api_raw"); print(validate_raw(s, "api", batch, "run1", "validate_api_raw"))
except CriticalValidationError as e:
    print("BLOQUEADO:", e); sys.exit(2)
print("== transform"); print(transform_and_integrate(s, batch)["counts"])
print("== validate_prepared"); print(validate_prepared(s, batch, "run1"))
print("== load"); print(load_dimensional(s, batch, "run1"))
print("PG_URI", uri)
