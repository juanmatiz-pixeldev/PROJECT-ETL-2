"""Rerun seguro y carga atomica (Seccion 20.3): una carga fallida NO deja el DW a medias."""
import shutil
from pathlib import Path

import pandas as pd
import pgserver
import pytest
import sqlalchemy as sa

import mock_api
from etl.config import Settings
from etl.errors import DeterministicError
from etl.extract_api import extract_api
from etl.extract_primary import extract_primary
from etl.load import ensure_schema, load_dimensional
from etl.prepare_validate import validate_prepared
from etl.raw_validate import validate_raw
from etl.transform import transform_and_integrate

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("e2e")
    pg = pgserver.get_server(tmp / "pg")
    srv, _, url = mock_api.start("ok")
    sample = tmp / "rnt.csv"
    import subprocess, sys
    subprocess.check_call([sys.executable, str(ROOT / "scripts/make_sample_data.py"), str(sample), "--rows", "800"])
    mp = pytest.MonkeyPatch()
    for k, v in {"DW_URL": pg.get_uri().replace("postgresql://", "postgresql+psycopg2://"), "API_BASE_URL": url,
                 "API_DATASET_ID": "abcd-1234", "API_PAGE_SIZE": "30", "DATA_DIR": str(tmp / "data"),
                 "API_FIELD_MAP": '{"cod_municipio":"codigo_municipio","ano":"anio"}',
                 "API_VALUE_FIELDS": "poblacion_total", "PRIMARY_SOURCE_PATH": str(sample),
                 "SQL_DIR": str(ROOT / "sql"), "RULES_CATALOG": str(ROOT / "gx/rules_catalog.yaml")}.items():
        mp.setenv(k, v)
    s = Settings()
    ensure_schema(s)
    yield s
    mp.undo(); srv.shutdown()


def _run(s, batch):
    extract_primary(s, batch); extract_api(s, batch)
    validate_raw(s, "primary", batch); validate_raw(s, "api", batch)
    transform_and_integrate(s, batch); validate_prepared(s, batch)


def _counts(s):
    with sa.create_engine(s.dw_url).connect() as c:
        return {t: c.execute(sa.text(f"select count(*) from {t}")).scalar() for t in ("fact_turismo_anual", "fact_api_indicador")}


def test_rerun_same_batch_is_idempotent(env):
    _run(env, "rr1"); load_dimensional(env, "rr1", "run-a"); first = _counts(env)
    load_dimensional(env, "rr1", "run-b"); second = _counts(env)   # mismo lote, otra corrida
    assert first == second and first["fact_turismo_anual"] > 0


def test_failed_load_rolls_back_and_keeps_previous_state(env):
    before = _counts(env)
    _run(env, "rr2")
    f = env.prepared_dir("rr2") / "fact_turismo_anual.parquet"
    df = pd.read_parquet(f); df.loc[0, "codigo_municipio"] = 99999   # FK huerfana inyectada DESPUES del gate
    df.to_parquet(f, index=False)
    with pytest.raises(DeterministicError):
        load_dimensional(env, "rr2", "run-c")
    assert _counts(env) == before                                    # DW intacto: rollback completo


def test_load_refused_without_accepted_prepared_gate(env):
    _run(env, "rr3")
    (env.validation_dir("rr3") / "prepared.json").unlink()
    with pytest.raises(DeterministicError):
        load_dimensional(env, "rr3", "run-d")
