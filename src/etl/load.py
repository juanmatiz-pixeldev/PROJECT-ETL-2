"""Carga dimensional al DW PostgreSQL. SOLO carga datos aceptados por el gate `prepared`.

Estrategia de rerun seguro (Seccion 20.3): REEMPLAZO ATOMICO del contenido en UNA transaccion
  BEGIN; advisory lock; TRUNCATE (todas las tablas) ; COPY dims -> COPY hechos ; conciliacion ; auditoria ; COMMIT
 - Re-ejecutar el mismo lote produce exactamente el mismo estado final (idempotente, sin duplicados).
 - Si algo falla a mitad, ROLLBACK: el DW conserva el estado anterior intacto (sin cargas parciales).
 - El advisory lock evita dos cargas concurrentes.
Justificacion: las fuentes son snapshots completos del registro (no incrementales) y el DW es analitico.
"""
from __future__ import annotations

import io
import logging

import pandas as pd
import psycopg2
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError

from .batch import read_json
from .config import Settings
from .errors import DeterministicError, TransientError
from .observability import get_engine, log_event

log = logging.getLogger("etl.load")

LOAD_ORDER = ["dim_ubicacion", "dim_categoria", "dim_prestador", "dim_tiempo", "fact_turismo_anual", "fact_api_indicador"]
LOCK_KEY = 727274  # clave arbitraria de advisory lock para la carga del DW


def ensure_schema(settings: Settings) -> None:
    """Crea (si no existen) los esquemas ops y dimensional. Idempotente."""
    scripts = [settings.sql_dir / "01_ops.sql", settings.sql_dir / "02_create_dw.sql"]
    try:
        with get_engine(settings).begin() as conn:
            for s in scripts:
                conn.exec_driver_sql(s.read_text(encoding="utf-8"))
    except OperationalError as exc:
        raise TransientError(f"DW no disponible: {exc.orig}") from exc


def _copy(conn, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cols = ", ".join(df.columns)
    cur = conn.connection.cursor()
    cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)


def load_dimensional(settings: Settings, batch_id: str, run_id: str | None = None) -> dict:
    # Defensa en profundidad: sin gate `prepared` aceptado NO se carga (ademas de la dependencia de Airflow)
    vpath = settings.validation_dir(batch_id) / "prepared.json"
    if not vpath.exists() or read_json(vpath)["decision"] == "stop":
        raise DeterministicError(f"Gate prepared no aceptado para {batch_id}: carga bloqueada.")

    d = settings.prepared_dir(batch_id)
    manifest = read_json(d / "manifest.json")
    tables = {t: pd.read_parquet(d / f"{t}.parquet") for t in LOAD_ORDER}
    expected = {t: len(df) for t, df in tables.items()}

    try:
        with get_engine(settings).begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": LOCK_KEY})
            conn.execute(text("TRUNCATE " + ", ".join(reversed(LOAD_ORDER))))
            for t in LOAD_ORDER:
                _copy(conn, t, tables[t])

            loaded = {t: conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar() for t in LOAD_ORDER}
            if loaded != expected:  # conciliacion dentro de la transaccion: si no cuadra, ROLLBACK
                raise DeterministicError(f"Conciliacion fallida: esperado={expected} cargado={loaded}")

            for t, n in loaded.items():
                conn.execute(text("INSERT INTO ops.load_audit (batch_id, run_id, table_name, rows_loaded, strategy) "
                                  "VALUES (:b,:r,:t,:n,'atomic_truncate_and_load')"), {"b": batch_id, "r": run_id, "t": t, "n": int(n)})
            for metric, value in manifest["counts"].items():
                conn.execute(text("INSERT INTO ops.batch_reconciliation (batch_id, run_id, metric, value) VALUES (:b,:r,:m,:v)"),
                             {"b": batch_id, "r": run_id, "m": metric, "v": int(value)})
    except (OperationalError, psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
        # conexion caida, timeout, servidor reiniciando -> reintentable (el rollback ya se aplico)
        raise TransientError(f"Fallo operacional de BD durante la carga (rollback aplicado): {exc}") from exc
    except (DBAPIError, psycopg2.Error) as exc:
        # COPY usa el cursor crudo de psycopg2 (no pasa por SQLAlchemy): FK, tipos, constraints -> deterministico
        raise DeterministicError(f"Violacion de contrato en la carga (rollback aplicado): {exc}") from exc

    log_event(settings, batch_id=batch_id, task_id="load_dimensional_dw", run_id=run_id, source="dw",
              event="dw_loaded", rows_out=sum(loaded.values()), detail={"loaded": {k: int(v) for k, v in loaded.items()}})
    return {"batch_id": batch_id, "loaded": {k: int(v) for k, v in loaded.items()}}
