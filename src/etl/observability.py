"""Observabilidad (Seccion 19). Cada evento relevante se escribe en:
  1) el log estandar (visible en el log de la tarea en Airflow),
  2) un JSONL por lote (data/logs/<batch>/events.jsonl) como evidencia local,
  3) ops.pipeline_run_log en el DW (consultable por SQL / dashboard de monitoreo).
La escritura a BD es best-effort: un fallo de observabilidad no debe ocultar ni causar el fallo del dato.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .batch import utc_now_iso
from .config import Settings

log = logging.getLogger("etl")

_ENGINES: dict[str, Engine] = {}


def get_engine(settings: Settings) -> Engine:
    url = settings.dw_url
    if url not in _ENGINES:
        _ENGINES[url] = create_engine(url, pool_pre_ping=True)
    return _ENGINES[url]


def log_event(
    settings: Settings,
    *,
    batch_id: str,
    task_id: str,
    event: str,
    status: str = "ok",
    source: str | None = None,
    rows_in: int | None = None,
    rows_out: int | None = None,
    run_id: str | None = None,
    dag_id: str = "etl_turismo_second_delivery",
    detail: dict[str, Any] | None = None,
) -> None:
    record = {
        "ts": utc_now_iso(), "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
        "batch_id": batch_id, "source": source, "event": event, "status": status,
        "rows_in": rows_in, "rows_out": rows_out, "detail": detail or {},
    }
    level = logging.ERROR if status == "error" else logging.WARNING if status == "warning" else logging.INFO
    log.log(level, "EVENT %s", json.dumps(record, ensure_ascii=False, default=str))

    try:
        d = settings.log_dir(batch_id)
        d.mkdir(parents=True, exist_ok=True)
        with open(d / "events.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except OSError as exc:  # pragma: no cover
        log.warning("No se pudo escribir events.jsonl: %s", exc)

    try:
        with get_engine(settings).begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO ops.pipeline_run_log "
                    "(dag_id, run_id, task_id, batch_id, source, event, status, rows_in, rows_out, detail) "
                    "VALUES (:dag_id,:run_id,:task_id,:batch_id,:source,:event,:status,:rows_in,:rows_out,"
                    "CAST(:detail AS JSONB))"
                ),
                {**record, "detail": json.dumps(record["detail"], default=str)},
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("ops.pipeline_run_log no disponible (best-effort): %s", exc)
