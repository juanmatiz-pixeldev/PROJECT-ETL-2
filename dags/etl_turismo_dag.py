"""DAG del pipeline ETL - Segunda Entrega (Apache Airflow 3.1.8, TaskFlow API).

Estructura (Seccion 16.1):

    ensure_dw_schema -> init_batch -+-> extract_primary -> validate_primary_raw -+
                                    |                                           +-> transform_and_clean_and_integrate
                                    +-> extract_api     -> validate_api_raw     -+        -> validate_prepared -> load_dimensional_dw

Airflow solo ORQUESTA: la logica vive en src/etl. Entre tareas viajan metadatos compactos por XCom
(batch_id, rutas, conteos, decision de validacion), nunca datasets.
Politica de fallas: TransientError -> reintento acotado; DeterministicError/CriticalValidationError ->
AirflowFailException (sin reintento, evidencia preservada, las tareas dependientes quedan en upstream_failed).
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from etl.batch import VALID_FAULTS, resolve_batch_id
from etl.config import get_settings
from etl.errors import DeterministicError
from etl.observability import log_event

log = logging.getLogger("airflow.task")
DAG_ID = "etl_turismo_second_delivery"


@contextmanager
def classify_failures():
    """Deterministico -> AirflowFailException (sin retry). Transitorio -> se propaga (Airflow reintenta)."""
    try:
        yield
    except DeterministicError as exc:
        log.error("FALLA DETERMINISTICA (sin reintento): %s", exc)
        raise AirflowFailException(str(exc)) from exc


def _on_failure(context):
    """Registra en ops.pipeline_run_log que tarea fallo y por que (Seccion 19: 'donde y por que fallo')."""
    try:
        ti = context["ti"]
        batch = (context.get("params") or {}).get("batch_id") or "unknown"
        log_event(
            get_settings(), batch_id=str(batch), task_id=ti.task_id, run_id=context.get("run_id"), dag_id=DAG_ID,
            event="task_failed", status="error",
            detail={"try_number": getattr(ti, "try_number", None), "error": str(context.get("exception") or context.get("reason") or "ver log de la tarea")[:1000]},
        )
    except Exception:  # noqa: BLE001  - el callback nunca debe romper el flujo
        log.exception("on_failure_callback fallo")


# --- Politicas de reintento por tarea (justificacion completa en docs/orchestration.md, Seccion 18) ---
API_RETRY = dict(retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
                 max_retry_delay=timedelta(minutes=5))
DB_RETRY = dict(retries=2, retry_delay=timedelta(seconds=60))
NO_RETRY = dict(retries=0)


@dag(
    dag_id=DAG_ID,
    description="ETL RNT + API (ODS 8) -> DW dimensional PostgreSQL, con 3 gates GX",
    schedule=None,                 # corrida manual: las fuentes son snapshots, no streams
    catchup=False,
    max_active_runs=1,             # nunca dos cargas concurrentes sobre el mismo DW
    dagrun_timeout=timedelta(hours=2),
    default_args={"on_failure_callback": _on_failure, "owner": "data-eng"},
    params={
        "batch_id": Param("", type="string", description="Vacio = se deriva del run_id. Repetirlo = re-ejecutar el MISMO lote (Test C)."),
        "fault_injection": Param("none", type="string", enum=list(VALID_FAULTS),
                                 description="Falla controlada para el Test B (Critical)."),
    },
    tags=["etl", "ods8", "segunda-entrega"],
    doc_md=__doc__,
)
def etl_turismo_second_delivery():
    @task(**DB_RETRY, execution_timeout=timedelta(minutes=5))
    def ensure_dw_schema() -> str:
        from etl.load import ensure_schema

        with classify_failures():
            ensure_schema(get_settings())
        return "ok"

    @task(**NO_RETRY)
    def init_batch() -> dict:
        ctx = get_current_context()
        p = ctx["params"]
        batch_id = resolve_batch_id(p.get("batch_id"), ctx["run_id"])
        fault = p.get("fault_injection", "none")
        if fault not in VALID_FAULTS:
            raise AirflowFailException(f"fault_injection invalido: {fault}")
        s = get_settings()
        log_event(s, batch_id=batch_id, task_id="init_batch", run_id=ctx["run_id"], dag_id=DAG_ID,
                  event="batch_started", detail={"fault_injection": fault, "logical_date": str(ctx.get("logical_date"))})
        return {"batch_id": batch_id, "fault_injection": fault}

    @task(**NO_RETRY, execution_timeout=timedelta(minutes=30))
    def extract_primary(batch: dict) -> dict:
        from etl.extract_primary import extract_primary as run

        s, ctx = get_settings(), get_current_context()
        with classify_failures():
            meta = run(s, batch["batch_id"], batch["fault_injection"])
        log_event(s, batch_id=meta["batch_id"], task_id="extract_primary", run_id=ctx["run_id"], dag_id=DAG_ID,
                  source="primary", event="extracted", rows_out=meta["row_count"], detail={"raw_path": meta["raw_path"]})
        return meta

    @task(**NO_RETRY, execution_timeout=timedelta(minutes=15))
    def validate_primary_raw(meta: dict) -> dict:
        from etl.raw_validate import validate_raw

        with classify_failures():
            return validate_raw(get_settings(), "primary", meta["batch_id"], get_current_context()["run_id"], "validate_primary_raw")

    @task(**API_RETRY, execution_timeout=timedelta(minutes=20))
    def extract_api(batch: dict) -> dict:
        from etl.extract_api import extract_api as run

        s, ctx = get_settings(), get_current_context()
        with classify_failures():          # TransientError escapa -> retry con backoff
            meta = run(s, batch["batch_id"], batch["fault_injection"])
        log_event(s, batch_id=meta["batch_id"], task_id="extract_api", run_id=ctx["run_id"], dag_id=DAG_ID,
                  source="api", event="extracted", rows_out=meta["row_count"],
                  detail={"raw_path": meta["raw_path"], "extracted_at": meta["extracted_at"]})
        return meta

    @task(**NO_RETRY, execution_timeout=timedelta(minutes=15))
    def validate_api_raw(meta: dict) -> dict:
        from etl.raw_validate import validate_raw

        with classify_failures():
            return validate_raw(get_settings(), "api", meta["batch_id"], get_current_context()["run_id"], "validate_api_raw")

    # Solo se ejecuta si AMBAS validaciones raw terminaron en success (trigger_rule por defecto: all_success)
    @task(**NO_RETRY, execution_timeout=timedelta(minutes=45))
    def transform_and_clean_and_integrate(primary_ok: dict, api_ok: dict) -> dict:
        from etl.transform import transform_and_integrate

        s, ctx = get_settings(), get_current_context()
        with classify_failures():
            out = transform_and_integrate(s, primary_ok["batch_id"])
        c = out["counts"]
        log_event(s, batch_id=out["batch_id"], task_id="transform_and_clean_and_integrate", run_id=ctx["run_id"], dag_id=DAG_ID,
                  event="transformed", rows_in=c["primary_raw_rows"], rows_out=c["prepared_fact_turismo_anual"], detail=c)
        return {"batch_id": out["batch_id"], "prepared_dir": out["prepared_dir"]}

    @task(**NO_RETRY, execution_timeout=timedelta(minutes=15))
    def validate_prepared(prep: dict) -> dict:
        from etl.prepare_validate import validate_prepared as run

        with classify_failures():
            return run(get_settings(), prep["batch_id"], get_current_context()["run_id"], "validate_prepared")

    @task(**DB_RETRY, execution_timeout=timedelta(minutes=30))
    def load_dimensional_dw(prepared_ok: dict) -> dict:
        from etl.load import load_dimensional

        with classify_failures():          # TransientError (BD caida) -> retry; el rollback deja el DW intacto
            return load_dimensional(get_settings(), prepared_ok["batch_id"], get_current_context()["run_id"])

    schema = ensure_dw_schema()
    batch = init_batch()
    schema >> batch

    p_raw = extract_primary(batch)
    p_ok = validate_primary_raw(p_raw)
    a_raw = extract_api(batch)
    a_ok = validate_api_raw(a_raw)

    prepared = transform_and_clean_and_integrate(p_ok, a_ok)
    prepared_ok = validate_prepared(prepared)
    load_dimensional_dw(prepared_ok)


etl_turismo_second_delivery()
