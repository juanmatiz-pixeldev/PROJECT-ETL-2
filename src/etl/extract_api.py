"""Extraccion programatica desde la API (Socrata SODA 2.x en datos.gov.co).

Comportamiento operacional (Seccion 4.1):
  * Paginacion: $limit/$offset con $order=:id (orden estable) hasta recibir una pagina corta.
  * Timeouts: (connect, read) configurables; sin reintentos en el codigo -> el reintento lo hace Airflow
    (asi queda visible en la UI y se aplica la politica por tipo de falla).
  * Clasificacion: timeout / conexion / 5xx / 429 / 408  -> TransientError (reintentable)
                   4xx restante, JSON invalido, respuesta vacia, campos clave ausentes -> DeterministicError
  * Autenticacion: opcional, header X-App-Token tomado de API_APP_TOKEN (nunca se persiste en manifiesto).
  * Evidencia: cada pagina queda registrada (status, filas, ms) y se guarda timestamp de extraccion.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd
import requests

from .batch import utc_now_iso, write_json
from .config import Settings
from .errors import DeterministicError, TransientError

log = logging.getLogger("etl.extract_api")

RAW_FILE = "api_raw.parquet"
TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504}


def _fetch_page(session: requests.Session, url: str, params: dict, timeout: tuple) -> tuple[list[dict], dict]:
    t0 = time.time()
    try:
        resp = session.get(url, params=params, timeout=timeout)
    except (requests.Timeout, requests.ConnectionError) as exc:
        raise TransientError(f"API no disponible ({type(exc).__name__}): {exc}") from exc

    meta = {"status": resp.status_code, "ms": int((time.time() - t0) * 1000), "offset": params.get("$offset")}
    if resp.status_code in TRANSIENT_STATUS:
        raise TransientError(f"API respondio HTTP {resp.status_code} (transitorio) offset={params.get('$offset')}")
    if resp.status_code >= 400:
        raise DeterministicError(f"API respondio HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        payload = resp.json()
    except ValueError as exc:
        raise DeterministicError(f"Respuesta de la API no es JSON valido: {exc}") from exc
    if not isinstance(payload, list):
        raise DeterministicError(f"Se esperaba una lista de registros y llego {type(payload).__name__}: {str(payload)[:200]}")
    meta["rows"] = len(payload)
    return payload, meta


def extract_api(settings: Settings, batch_id: str, fault_injection: str = "none") -> dict:
    if not settings.api_dataset_id or settings.api_dataset_id == "xxxx-xxxx":
        raise DeterministicError("API_DATASET_ID no configurado (ver .env.example y docs/api_source.md).")

    url = f"{settings.api_base_url.rstrip('/')}/resource/{settings.api_dataset_id}.json"
    headers = {"Accept": "application/json"}
    if settings.api_app_token:
        headers["X-App-Token"] = settings.api_app_token

    base_params: dict[str, Any] = {"$limit": settings.api_page_size, "$order": ":id"}
    if settings.api_where:
        base_params["$where"] = settings.api_where
    timeout = (settings.api_connect_timeout_s, settings.api_read_timeout_s)

    extracted_at = utc_now_iso()
    t0 = time.time()
    records: list[dict] = []
    pages: list[dict] = []
    session = requests.Session()
    session.headers.update(headers)

    for page_no in range(settings.api_max_pages):
        params = {**base_params, "$offset": page_no * settings.api_page_size}
        rows, meta = _fetch_page(session, url, params, timeout)
        pages.append(meta)
        log.info("API pagina %d: HTTP %s, %d filas, %d ms", page_no, meta["status"], meta["rows"], meta["ms"])
        records.extend(rows)
        if len(rows) < settings.api_page_size:
            break
    else:
        raise DeterministicError(f"Se alcanzo API_MAX_PAGES={settings.api_max_pages} sin agotar el dataset")

    if not records:
        raise DeterministicError("La API devolvio 0 registros para los parametros dados (extraccion vacia)")

    # Representacion cruda normalizada: todo texto, nombres canonicos segun API_FIELD_MAP.
    df = pd.DataFrame.from_records(records).astype("string")
    df = df.rename(columns=settings.api_field_map)
    keep = [c for c in ("codigo_municipio", "anio") if c in df.columns] + [
        c for c in settings.api_value_fields if c in df.columns
    ]
    df = df[keep] if keep else df
    df["_extracted_at"] = extracted_at
    df["_batch_id"] = batch_id

    fault_applied = None
    if fault_injection == "api_null_keys" and "codigo_municipio" in df.columns:
        # FALLA CONTROLADA (Test B): ~30% de llaves de integracion nulas. Declarada en el manifiesto.
        n = int(len(df) * 0.30)
        df.loc[df.index[:n], "codigo_municipio"] = pd.NA
        fault_applied = f"api_null_keys:{n}_rows"
        log.warning("FAULT INJECTION activa: %s", fault_applied)

    out_dir = settings.raw_dir("api", batch_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / RAW_FILE
    tmp = out_path.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(out_path)

    manifest = {
        "batch_id": batch_id,
        "source": "api",
        "endpoint": url,  # sin token
        "params": {k: v for k, v in base_params.items()},
        "authenticated": bool(settings.api_app_token),
        "extracted_at": extracted_at,
        "pages": pages,
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "raw_path": str(out_path),
        "fault_injected": fault_applied,
        "elapsed_s": round(time.time() - t0, 2),
    }
    write_json(out_dir / "manifest.json", manifest)
    log.info("API extraida: %d filas en %d paginas -> %s", manifest["row_count"], len(pages), out_path)
    return {k: manifest[k] for k in ("batch_id", "source", "raw_path", "row_count", "extracted_at", "fault_injected")}
