"""Extraccion de la fuente primaria (RNT, CSV de datos.gov.co).

Contrato (Seccion 17):
  entrada : PRIMARY_SOURCE_PATH + batch_id
  salida  : data/raw/primary/<batch_id>/rnt_raw.parquet + manifest.json  (se devuelve solo metadata compacta)
  rerun   : re-extraer el mismo batch_id sobrescribe de forma atomica los mismos archivos (idempotente)

Se lee TODO como texto (dtype=str): la capa raw preserva el dato tal cual llega; el parseo de tipos
es una transformacion posterior y la validacion raw puede detectar valores no numericos.
"""
from __future__ import annotations

import logging
import sqlite3
import time

import pandas as pd

from .batch import sha256_file, utc_now_iso, write_json
from .config import Settings
from .errors import DeterministicError

log = logging.getLogger("etl.extract_primary")

RAW_FILE = "rnt_raw.parquet"

# Reconstruye la vista desnormalizada de 14 columnas del RNT desde el modelo estrella de la Primera Entrega
# (turismo_dw.db, SQLite). dim_ubicacion trae 50 codigos de municipio repetidos con departamentos distintos; se conserva
# el primero por codigo (misma regla de la Primera Entrega) para no multiplicar filas del hecho. LIMITACION documentada en el README.
# (turismo_dw.db, SQLite). Se abre en SOLO LECTURA: el archivo original nunca se modifica.
SQLITE_QUERY = """
SELECT p.codigo_rnt AS CODIGO_RNT, p.razon_social_establecimiento AS RAZON_SOCIAL_ESTABLECIMIENTO, p.nit AS NIT,
       u.codigo_departamento AS CODIGO_DEPARTAMENTO, u.departamento AS DEPARTAMENTO,
       u.codigo_municipio AS CODIGO_MUNICIPIO, u.municipio AS MUNICIPIO,
       c.categoria AS CATEGORIA, c.sub_categoria AS SUB_CATEGORIA, p.estado_rnt AS ESTADO_RNT,
       f.numero_de_habitaciones AS NUMERO_DE_HABITACIONES, f.numero_de_camas AS NUMERO_DE_CAMAS,
       f.numero_de_empleados AS NUMERO_DE_EMPLEADOS, f.anio AS "AÑO"
FROM fact_turismo_anual f
JOIN dim_prestador p ON p.codigo_rnt = f.codigo_rnt
JOIN (SELECT * FROM dim_ubicacion WHERE rowid IN (SELECT MIN(rowid) FROM dim_ubicacion GROUP BY codigo_municipio)) u
     ON u.codigo_municipio = f.codigo_municipio
JOIN dim_categoria c ON c.id_categoria = f.id_categoria
ORDER BY f.id_fact
"""


def _read_source(src) -> tuple[pd.DataFrame, str]:
    if src.suffix.lower() in (".db", ".sqlite", ".sqlite3"):
        con = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
        try:
            df = pd.read_sql_query(SQLITE_QUERY, con).astype("string")
        finally:
            con.close()
        return df, "sqlite_star_schema"
    return pd.read_csv(src, dtype=str, low_memory=False), "csv"


def extract_primary(settings: Settings, batch_id: str, fault_injection: str = "none") -> dict:
    src = settings.primary_source_path
    if not src.exists():
        # Archivo faltante no se arregla reintentando -> deterministico
        raise DeterministicError(f"Fuente primaria no encontrada: {src}. Ver README (data/source).")

    t0 = time.time()
    df, source_format = _read_source(src)
    fault_applied = None
    if fault_injection == "primary_drop_column":
        # FALLA CONTROLADA (Test B): se elimina un atributo obligatorio. Queda declarada en el manifiesto.
        df = df.drop(columns=["CODIGO_MUNICIPIO"], errors="ignore")
        fault_applied = "primary_drop_column:CODIGO_MUNICIPIO"
        log.warning("FAULT INJECTION activa: %s", fault_applied)

    out_dir = settings.raw_dir("primary", batch_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / RAW_FILE
    tmp = out_path.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(out_path)

    manifest = {
        "batch_id": batch_id,
        "source": "primary",
        "source_ref": str(src),
        "source_format": source_format,
        "source_sha256": sha256_file(src),
        "extracted_at": utc_now_iso(),
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "raw_path": str(out_path),
        "fault_injected": fault_applied,
        "elapsed_s": round(time.time() - t0, 2),
    }
    write_json(out_dir / "manifest.json", manifest)
    log.info("Primary extraido: %s filas -> %s", manifest["row_count"], out_path)
    return {k: manifest[k] for k in ("batch_id", "source", "raw_path", "row_count", "extracted_at", "fault_injected")}
