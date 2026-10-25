"""Gate 3: validacion de datos preparados (antes de la carga dimensional)."""
from __future__ import annotations

import pandas as pd

from .batch import read_json
from .config import Settings
from .validation.gx_runner import run_gate
from .validation.policy import apply_policy


def validate_prepared(settings: Settings, batch_id: str, run_id: str | None = None, task_id: str = "validate_prepared") -> dict:
    d = settings.prepared_dir(batch_id)
    objs = {n: pd.read_parquet(d / f"{n}.parquet") for n in ("fact_turismo_anual", "integration_match", "dim_ubicacion")}
    refs = {"dim_ubicacion.codigo_municipio": objs["dim_ubicacion"]["codigo_municipio"].astype(int).tolist()}
    rows = run_gate(settings, "prepared", objs, batch_id, references=refs)
    summary = apply_policy(settings, "prepared", batch_id, rows, run_id=run_id, task_id=task_id)
    summary["counts"] = read_json(d / "manifest.json")["counts"]
    return summary
