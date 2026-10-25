"""Gates 1 y 2: validacion de las capas raw (primaria y API) con GX + politica de severidad."""
from __future__ import annotations

import pandas as pd

from .batch import read_json
from .config import Settings
from .validation.gx_runner import run_gate
from .validation.policy import apply_policy

GATE_FOR_SOURCE = {"primary": "primary_raw", "api": "api_raw"}


def validate_raw(settings: Settings, source: str, batch_id: str, run_id: str | None = None, task_id: str = "") -> dict:
    gate = GATE_FOR_SOURCE[source]
    manifest = read_json(settings.raw_dir(source, batch_id) / "manifest.json")
    df = pd.read_parquet(manifest["raw_path"])
    rows = run_gate(settings, gate, {gate: df}, batch_id)
    summary = apply_policy(settings, gate, batch_id, rows, run_id=run_id, task_id=task_id or f"validate_{gate}")
    summary["row_count"] = int(len(df))
    summary["extracted_at"] = manifest["extracted_at"]
    return summary
