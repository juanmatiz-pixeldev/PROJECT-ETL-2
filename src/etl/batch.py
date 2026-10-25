"""Identidad de lote y manifiestos (Seccion 17: batch identity / output contract)."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

VALID_FAULTS = ("none", "api_null_keys", "primary_drop_column")


def resolve_batch_id(param_batch_id: str | None, run_id: str) -> str:
    """El lote se identifica por un id explicito (rerun del MISMO lote en otro DAG run)
    o, si no se da, se deriva del run_id (clear de tareas dentro del mismo DAG run)."""
    raw = (param_batch_id or "").strip() or run_id
    safe = re.sub(r"[^0-9A-Za-z]+", "", raw)
    if not safe:
        raise ValueError("batch_id vacio tras sanitizar")
    return safe[:40]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)  # escritura atomica: nunca queda un manifiesto a medias
    return path


def read_json(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))
