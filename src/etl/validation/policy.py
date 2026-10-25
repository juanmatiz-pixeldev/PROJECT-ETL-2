"""Politica de severidad del pipeline (Seccion 8.1). GX detecta; esta politica decide.

  critical      -> accion `stop`: se registra, se lanza CriticalValidationError y la rama NO continua.
  warning       -> accion `continue_with_warning`: se registra y se hace visible (log + ops); el pipeline sigue.
  informational -> accion `continue`: solo observabilidad.
Un resultado exitoso siempre es `pass`. Una Expectation sin meta de trazabilidad se trata como critical.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import text

from ..batch import utc_now_iso, write_json
from ..config import Settings
from ..errors import CriticalValidationError
from ..observability import get_engine, log_event

log = logging.getLogger("etl.policy")

ACTION = {"critical": "stop", "warning": "continue_with_warning", "informational": "continue"}


def apply_policy(
    settings: Settings, gate: str, batch_id: str, rows: list[dict], *, run_id: str | None = None, task_id: str = ""
) -> dict:
    for r in rows:
        r["action"] = "pass" if r["success"] else ACTION.get(r["severity"], "stop")

    failed = [r for r in rows if not r["success"]]
    critical = [r for r in failed if r["severity"] == "critical" or r["action"] == "stop"]
    warnings = [r for r in failed if r["severity"] == "warning"]
    info = [r for r in failed if r["severity"] == "informational"]
    decision = "stop" if critical else ("continue_with_warning" if warnings else "continue")

    summary = {
        "gate": gate,
        "batch_id": batch_id,
        "checked_at": utc_now_iso(),
        "decision": decision,
        "rules_evaluated": len(rows),
        "rules_failed": len(failed),
        "critical_failed": [r["rule_id"] for r in critical],
        "warnings": [r["rule_id"] for r in warnings],
        "informational": [r["rule_id"] for r in info],
        "results": rows,
    }

    # 1) Evidencia en archivo (se conserva incluso si el paso falla)
    write_json(settings.validation_dir(batch_id) / f"{gate}.json", summary)

    # 2) Logging legible en el log de la tarea
    for r in rows:
        lvl = logging.INFO if r["success"] else (logging.ERROR if r["action"] == "stop" else logging.WARNING)
        log.log(
            lvl,
            "GX %s %s [%s] success=%s unexpected=%s (%s%%) observed=%s -> %s",
            gate, r["rule_id"], r["severity"], r["success"], r["unexpected_count"],
            r["unexpected_percent"], r["observed_value"], r["action"],
        )

    # 3) Persistencia en ops.validation_result (best-effort)
    try:
        with get_engine(settings).begin() as conn:
            for r in rows:
                conn.execute(
                    text(
                        "INSERT INTO ops.validation_result (batch_id, run_id, gate, data_object, rule_id, "
                        "expectation_type, severity, requirement, success, element_count, unexpected_count, "
                        "unexpected_percent, observed_value, action) VALUES (:batch_id,:run_id,:gate,:data_object,"
                        ":rule_id,:expectation_type,:severity,:requirement,:success,:element_count,"
                        ":unexpected_count,:unexpected_percent,:observed_value,:action)"
                    ),
                    {
                        **{k: r.get(k) for k in (
                            "data_object", "rule_id", "expectation_type", "severity", "requirement", "success",
                            "element_count", "unexpected_count", "unexpected_percent", "action")},
                        "batch_id": batch_id, "run_id": run_id, "gate": gate,
                        "observed_value": json.dumps(r.get("observed_value"), default=str),
                    },
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("ops.validation_result no disponible (best-effort): %s", exc)

    log_event(
        settings, batch_id=batch_id, task_id=task_id or gate, run_id=run_id, source=gate,
        event="validation_gate", status={"stop": "error", "continue_with_warning": "warning"}.get(decision, "ok"),
        rows_in=None, rows_out=None,
        detail={k: summary[k] for k in ("decision", "rules_evaluated", "rules_failed", "critical_failed", "warnings")},
    )

    if critical:
        raise CriticalValidationError(gate, batch_id, critical)
    return {k: summary[k] for k in ("gate", "batch_id", "decision", "rules_evaluated", "rules_failed",
                                    "critical_failed", "warnings")}
