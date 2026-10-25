"""Ejecucion controlada de Great Expectations (GX 1.x) para un gate.

Mapeo de elementos GX (Seccion 10):
  Data object        -> DataFrame de pandas (raw primario, raw API o tabla preparada) via Data Source/Asset
  Expectation        -> una regla del catalogo (meta.rule_id)
  Expectation Suite  -> reglas de UN data_object de UN gate
  Validation Def.    -> asocia Batch Definition (DataFrame completo) + Suite, repetible
  Controlled exec.   -> run_gate(): ejecuta las Validation Definitions del gate y devuelve resultados planos
GX solo DETECTA. La decision continue/warn/stop la toma policy.apply_policy().
"""
from __future__ import annotations

import json
import logging

import great_expectations as gx
import pandas as pd

from ..config import Settings
from .suites import build_suite, rules_by_object

log = logging.getLogger("etl.gx")


def _py(v):
    """numpy/pandas escalares -> tipos nativos (psycopg2 no adapta numpy.int64)."""
    if v is None:
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:  # noqa: BLE001
            return str(v)
    if isinstance(v, (list, tuple)):
        return [_py(x) for x in v]
    return v


def _raised_exception(info) -> bool:
    """exception_info: {'raised_exception': bool, ...} (o un dict de ellos segun version)."""
    if not info:
        return False
    if "raised_exception" in info:
        return bool(info["raised_exception"])
    return any(isinstance(v, dict) and v.get("raised_exception") for v in info.values())


def _flatten(validation_result, rules_index: dict[str, dict], data_object: str) -> list[dict]:
    rows = []
    for er in validation_result.results:
        cfg = er.expectation_config
        meta = cfg.meta or {}
        rid = meta.get("rule_id", "UNTRACED")
        res = er.result or {}
        has_exc = _raised_exception(er.exception_info)
        rows.append(
            {
                "rule_id": rid,
                "data_object": data_object,
                "expectation_type": cfg.type,
                "severity": meta.get("severity", "critical"),  # sin trazabilidad -> se trata como critico
                "requirement": meta.get("requirement"),
                "dimension": meta.get("dimension"),
                "metric": meta.get("metric"),
                "success": bool(er.success) and not has_exc,
                "element_count": _py(res.get("element_count")),
                "unexpected_count": _py(res.get("unexpected_count")),
                "unexpected_percent": _py(res.get("unexpected_percent")),
                "observed_value": _py(res.get("observed_value")),
                "partial_unexpected_list": _py((res.get("partial_unexpected_list") or [])[:5]),
                "exception": has_exc,
            }
        )
    return rows


def run_gate(
    settings: Settings,
    gate: str,
    data_objects: dict[str, pd.DataFrame],
    batch_id: str,
    references: dict[str, list] | None = None,
) -> list[dict]:
    """Ejecuta todas las suites del gate. Devuelve una fila por Expectation evaluada."""
    references = references or {}
    by_obj = rules_by_object(settings, gate)
    if not by_obj:
        raise RuntimeError(f"El catalogo no define reglas para el gate '{gate}'")

    context = gx.get_context(mode="ephemeral")
    try:  # barras de progreso ensucian los logs de Airflow
        from great_expectations.data_context.types.base import ProgressBarsConfig

        context.variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:  # noqa: BLE001
        pass
    source = context.data_sources.add_pandas(name=f"pandas_{gate}")
    out_dir = settings.validation_dir(batch_id) / "suites"
    out_dir.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict] = []

    for data_object, rules in by_obj.items():
        if data_object not in data_objects:
            raise RuntimeError(f"Gate '{gate}': falta el data object '{data_object}'")
        df = data_objects[data_object]
        asset = source.add_dataframe_asset(name=data_object)
        batch_def = asset.add_batch_definition_whole_dataframe(f"{data_object}_whole")
        suite = context.suites.add(build_suite(settings, gate, data_object, rules, references))
        vdef = context.validation_definitions.add(
            gx.ValidationDefinition(name=f"vd_{gate}_{data_object}", data=batch_def, suite=suite)
        )
        # Evidencia: se guarda la suite exacta que se ejecuto
        (out_dir / f"{gate}.{data_object}.suite.json").write_text(
            json.dumps(suite.to_json_dict(), indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )
        result = vdef.run(
            batch_parameters={"dataframe": df},
            result_format={"result_format": "COMPLETE", "partial_unexpected_count": 5},
        )
        rows = _flatten(result, {r["rule_id"]: r for r in rules}, data_object)
        log.info("GX gate=%s object=%s success=%s (%d expectations)", gate, data_object, result.success, len(rows))
        all_rows.extend(rows)
    return all_rows
