"""Construye Expectation Suites de Great Expectations desde gx/rules_catalog.yaml.

Un Suite agrupa las reglas de UN data_object en UN gate (misma capa y mismo objeto de datos).
Cada Expectation lleva en `meta` el rule_id, la severidad y el requerimiento que protege.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

import great_expectations as gx
import yaml

from ..config import Settings


def load_catalog(settings: Settings) -> list[dict]:
    with open(settings.rules_catalog, encoding="utf-8") as f:
        return yaml.safe_load(f)["rules"]


def rules_by_object(settings: Settings, gate: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for r in load_catalog(settings):
        if r["gate"] == gate:
            out[r["data_object"]].append(r)
    return dict(out)


def _resolve(value: Any, settings: Settings, references: dict[str, list]) -> Any:
    if isinstance(value, str) and value.startswith("$API_VALUE_FIELD_"):
        idx = int(value.rsplit("_", 1)[1])
        if idx >= len(settings.api_value_fields):
            raise ValueError(f"{value} requiere API_VALUE_FIELDS con al menos {idx + 1} campo(s)")
        return settings.api_value_fields[idx]
    if isinstance(value, dict) and "$ref" in value:
        key = value["$ref"]
        if key not in references:
            raise KeyError(f"Referencia GX no disponible: {key}")
        return references[key]
    if isinstance(value, dict):
        return {k: _resolve(v, settings, references) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(v, settings, references) for v in value]
    return value


def build_suite(
    settings: Settings, gate: str, data_object: str, rules: list[dict], references: dict[str, list]
) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=f"{gate}.{data_object}")
    for r in rules:
        kwargs = _resolve(r.get("kwargs", {}), settings, references)
        exp_cls = getattr(gx.expectations, r["expectation"])
        suite.add_expectation(
            exp_cls(
                **kwargs,
                meta={
                    "rule_id": r["rule_id"],
                    "severity": r["severity"],
                    "requirement": r["requirement"],
                    "dimension": r["dimension"],
                    "metric": r["metric"],
                },
            )
        )
    return suite
