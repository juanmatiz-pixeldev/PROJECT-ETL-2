"""Transformacion e integracion (BASE funcional; el equipo de transformacion/modelado la refina, Secciones 12-13).

Regla de oro (Seccion 13): la validacion DETECTA, la transformacion aplica reglas de ingenieria conocidas.
Nada se modifica solo para que una validacion pase. Los registros no transformables se RECHAZAN a
cuarentena (data/prepared/<batch>/rejected_*.parquet) y se cuentan; no se convierten silenciosamente a 0.

IDs de transformacion (para la tabla de la Seccion 13):
  T01 primary: normalizacion de texto (strip/upper) e imputacion explicita 'NO REGISTRA' en razon social
  T02 primary: conversion de tipos numericos; filas con llaves/medidas no parseables o con codigo de municipio fuera
              del rango DIVIPOLA (1000-99999) -> cuarentena (en los datos reales: 22 filas con codigo 1)
  T03 api    : llaves canonicas (codigo_municipio INT, anio INT) y valores numericos; no parseables -> cuarentena
  T04 api    : desduplicacion determinista por (municipio, anio, indicador); se cuenta lo descartado
  T05 model  : dimensiones y hecho del RNT (linea base Primera Entrega)
  T06 integ. : enriquecimiento por llave (codigo_municipio, anio): fact_api_indicador + integration_match
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .batch import read_json, utc_now_iso, write_json
from .config import Settings
from .errors import DeterministicError

log = logging.getLogger("etl.transform")

PRIMARY_INT = ["CODIGO_RNT", "CODIGO_DEPARTAMENTO", "CODIGO_MUNICIPIO", "AÑO",
               "NUMERO_DE_HABITACIONES", "NUMERO_DE_CAMAS", "NUMERO_DE_EMPLEADOS"]
PRIMARY_TEXT = ["RAZON_SOCIAL_ESTABLECIMIENTO", "NIT", "DEPARTAMENTO", "MUNICIPIO", "CATEGORIA",
                "SUB_CATEGORIA", "ESTADO_RNT"]


def _t_primary(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    for c in PRIMARY_TEXT:                                               # T01
        df[c] = df[c].astype("string").str.strip().str.upper()
    df["RAZON_SOCIAL_ESTABLECIMIENTO"] = df["RAZON_SOCIAL_ESTABLECIMIENTO"].fillna("NO REGISTRA")
    for c in PRIMARY_INT:                                                # T02
        df[c] = pd.to_numeric(df[c], errors="coerce")
    bad = df[PRIMARY_INT].isna().any(axis=1) | (df[["NUMERO_DE_HABITACIONES", "NUMERO_DE_CAMAS", "NUMERO_DE_EMPLEADOS"]] < 0).any(axis=1)
    bad = bad | ~df["CODIGO_MUNICIPIO"].between(1000, 99999)
    rejected, ok = df[bad].copy(), df[~bad].copy()
    for c in PRIMARY_INT:
        ok[c] = ok[c].astype("int64")
    return ok, rejected


def _t_api(df: pd.DataFrame, value_fields: tuple) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    df = df.copy()
    df["codigo_municipio"] = pd.to_numeric(df["codigo_municipio"], errors="coerce")     # '05001' -> 5001 (T03)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
    long = df.melt(id_vars=["codigo_municipio", "anio"], value_vars=list(value_fields),
                   var_name="indicador", value_name="valor")
    long["valor"] = pd.to_numeric(long["valor"], errors="coerce")
    bad = long[["codigo_municipio", "anio", "valor"]].isna().any(axis=1)
    rejected, ok = long[bad].copy(), long[~bad].copy()
    ok["codigo_municipio"] = ok["codigo_municipio"].astype("int64")
    ok["anio"] = ok["anio"].astype("int64")
    before = len(ok)                                                                     # T04
    ok = ok.sort_values(["codigo_municipio", "anio", "indicador", "valor"]).drop_duplicates(
        subset=["codigo_municipio", "anio", "indicador"], keep="first")
    return ok, rejected, before - len(ok)


def transform_and_integrate(settings: Settings, batch_id: str) -> dict:
    p_manifest = read_json(settings.raw_dir("primary", batch_id) / "manifest.json")
    a_manifest = read_json(settings.raw_dir("api", batch_id) / "manifest.json")
    # Defensa en profundidad: ambas ramas deben haber pasado su gate (la dependencia de Airflow ya lo impone)
    for gate in ("primary_raw", "api_raw"):
        vp = settings.validation_dir(batch_id) / f"{gate}.json"
        if not vp.exists() or read_json(vp)["decision"] == "stop":
            raise DeterministicError(f"Gate {gate} no aceptado para el lote {batch_id}; no se transforma.")

    primary = pd.read_parquet(p_manifest["raw_path"])
    api = pd.read_parquet(a_manifest["raw_path"])
    if not settings.api_value_fields:
        raise DeterministicError("API_VALUE_FIELDS vacio: no hay indicadores de la API que integrar")

    ok, rej_p = _t_primary(primary)
    api_ok, rej_a, api_dups = _t_api(api, settings.api_value_fields)

    # ---- T05: dimensiones y hecho (linea base Primera Entrega) ----
    dim_ubicacion = (ok[["CODIGO_MUNICIPIO", "MUNICIPIO", "CODIGO_DEPARTAMENTO", "DEPARTAMENTO"]]
                     .drop_duplicates(subset=["CODIGO_MUNICIPIO"]).rename(columns=str.lower)
                     .rename(columns={"codigo_municipio": "codigo_municipio"}))
    dim_categoria = ok[["CATEGORIA", "SUB_CATEGORIA"]].drop_duplicates().sort_values(["CATEGORIA", "SUB_CATEGORIA"]).reset_index(drop=True)
    dim_categoria.insert(0, "id_categoria", np.arange(1, len(dim_categoria) + 1))
    dim_categoria.columns = ["id_categoria", "categoria", "sub_categoria"]
    dim_prestador = (ok[["CODIGO_RNT", "RAZON_SOCIAL_ESTABLECIMIENTO", "NIT", "ESTADO_RNT"]]
                     .drop_duplicates(subset=["CODIGO_RNT"]))
    dim_prestador.columns = ["codigo_rnt", "razon_social", "nit", "estado_rnt"]
    dim_tiempo = pd.DataFrame({"anio": sorted(ok["AÑO"].unique())})

    f = ok.merge(dim_categoria, left_on=["CATEGORIA", "SUB_CATEGORIA"], right_on=["categoria", "sub_categoria"], how="left")
    f = f.sort_values(["CODIGO_RNT", "CODIGO_MUNICIPIO", "AÑO", "id_categoria"]).reset_index(drop=True)
    fact = pd.DataFrame({
        "id_fact": np.arange(1, len(f) + 1, dtype="int64"),
        "codigo_rnt": f["CODIGO_RNT"], "codigo_municipio": f["CODIGO_MUNICIPIO"],
        "id_categoria": f["id_categoria"].astype("int64"), "anio": f["AÑO"],
        "numero_de_habitaciones": f["NUMERO_DE_HABITACIONES"], "numero_de_camas": f["NUMERO_DE_CAMAS"],
        "numero_de_empleados": f["NUMERO_DE_EMPLEADOS"],
    })

    # ---- T06: integracion por (codigo_municipio, anio) ----
    pairs = fact[["codigo_municipio", "anio"]].drop_duplicates()
    api_keys = api_ok[["codigo_municipio", "anio"]].drop_duplicates().assign(_hit=1)
    match = pairs.merge(api_keys, on=["codigo_municipio", "anio"], how="left")
    match["matched"] = match["_hit"].fillna(0).astype("int64")
    integration_match = match.drop(columns="_hit")

    in_dims = api_ok["codigo_municipio"].isin(dim_ubicacion["codigo_municipio"]) & api_ok["anio"].isin(dim_tiempo["anio"])
    fact_api = api_ok[in_dims][["codigo_municipio", "anio", "indicador", "valor"]].reset_index(drop=True)
    api_unmatched = int((~in_dims).sum())

    out = settings.prepared_dir(batch_id)
    out.mkdir(parents=True, exist_ok=True)
    tables = {"dim_ubicacion": dim_ubicacion, "dim_categoria": dim_categoria, "dim_prestador": dim_prestador,
              "dim_tiempo": dim_tiempo, "fact_turismo_anual": fact, "fact_api_indicador": fact_api,
              "integration_match": integration_match}
    for name, t in tables.items():
        t.to_parquet(out / f"{name}.parquet", index=False)
    rej_p.to_parquet(out / "rejected_primary.parquet", index=False)
    rej_a.to_parquet(out / "rejected_api.parquet", index=False)

    counts = {
        "primary_raw_rows": len(primary), "primary_accepted_rows": len(ok), "primary_rejected_rows": len(rej_p),
        "api_raw_rows": len(api), "api_indicator_rows_accepted": len(api_ok), "api_rejected_rows": len(rej_a),
        "api_duplicates_dropped": api_dups, "api_rows_without_dimension_match": api_unmatched,
        "municipio_anio_pairs_rnt": len(integration_match), "municipio_anio_pairs_matched": int(integration_match["matched"].sum()),
        **{f"prepared_{k}": len(v) for k, v in tables.items()},
    }
    counts = {k: int(v) for k, v in counts.items()}
    write_json(out / "manifest.json", {"batch_id": batch_id, "prepared_at": utc_now_iso(), "counts": counts})
    log.info("Transformacion/integracion OK: %s", counts)
    return {"batch_id": batch_id, "prepared_dir": str(out), "counts": counts}
