# Orquestación, interfaces, fallos y observabilidad (Secciones 11, 16, 17, 18, 19)

## 11 / 16. Estructura del DAG (`dags/etl_turismo_dag.py`, Airflow 3.1.8, TaskFlow, `from airflow.sdk import dag, task`)

```
ensure_dw_schema → init_batch ─┬→ extract_primary → validate_primary_raw ─┐
                               └→ extract_api     → validate_api_raw     ─┴→ transform_and_clean_and_integrate
                                                                            → validate_prepared → load_dimensional_dw
```
Tareas extra justificadas: `ensure_dw_schema` (DDL idempotente: el DW existe antes de registrar observabilidad) e `init_batch` (resuelve identidad de lote y falla controlada). `max_active_runs=1` impide cargas concurrentes. Dependencia por defecto `all_success`: si un gate falla, lo dependiente queda `upstream_failed`.

## 17. Interfaces de tareas (XCom solo con metadatos compactos)

| Tarea | Entrada | Salida (persistida / compacta) | Señal de falla | Rerun |
|---|---|---|---|---|
| extract_primary | `batch_id`, `PRIMARY_SOURCE_PATH` | `raw/primary/<b>/rnt_raw.parquet` + `manifest.json` (sha256, filas, timestamp) / `{batch_id, raw_path, row_count}` | `AirflowFailException` si falta el archivo | Sobrescribe de forma atómica el mismo lote |
| extract_api | `batch_id`, config API | `raw/api/<b>/api_raw.parquet` + manifiesto (endpoint, páginas, status, timestamp) | Transitorio → excepción (retry); determinístico → `AirflowFailException` | Re-extrae y sobrescribe el lote |
| validate_*_raw | metadatos de extracción | `validation/<b>/<gate>.json` + filas en `ops.validation_result` | `CriticalValidationError` → `AirflowFailException` | Nueva evidencia (historial por `run_id`) |
| transform_and_clean_and_integrate | `batch_id` (+ gates raw aceptados) | `prepared/<b>/*.parquet` + manifiesto con conteos | determinístico → fail | Regenera los mismos archivos |
| validate_prepared | `batch_id` | `validation/<b>/prepared.json` | Critical → fail | igual |
| load_dimensional_dw | `batch_id` (+ `prepared.json` aceptado) | filas en DW, `ops.load_audit`, `ops.batch_reconciliation` | transitorio → retry; contrato → fail | **Reemplazo atómico** (ver 20.3) |

**Identidad de lote:** parámetro `batch_id` del DAG; si va vacío se deriva del `run_id`. El mismo `batch_id` identifica rutas (`raw|prepared|validation/<batch>`), filas de `ops.*` y la carga. Repetirlo en otro DAG run = re-ejecutar el mismo lote.

**Defensa en profundidad:** además de las dependencias de Airflow, `transform` exige gates raw ≠ `stop` y `load` exige `prepared.json` ≠ `stop`.

## 18. Política de fallos y reintentos

| Condición | Tipo / severidad | Respuesta | Retry | Justificación |
|---|---|---|---|---|
| API timeout, conexión rechazada, HTTP 408/425/429/5xx | Transitoria | Excepción → `up_for_retry` | **3**, 30 s, backoff exponencial, máx 5 min | Mismo input/código puede funcionar luego; 3 intentos ≈ 30 s+60 s+120 s cubren cortes breves y límites de tasa sin retener el DAG más de ~4 min |
| API HTTP 4xx (≠408/425/429), JSON inválido, respuesta vacía, `API_DATASET_ID` ausente | Determinística | `AirflowFailException` | 0 | Repetir reproduce el error; requiere corrección de configuración |
| CSV primario inexistente | Determinística | fail | 0 | Un archivo ausente no aparece por reintentar |
| Regla DQ Critical incumplida (cualquier gate) | Critical | fail; rama y dependientes no avanzan | 0 | Es un hallazgo sobre los datos, no sobre la infraestructura |
| Regla DQ Warning | Warning | continúa; log WARNING + `ops.validation_result` (`continue_with_warning`) | n/a | No invalida el lote; evidencia preservada |
| Regla DQ Informational | Informational | continúa; solo observabilidad | n/a | Monitoreo de tendencia |
| BD no disponible (DDL o carga) | Transitoria | excepción → retry | **2**, 60 s | Reinicios breves de Postgres; seguro porque la carga es atómica (rollback) |
| FK / tipo / constraint / conciliación en la carga | Determinística | rollback + fail | 0 | Contrato violado: repetir no lo arregla |
| Transform / validaciones / init | Determinística | fail | 0 | Sin dependencias externas: no hay falla transitoria plausible |

No se usa un `retries=N` global: el único default es `retries=0`; cada tarea declara su política (constantes `API_RETRY`, `DB_RETRY`, `NO_RETRY`).

## 19. Logging y observabilidad

| Pregunta | Dónde se responde |
|---|---|
| ¿Qué corrió? | UI Airflow (DAG Run / Task Instance) y `ops.pipeline_run_log.task_id/event` |
| ¿Cuándo? | timestamps de la UI; `ops.pipeline_run_log.ts`; `manifest.extracted_at` |
| ¿Qué lote/fuente? | `batch_id`, `source`, `raw_path` en eventos y manifiestos |
| ¿Cuántos registros? | `rows_in/rows_out`, `ops.batch_reconciliation`, `ops.load_audit` |
| ¿Pasó la validación? | `ops.validation_result` (regla a regla) y `data/validation/<b>/<gate>.json` |
| ¿Dónde y por qué falló? | tarea `failed` + log de tarea + evento `task_failed` (callback) + regla `stop` |
| ¿Qué pasó después? | estado `upstream_failed` / `up_for_retry`, decisión `continue_with_warning` |

Consultas listas en `sql/03_reconciliation_queries.sql`. Evidencia por lote: `python scripts/export_evidence.py <batch_id>`.
