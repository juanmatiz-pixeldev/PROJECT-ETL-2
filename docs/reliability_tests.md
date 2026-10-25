# Pruebas de confiabilidad (Sección 20)

Ejecución: `scripts/run_reliability_tests.sh A|B|C|C_after` (Docker) o por UI → *Trigger DAG w/ config*.
Capturas obligatorias (legibles, recortadas, con interpretación) → `docs/evidence/`. Plantilla de interpretación: *Afirmación → Evidencia → Explicación*.

## Test A — corrida exitosa (`{"batch_id":"testA"}`)
| Etapa | Evidencia a capturar |
|---|---|
| Adquisición | Grid con `extract_primary` y `extract_api` en success; `manifest.json` de ambos (filas, timestamp, sha256/endpoint) |
| Validación raw | `data/validation/testA/primary_raw.json` y `api_raw.json`; filas en `ops.validation_result` |
| Transformación/integración | tarea success + `prepared/testA/manifest.json` (conteos, pares emparejados) |
| Preparada | `prepared.json` con `decision` ≠ stop |
| Carga y analítica | `ops.load_audit`, `ops.batch_reconciliation`, consultas/dashboard |

## Test B — falla Critical controlada (`{"batch_id":"testB","fault_injection":"api_null_keys"}`)
- **Falla:** se anula 30 % de `codigo_municipio` en la capa raw de la API (declarado en `manifest.fault_injected`; no se oculta ni se «arregla»).
- **Regla:** `DQ-A02` (llave de integración no nula ≥ 98 %), **Critical**, afecta **R6 y R7**.
- **Esperado:** extracción success → `validate_api_raw` **failed** → `transform…`, `validate_prepared`, `load_dimensional_dw` no se ejecutan (`upstream_failed` con scheduler) → DW sin cambios.
- Variante: `primary_drop_column` quita `CODIGO_MUNICIPIO` → `DQ-P01`/`DQ-P02` Critical en `validate_primary_raw`.

## Test C — rerun seguro (mismo `batch_id` en un segundo DAG run)
- **Estrategia:** reemplazo atómico en una transacción (advisory lock → `TRUNCATE` → `COPY` dims → hechos → conciliación → auditoría → `COMMIT`).
- **Ante falla parcial:** `ROLLBACK`; el DW conserva el estado previo. **Concurrencia:** `max_active_runs=1` + advisory lock.
- **Evidencia:** `docs/evidence/testC_before.txt` vs `testC_after.txt` (conteos y sumas idénticos); 2 corridas en `ops.load_audit` con las mismas filas.

## Resultados de verificación obtenidos al construir este esqueleto
Entorno: Airflow 3.1.8 real (`airflow dags test`), GX 1.24.1, PostgreSQL embebido, **CSV sintético (5 955 filas) y API mock** (no datos reales).

| Prueba | Resultado observado |
|---|---|
| A | 9/9 tareas `success`, DagRun `success`; DW: fact 5 955, fact_api 64, dim_prestador 1 200 |
| B | `validate_api_raw` `failed` (DQ-A02, 29.69 % nulos; DQ-A05 Warning); transform/prepared/load **no ejecutadas**; 0 filas de `testB` en `ops.load_audit`; evento `task_failed` en `ops.pipeline_run_log` |
| B′ | `primary_drop_column` → DQ-P01 y DQ-P02 Critical, bloqueado |
| C | 2.º run del mismo lote: conteos idénticos (5 955 / 64 / 1 200); 2 cargas auditadas × 6 tablas |
| Rollback | FK huérfana inyectada tras el gate → `DeterministicError`, DW intacto (pytest) |
| Reintento | API caída → `TransientError` → estado `up_for_retry` en Airflow; 403 / JSON inválido → sin reintento (pytest) |
| Suite | `pytest tests` → 15 passed |

**Debe repetirse con los datos reales y el stack Docker** antes de capturar la evidencia final.

### Corrida con la fuente primaria real (turismo_dw.db) + API mock — PostgreSQL embebido
679 548 filas leídas → gate primario sin fallas Critical → 22 filas a cuarentena → 679 526 hechos y 1 033 municipios cargados; DQ-D04 (Warning) reportó 416 036 filas en grupos duplicados del grano. Duración total ≈ 22 s.
