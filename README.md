# Proyecto ETL - Ingeniería de Datos para el Desarrollo Sostenible en Colombia (ODS 8)
**Fase 2: Automatización del pipeline, integración de API y validación de datos**

> Repositorio **de la Segunda Entrega** (autocontenido). Primera Entrega: ⟦URL del repo de la Primera Entrega⟧ (RNT → DW MySQL, R1–R5).
> Equipo ⟦X⟧ · Curso ETL (G01) · UAO.

Este README sigue el orden exigido por el enunciado (§24). Cada sección indica **responsable** y **estado**.
`✅` = implementado y verificado · `🟡` = base funcional lista, el responsable debe refinar · `⬜` = pendiente.

| § | Contenido | Responsable | Estado |
|---|---|---|---|
| 1 | Problema y objetivo analítico | ⟦⟧ | ⬜ (reutilizar README 1ª entrega) |
| 2 | Baseline Primera Entrega + enlace | ⟦⟧ | ⬜ |
| 3 | Estado de R1–R5 (Maintain/Refine/Replace) | ⟦⟧ | ⬜ |
| 4 | Requerimientos R6 y R7 | ⟦⟧ | ⬜ |
| 5 | Fuente primaria y API | Infra/API | 🟡 → [`docs/api_source.md`](docs/api_source.md) (falta fijar dataset) |
| 6 | Valor analítico de la API | ⟦⟧ | ⬜ |
| 7 | Arquitectura | Infra/API | ✅ → [`docs/architecture.png`](docs/architecture.png) |
| 8–9 | Profiling, riesgos y reglas | ⟦⟧ | 🟡 reglas base en [`gx/rules_catalog.yaml`](gx/rules_catalog.yaml) |
| 10–11 | Great Expectations y validación raw | ⟦⟧ | 🟡 `src/etl/validation/`, `src/etl/raw_validate.py` |
| 12 | Transformación e integración | ⟦⟧ | 🟡 `src/etl/transform.py` (IDs T01–T06) |
| 13 | Modelo dimensional refinado | ⟦⟧ | 🟡 `sql/02_create_dw.sql` (línea base + `fact_api_indicador`) |
| 14 | Validación de datos preparados | ⟦⟧ | 🟡 `src/etl/prepare_validate.py` |
| 15 | Implementación y carga del DW | ⟦⟧ | ✅ carga atómica `src/etl/load.py` |
| 16 | Diseño del DAG e interfaces | Infra/API | ✅ [`docs/orchestration.md`](docs/orchestration.md) |
| 17 | Fallos y reintentos | Infra/API | ✅ [`docs/orchestration.md`](docs/orchestration.md#18-política-de-fallos-y-reintentos) |
| 18–20 | Evidencia A/B y rerun | Infra/API | 🟡 procedimiento y resultados locales en [`docs/reliability_tests.md`](docs/reliability_tests.md); **capturas pendientes** |
| 21–22 | Dashboard y trazabilidad R→KPI | ⟦⟧ | ⬜ |
| 23 | Setup y ejecución | Infra/API | ✅ abajo |
| 24 | Supuestos y limitaciones | todos | 🟡 abajo |
| 25 | Equipo y responsabilidades | ⟦⟧ | ⬜ |

## 23. Setup y ejecución

Requisitos: Docker + Docker Compose v2.

```bash
cp .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # pegar en AIRFLOW__CORE__FERNET_KEY
# 1) Fuente primaria: ya incluida en data/source/turismo_dw.db (SQLite de la Primera Entrega, se abre en SOLO LECTURA y se
#    reconstruye la vista de 14 columnas del RNT). Alternativa: un CSV del RNT en data/source/ y ajustar PRIMARY_SOURCE_PATH.
#    Prueba de humo sintetica: python scripts/make_sample_data.py data/source/rnt_sample.csv
# 2) API: fijar API_DATASET_ID, API_FIELD_MAP, API_VALUE_FIELDS en .env (ver docs/api_source.md)
docker compose up -d --build
# UI: http://localhost:8080 (sin login en modo desarrollo) · DW: localhost:5433
scripts/run_reliability_tests.sh A        # Test A
scripts/run_reliability_tests.sh B        # Test B (falla Critical)
scripts/run_reliability_tests.sh C        # Test C (rerun) y luego C_after
```
Sin Docker (desarrollo): `pip install -r requirements-dev.txt && pytest tests && PYTHONPATH=src python tests/e2e_local.py`.

Estructura: `dags/` (solo orquestación) · `src/etl/` (extracción, validación, transformación, carga) · `gx/` (catálogo de reglas) · `sql/` · `tests/` · `docs/` · `data/` (local, ignorado por git).

## Hallazgos sobre la fuente primaria real (turismo_dw.db, 679 548 hechos)
- `dim_ubicacion` tiene 1 100 filas pero 1 034 códigos únicos: **50 códigos de municipio figuran con departamentos distintos** (p. ej. 5101 BOLIVAR en 4 departamentos). Se conserva el primero por código (regla de la Primera Entrega); la atribución departamental de esos municipios puede ser incorrecta (afecta R1).
- **22 filas con `CODIGO_MUNICIPIO = 1`** (no DIVIPOLA): detectadas por DQ-P02 y puestas en cuarentena (T02).
- **61 % de las filas pertenecen a grupos repetidos en el grano declarado** (PST, municipio, categoría, año): detectado por DQ-D04 (Warning). La declaración de grano de la Primera Entrega debe revisarse en el modelo refinado.
- Esta fuente ya es la salida limpia de la Primera Entrega (texto normalizado, `NO REGISTRA` imputado): los problemas de calidad *crudos* del CSV original no son observables aquí.

## 24. Supuestos y limitaciones (infraestructura)
- `docker-compose.yaml` **no se pudo ejecutar** en el entorno de construcción (sin Docker); sí se verificó el DAG con Airflow 3.1.8 (`dags test`), PostgreSQL embebido y API mock. Validar el stack Docker antes de capturar evidencia.
- Las pruebas usaron CSV sintético y API mock; umbrales y reglas son una **propuesta** que debe contrastarse con el profiling real.
- Autenticación de Airflow: `SimpleAuthManager` con todos administradores (solo local).
- Las fuentes son snapshots completos; por eso la carga es reemplazo atómico, no incremental.
- La API depende de un tercero: conservar evidencia previa (`data/raw/api/<batch>`) para la defensa.
