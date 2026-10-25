# Fuente API — documentación técnica (Sección 4.1)

> **Estado:** la extracción está implementada y probada contra un servidor mock con la forma de Socrata SODA.
> **Falta decidir y verificar el dataset real** (`API_DATASET_ID`). Completar los campos marcados ⟦…⟧ antes de entregar.

| Ítem | Documentación |
|---|---|
| Proveedor y propósito | ⟦Organización y dataset elegido⟧ publicado en el portal Datos Abiertos Colombia (datos.gov.co) mediante la API Socrata SODA. Debe aportar información **por municipio y año** que el RNT no tiene (p. ej. población proyectada, para normalizar empleo/camas per cápita). |
| Valor analítico | ⟦Por qué R6 y R7 no se pueden responder solo con el RNT: el RNT mide oferta (PST, camas, empleos) pero no tamaño/demanda del territorio.⟧ |
| Endpoint y parámetros | `GET {API_BASE_URL}/resource/{API_DATASET_ID}.json` · `$limit` (=`API_PAGE_SIZE`), `$offset`, `$order=:id` (orden estable para paginar), `$where` opcional (`API_WHERE`). Alcance: años del RNT (2019–2026). |
| Respuesta | JSON: lista de objetos planos. Se retienen la llave municipal, el año y los campos de `API_VALUE_FIELDS`; se renombran a `codigo_municipio`, `anio` mediante `API_FIELD_MAP`. Se agregan `_extracted_at` y `_batch_id`. |
| Autenticación | Lectura pública. Opcional: token de aplicación Socrata en el header `X-App-Token` (variable `API_APP_TOKEN`, nunca en el repo ni en los manifiestos). |
| Comportamiento operacional | Paginación hasta página corta (tope `API_MAX_PAGES`). Timeouts (connect=`API_CONNECT_TIMEOUT_S`, read=`API_READ_TIMEOUT_S`). Transitorios (timeout, conexión, HTTP 408/425/429/5xx) → reintento de Airflow; 4xx restantes, JSON inválido, respuesta vacía → falla determinística sin reintento. Timestamp de extracción en el manifiesto. |
| Reproducibilidad | `cp .env.example .env`, fijar `API_DATASET_ID` y mapeo de campos, `docker compose up`, disparar el DAG. Evidencia cruda: `data/raw/api/<batch>/manifest.json` (endpoint, parámetros, estado y latencia por página, conteos, timestamp). Verificación manual: abrir el endpoint con `$limit=5`. |

## Cómo elegir y configurar el dataset
1. En datos.gov.co buscar un dataset con **código DIVIPOLA de municipio** y **año**. Abrir *API* → copiar el identificador `xxxx-xxxx`.
2. Revisar nombres reales de columnas y escribir `API_FIELD_MAP` (campo API → `codigo_municipio` / `anio`) y `API_VALUE_FIELDS`.
3. Verificar: `curl "https://www.datos.gov.co/resource/<ID>.json?\$limit=3"`.
4. Atención a la llave: el RNT trae `CODIGO_MUNICIPIO` como entero sin cero inicial (`05001` → `5001`); el pipeline normaliza ambos lados a entero.
