#!/usr/bin/env bash
# Pruebas de confiabilidad (Seccion 20) sobre el stack Docker. Uso: scripts/run_reliability_tests.sh [A|B|C]
# A: corrida exitosa | B: falla Critical controlada | C: rerun del MISMO lote (comparar conteos antes/despues)
set -euo pipefail
DAG=etl_turismo_second_delivery
trigger() { docker compose exec -T airflow-scheduler airflow dags trigger "$DAG" --conf "$1"; }
counts() {
  docker compose exec -T postgres-dw psql -U "${DW_USER:-dw_user}" -d "${DW_NAME:-dw_turismo_ods8}" -At -c \
   "SELECT 'fact_turismo_anual',COUNT(*),SUM(numero_de_empleados) FROM fact_turismo_anual UNION ALL SELECT 'fact_api_indicador',COUNT(*),SUM(valor) FROM fact_api_indicador UNION ALL SELECT 'dim_prestador',COUNT(*),0 FROM dim_prestador"
}
case "${1:-}" in
  A) trigger '{"batch_id":"testA","fault_injection":"none"}' ;;
  B) trigger '{"batch_id":"testB","fault_injection":"api_null_keys"}'   # alternativa: primary_drop_column
     echo "Esperado: validate_api_raw=failed (DQ-A02 Critical); transform/validate_prepared/load = upstream_failed" ;;
  C) echo "== ANTES"; counts | tee docs/evidence/testC_before.txt
     trigger '{"batch_id":"testA","fault_injection":"none"}'
     echo "Espere a que termine el DAG run y ejecute: scripts/run_reliability_tests.sh C_after" ;;
  C_after) echo "== DESPUES"; counts | tee docs/evidence/testC_after.txt
     diff docs/evidence/testC_before.txt docs/evidence/testC_after.txt && echo "IDENTICO: rerun sin duplicados" ;;
  *) echo "Uso: $0 A|B|C|C_after"; exit 1 ;;
esac
