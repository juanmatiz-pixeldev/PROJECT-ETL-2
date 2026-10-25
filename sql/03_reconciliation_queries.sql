-- Consultas de evidencia (Secciones 19-20). Sustituir :batch por el batch_id.
-- Que paso en el lote, en orden:
SELECT ts, task_id, event, status, rows_in, rows_out FROM ops.pipeline_run_log WHERE batch_id = :batch ORDER BY ts;
-- Resultado de validacion por regla:
SELECT gate, rule_id, severity, success, unexpected_count, unexpected_percent, action
FROM ops.validation_result WHERE batch_id = :batch ORDER BY checked_at, gate, rule_id;
-- Conciliacion:
SELECT metric, value FROM ops.batch_reconciliation WHERE batch_id = :batch ORDER BY id;
-- Estado actual del DW (para comparacion antes/despues del rerun, Test C):
SELECT 'dim_ubicacion' t, COUNT(*) n FROM dim_ubicacion UNION ALL
SELECT 'dim_categoria', COUNT(*) FROM dim_categoria UNION ALL
SELECT 'dim_prestador', COUNT(*) FROM dim_prestador UNION ALL
SELECT 'dim_tiempo', COUNT(*) FROM dim_tiempo UNION ALL
SELECT 'fact_turismo_anual', COUNT(*) FROM fact_turismo_anual UNION ALL
SELECT 'fact_api_indicador', COUNT(*) FROM fact_api_indicador;
SELECT SUM(numero_de_empleados) empleos, SUM(numero_de_habitaciones) habitaciones, SUM(numero_de_camas) camas FROM fact_turismo_anual;
