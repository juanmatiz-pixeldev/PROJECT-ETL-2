-- Esquema operacional (observabilidad, Seccion 19). Idempotente: seguro de ejecutar en cada corrida.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id    TEXT,
    run_id    TEXT,
    task_id   TEXT NOT NULL,
    batch_id  TEXT NOT NULL,
    source    TEXT,
    event     TEXT NOT NULL,
    status    TEXT NOT NULL,           -- ok | warning | error
    rows_in   BIGINT,
    rows_out  BIGINT,
    detail    JSONB
);
CREATE INDEX IF NOT EXISTS ix_run_log_batch ON ops.pipeline_run_log (batch_id, ts);

CREATE TABLE IF NOT EXISTS ops.validation_result (
    id                 BIGSERIAL PRIMARY KEY,
    checked_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id           TEXT NOT NULL,
    run_id             TEXT,
    gate               TEXT NOT NULL,  -- primary_raw | api_raw | prepared
    data_object        TEXT,
    rule_id            TEXT NOT NULL,
    expectation_type   TEXT,
    severity           TEXT NOT NULL,  -- critical | warning | informational
    requirement        TEXT,
    success            BOOLEAN NOT NULL,
    element_count      BIGINT,
    unexpected_count   BIGINT,
    unexpected_percent DOUBLE PRECISION,
    observed_value     TEXT,
    action             TEXT NOT NULL   -- pass | continue | continue_with_warning | stop
);
CREATE INDEX IF NOT EXISTS ix_validation_batch ON ops.validation_result (batch_id, gate);

CREATE TABLE IF NOT EXISTS ops.load_audit (
    id          BIGSERIAL PRIMARY KEY,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id    TEXT NOT NULL,
    run_id      TEXT,
    table_name  TEXT NOT NULL,
    rows_loaded BIGINT NOT NULL,
    strategy    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.batch_reconciliation (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id  TEXT NOT NULL,
    run_id    TEXT,
    metric    TEXT NOT NULL,
    value     BIGINT NOT NULL
);
