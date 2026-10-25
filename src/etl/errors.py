"""Clasificacion de fallos (Seccion 18). El DAG mapea estas clases a la politica de reintentos:

- TransientError      -> se relanza tal cual: Airflow reintenta (retries acotados).
- DeterministicError  -> el DAG lo convierte en AirflowFailException: NO se reintenta.
"""


class PipelineError(Exception):
    """Base."""


class TransientError(PipelineError):
    """Fallo operacional que puede desaparecer en otro intento (timeout, 5xx, 429, BD no disponible)."""


class DeterministicError(PipelineError):
    """Fallo de datos o de contrato; repetir el mismo input reproduce el fallo."""


class CriticalValidationError(DeterministicError):
    """Una o mas reglas de severidad Critical fallaron en un gate de validacion."""

    def __init__(self, gate: str, batch_id: str, failed_rules: list[dict]):
        self.gate = gate
        self.batch_id = batch_id
        self.failed_rules = failed_rules
        ids = ", ".join(r["rule_id"] for r in failed_rules)
        super().__init__(f"[{gate}] batch={batch_id}: reglas CRITICAL incumplidas -> {ids}")
