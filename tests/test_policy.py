"""La politica de severidad decide; GX solo detecta (Seccion 8.1)."""
import pytest

from etl.config import Settings
from etl.errors import CriticalValidationError
from etl.validation.policy import apply_policy


def _row(rid, sev, ok):
    return {"rule_id": rid, "severity": sev, "success": ok, "data_object": "x", "expectation_type": "t",
            "requirement": "R1", "element_count": 10, "unexpected_count": 0 if ok else 3,
            "unexpected_percent": 0.0 if ok else 30.0, "observed_value": None}


@pytest.fixture
def s(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DW_URL", "sqlite:///:memory:")  # ops no disponible -> best-effort, no debe romper
    return Settings()


def test_critical_stops(s):
    with pytest.raises(CriticalValidationError) as e:
        apply_policy(s, "api_raw", "b", [_row("A", "critical", False), _row("B", "warning", False)])
    assert [r["rule_id"] for r in e.value.failed_rules] == ["A"]


def test_warning_continues_with_evidence(s, tmp_path):
    out = apply_policy(s, "api_raw", "b", [_row("A", "critical", True), _row("B", "warning", False)])
    assert out["decision"] == "continue_with_warning" and out["warnings"] == ["B"]
    assert (tmp_path / "validation/b/api_raw.json").exists()


def test_informational_continues(s):
    assert apply_policy(s, "api_raw", "b", [_row("I", "informational", False)])["decision"] == "continue"


def test_untraced_expectation_is_treated_as_critical(s):
    r = _row("UNTRACED", "critical", False)
    with pytest.raises(CriticalValidationError):
        apply_policy(s, "g", "b", [r])
