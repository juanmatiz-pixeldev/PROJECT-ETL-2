"""Politica de errores de la extraccion API (Seccion 18): transitorio -> reintentable, deterministico -> no."""
import pytest

import mock_api
from etl.config import Settings
from etl.errors import DeterministicError, TransientError
from etl.extract_api import extract_api


def _settings(tmp_path, monkeypatch, url, **env):
    monkeypatch.setenv("API_BASE_URL", url)
    monkeypatch.setenv("API_DATASET_ID", "abcd-1234")
    monkeypatch.setenv("API_PAGE_SIZE", env.get("page", "20"))
    monkeypatch.setenv("API_FIELD_MAP", '{"cod_municipio":"codigo_municipio","ano":"anio"}')
    monkeypatch.setenv("API_VALUE_FIELDS", "poblacion_total")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("API_READ_TIMEOUT_S", "2")
    return Settings()


def test_pagination_and_manifest(tmp_path, monkeypatch):
    srv, state, url = mock_api.start("ok")
    s = _settings(tmp_path, monkeypatch, url)
    meta = extract_api(s, "b1")
    assert meta["row_count"] == 64 and state["calls"] == 4          # 64 filas / 20 por pagina = 4 paginas
    manifest = (tmp_path / "raw/api/b1/manifest.json").read_text()
    assert "abcd-1234" in manifest and "extracted_at" in manifest
    srv.shutdown()


def test_5xx_is_transient(tmp_path, monkeypatch):
    srv, _, url = mock_api.start("flaky", fail_first=5)
    with pytest.raises(TransientError):
        extract_api(_settings(tmp_path, monkeypatch, url), "b2")
    srv.shutdown()


def test_403_is_deterministic(tmp_path, monkeypatch):
    srv, _, url = mock_api.start("forbidden")
    with pytest.raises(DeterministicError):
        extract_api(_settings(tmp_path, monkeypatch, url), "b3")
    srv.shutdown()


def test_invalid_json_is_deterministic(tmp_path, monkeypatch):
    srv, _, url = mock_api.start("badjson")
    with pytest.raises(DeterministicError):
        extract_api(_settings(tmp_path, monkeypatch, url), "b4")
    srv.shutdown()


def test_connection_refused_is_transient(tmp_path, monkeypatch):
    with pytest.raises(TransientError):
        extract_api(_settings(tmp_path, monkeypatch, "http://127.0.0.1:9"), "b5")


def test_missing_dataset_id_is_deterministic(tmp_path, monkeypatch):
    s = _settings(tmp_path, monkeypatch, "http://127.0.0.1:9")
    monkeypatch.setenv("API_DATASET_ID", "xxxx-xxxx")
    with pytest.raises(DeterministicError):
        extract_api(Settings(), "b6")
