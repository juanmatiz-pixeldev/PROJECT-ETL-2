from etl.batch import resolve_batch_id


def test_explicit_batch_wins_and_is_stable():
    assert resolve_batch_id("lote-2026-10", "manual__x") == resolve_batch_id("lote-2026-10", "manual__y") == "lote202610"


def test_derived_from_run_id():
    assert resolve_batch_id("", "manual__2026-10-25T10:00:00+00:00").startswith("manual20261025")
