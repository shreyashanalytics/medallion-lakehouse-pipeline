"""Unit tests for the deterministic transaction generator."""

from lakehouse.ingestion import generate_transactions

PRODUCTS = [{"id": i, "price": 9.99 + i} for i in range(1, 6)]
USERS = [{"id": i} for i in range(1, 4)]
INGESTED_AT = "2025-01-02T00:00:00Z"


def _generate(n_rows: int = 2_000):
    return generate_transactions(PRODUCTS, USERS, "2025-01-01", n_rows, 30, INGESTED_AT)


def test_generation_is_deterministic():
    assert _generate() == _generate()


def test_duplicates_are_injected_with_later_ingestion_time():
    rows = _generate()
    ids = [r["transaction_id"] for r in rows]
    assert len(ids) > len(set(ids))
    assert len(rows) == 2_000 + int(2_000 * 0.02)
    assert max(r["ingested_at"] for r in rows) > INGESTED_AT


def test_rows_have_expected_contract():
    expected = {
        "transaction_id", "user_id", "product_id", "quantity", "unit_price",
        "discount_pct", "status", "payment_method", "transaction_ts", "ingested_at",
    }  # fmt: skip
    assert set(_generate(10)[0]) == expected


def test_defects_exist_for_quality_layer_to_catch():
    rows = _generate(20_000)
    assert any(r["user_id"] is None for r in rows)
    assert any(r["quantity"] < 0 for r in rows)
    assert any(r["status"] != r["status"].strip().lower() for r in rows)
