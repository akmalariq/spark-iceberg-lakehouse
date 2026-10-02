from pathlib import Path

from spark_lakehouse import generate

EXPECTED_CHANGED = 114


def test_generator_determinism(tmp_path: Path):
    generate.generate_customers(tmp_path / "batch1", 1, 6)
    generate.generate_customers(tmp_path / "batch2", 2, 6)
    import csv

    def read(p):
        with open(p) as f:
            return list(csv.DictReader(f))

    b1 = read(tmp_path / "batch1" / "customers_b1.csv")
    b2 = read(tmp_path / "batch2" / "customers_b2.csv")
    assert len(b1) == len(b2) == generate.CUSTOMERS

    changed = sum(
        1
        for a, b in zip(b1, b2, strict=True)
        if (a["city"], a["segment"]) != (b["city"], b["segment"])
    )
    assert changed == EXPECTED_CHANGED
    assert changed < generate.CUSTOMERS * 0.3


def test_generator_products(tmp_path: Path):
    generate.generate_products(tmp_path / "batch1", 1, 6)
    import csv

    with open(tmp_path / "batch1" / "products_b1.csv") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 200
    assert all(float(r["unit_price"]) > 0 for r in rows)


def test_generator_orders_linked(tmp_path: Path):
    generate.generate_products(tmp_path / "batch1", 1, 6)
    generate.generate_orders(tmp_path / "batch1", 1, 6)
    import csv

    with open(tmp_path / "batch1" / "orders_b1.csv") as f:
        orders = list(csv.DictReader(f))
    with open(tmp_path / "batch1" / "order_items_b1.csv") as f:
        items = list(csv.DictReader(f))

    order_ids = {o["order_id"] for o in orders}
    assert all(i["order_id"] in order_ids for i in items)
    assert len({o["order_id"] for o in orders}) == len(orders)
