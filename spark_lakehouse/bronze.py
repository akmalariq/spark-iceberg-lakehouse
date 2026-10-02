"""Bronze layer: load raw CSVs into Iceberg tables (append-only, immutable)."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from spark_lakehouse.config import RAW_DATA_DIR, build_spark

SCHEMAS = {
    "customers": "customer_id LONG, name STRING, city STRING, segment STRING, signup_date DATE",
    "products": (
        "product_id LONG, sku STRING, name STRING, category STRING, subcategory STRING, "
        "unit_price DECIMAL(14,2), cost DECIMAL(14,2), launch_date DATE"
    ),
    "orders": "order_id LONG, customer_id LONG, order_date TIMESTAMP, channel STRING, status STRING",
    "order_items": (
        "order_item_id STRING, order_id LONG, product_id LONG, quantity INT, "
        "unit_price_at_sale DECIMAL(14,2), discount_pct DOUBLE"
    ),
}


def load_batch(spark: SparkSession, batch_dir: Path) -> None:
    for name, schema in SCHEMAS.items():
        csv_file = batch_dir / f"{name}_b{batch_dir.name[-1]}.csv"
        df = (
            spark.read.option("header", True)
            .schema(schema)
            .csv(str(csv_file))
        )
        df.createOrReplaceTempView(f"raw_{name}")
        table_name = f"lakehouse.bronze.{name}"
        writer = df.writeTo(table_name)
        if spark.catalog.tableExists(table_name):
            writer.append()
        else:
            writer.create()
        rows = df.count()
        print(f"bronze.{name}: {rows} rows ({batch_dir.name})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=int, required=True, choices=[1, 2])
    args = parser.parse_args()

    batch_dir = Path(RAW_DATA_DIR) / f"batch{args.batch}"
    if not batch_dir.exists():
        raise SystemExit(f"missing raw dir: {batch_dir} (run generate first)")

    spark = build_spark("bronze")
    try:
        load_batch(spark, batch_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
