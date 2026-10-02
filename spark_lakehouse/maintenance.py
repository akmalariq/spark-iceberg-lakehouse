"""Lakehouse maintenance: compaction, snapshot expiry, and time-travel demo."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from spark_lakehouse.config import build_spark


def show_snapshots(spark: SparkSession, table: str) -> None:
    query = f"SELECT committed_at, snapshot_id, operation FROM {table}.snapshots ORDER BY committed_at"
    spark.sql(query).show(truncate=False)


def compact(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"CALL lakehouse.system.rewrite_data_files("
        f"table => '{table}', options => map('rewrite-all', 'true'))"
    )


def expire_snapshots(spark: SparkSession, table: str, days: int = 30) -> None:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('gc.enabled' = 'true')")
    cutoff = datetime.now() - timedelta(days=days)
    query = (
        f"CALL lakehouse.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{cutoff.isoformat()[:19]}')"
    )
    spark.sql(query)


def time_travel_demo(spark: SparkSession, table: str, snapshot_id: int | None = None) -> None:
    if snapshot_id is None:
        query = f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 2"
        snaps = spark.sql(query).collect()
        if len(snaps) < 2:
            print("need at least 2 snapshots for time travel demo")
            return
        snapshot_id = snaps[1].snapshot_id
    print(f"querying {table} at snapshot {snapshot_id} (previous committed state)")
    query = f"SELECT count(*) AS rows_at_previous_snapshot FROM {table} VERSION AS OF {snapshot_id}"
    spark.sql(query).show()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="lakehouse.bronze.orders", help="table to inspect/maintain")
    parser.add_argument("--snapshots", action="store_true", help="list snapshots")
    parser.add_argument("--compact", action="store_true", help="rewrite data files")
    parser.add_argument("--expire", action="store_true", help="expire old snapshots")
    parser.add_argument("--demo", action="store_true", help="time-travel demo (previous snapshot)")
    args = parser.parse_args()

    spark = build_spark("maintenance")
    try:
        if args.snapshots:
            show_snapshots(spark, args.table)
        if args.compact:
            compact(spark, args.table)
            print("compaction done")
        if args.expire:
            expire_snapshots(spark, args.table)
            print("snapshot expiry done")
        if args.demo:
            time_travel_demo(spark, args.table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
