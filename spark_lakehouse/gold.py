"""Gold layer: aggregated marts for analytics."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from spark_lakehouse.config import build_spark


def build_daily_sales(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.daily_sales (
            sale_date DATE,
            channel STRING,
            category STRING,
            orders LONG,
            units LONG,
            gross_amount DECIMAL(18,2),
            net_amount DECIMAL(18,2),
            cost_amount DECIMAL(18,2),
            margin_amount DECIMAL(18,2),
            margin_pct DOUBLE
        ) USING iceberg
        """
    )
    spark.sql(
        """
        INSERT INTO lakehouse.gold.daily_sales
        SELECT
            to_date(f.order_date) AS sale_date,
            f.channel,
            p.category,
            count(DISTINCT f.order_id) AS orders,
            sum(f.quantity) AS units,
            round(sum(f.gross_amount), 2) AS gross_amount,
            round(sum(f.gross_amount), 2) AS net_amount,
            round(sum(f.cost_amount), 2) AS cost_amount,
            round(sum(f.gross_amount) - sum(f.cost_amount), 2) AS margin_amount,
            round(
                (sum(f.gross_amount) - sum(f.cost_amount)) / NULLIF(sum(f.gross_amount), 0), 4
            ) AS margin_pct
        FROM lakehouse.silver.fact_sales AS f
        JOIN lakehouse.silver.dim_products AS p ON p.product_id = f.product_id
        WHERE NOT EXISTS (
            SELECT 1 FROM lakehouse.gold.daily_sales g
            WHERE g.sale_date = to_date(f.order_date)
              AND g.channel = f.channel
              AND g.category = p.category
        )
        GROUP BY 1, 2, 3
        """
    )


def build_customer_segments(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_segments (
            segment STRING,
            customers LONG,
            total_gross DECIMAL(18,2),
            avg_order_value DECIMAL(18,2)
        ) USING iceberg
        """
    )
    spark.sql(
        """
        INSERT INTO lakehouse.gold.customer_segments
        SELECT
            c.segment,
            count(DISTINCT f.customer_id) AS customers,
            round(sum(f.gross_amount), 2) AS total_gross,
            round(sum(f.gross_amount) / NULLIF(count(DISTINCT f.order_id), 0), 2) AS avg_order_value
        FROM lakehouse.silver.fact_sales AS f
        JOIN lakehouse.silver.dim_customers AS c
          ON c.customer_id = f.customer_id AND c.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 FROM lakehouse.gold.customer_segments g WHERE g.segment = c.segment
        )
        GROUP BY 1
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    spark = build_spark("gold")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        build_daily_sales(spark)
        build_customer_segments(spark)
        print("gold complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
