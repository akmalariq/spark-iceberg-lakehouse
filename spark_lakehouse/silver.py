"""Silver layer: conformed dimensions (SCD Type 2) and cleaned facts."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from spark_lakehouse.config import build_spark


def build_dim_customers(spark: SparkSession) -> None:
    spark.sql("DROP VIEW IF EXISTS new_customer_states")
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW new_customer_states AS
        SELECT
            customer_id,
            name,
            city,
            segment,
            signup_date,
            DATE '2026-12-31' AS valid_from,
            DATE '9999-12-31' AS valid_to,
            TRUE AS is_current,
            sha2(concat_ws('|', city, segment), 256) AS hash_value
        FROM lakehouse.bronze.customers
        WHERE NOT EXISTS (
            SELECT 1 FROM lakehouse.silver.dim_customers d
            WHERE d.customer_id = lakehouse.bronze.customers.customer_id
              AND d.is_current = TRUE
              AND d.hash_value = sha2(
                  concat_ws('|', lakehouse.bronze.customers.city, lakehouse.bronze.customers.segment), 256)
        )
        """
    )
    spark.sql("CACHE TABLE new_customer_states")
    spark.sql(
        """
        UPDATE lakehouse.silver.dim_customers
        SET valid_to = DATE '2026-12-31', is_current = FALSE
        WHERE is_current = TRUE
          AND EXISTS (
              SELECT 1 FROM new_customer_states n
              WHERE n.customer_id = lakehouse.silver.dim_customers.customer_id
          )
        """
    )
    spark.sql(
        """
        INSERT INTO lakehouse.silver.dim_customers
        SELECT * FROM new_customer_states
        """
    )


def build_dim_products(spark: SparkSession) -> None:
    spark.sql(
        """
        INSERT INTO lakehouse.silver.dim_products
        SELECT
            product_id,
            sku,
            name,
            category,
            subcategory,
            unit_price,
            cost,
            launch_date
        FROM lakehouse.bronze.products
        WHERE NOT EXISTS (
            SELECT 1 FROM lakehouse.silver.dim_products d
            WHERE d.product_id = lakehouse.bronze.products.product_id
        )
        """
    )


def build_fact_sales(spark: SparkSession) -> None:
    spark.sql(
        """
        INSERT INTO lakehouse.silver.fact_sales
        SELECT
            o.order_id,
            o.order_date,
            o.channel,
            o.customer_id,
            i.product_id,
            i.quantity,
            i.unit_price_at_sale,
            i.discount_pct,
            round(i.quantity * i.unit_price_at_sale * (1 - i.discount_pct), 2) AS gross_amount,
            round(i.quantity * p.cost, 2) AS cost_amount
        FROM lakehouse.bronze.orders AS o
        JOIN lakehouse.bronze.order_items AS i ON o.order_id = i.order_id
        JOIN lakehouse.silver.dim_products AS p ON p.product_id = i.product_id
        WHERE o.status = 'completed'
          AND NOT EXISTS (
              SELECT 1 FROM lakehouse.silver.fact_sales f
              WHERE f.order_id = o.order_id
          )
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--init", action="store_true", help="create tables (run once)")
    args = parser.parse_args()

    spark = build_spark("silver")
    try:
        if args.init:
            for ddl in [
                """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_customers (
                    customer_id LONG,
                    name STRING,
                    city STRING,
                    segment STRING,
                    signup_date DATE,
                    valid_from DATE,
                    valid_to DATE,
                    is_current BOOLEAN,
                    hash_value STRING
                ) USING iceberg""",
                """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_products (
                    product_id LONG,
                    sku STRING,
                    name STRING,
                    category STRING,
                    subcategory STRING,
                    unit_price DECIMAL(14,2),
                    cost DECIMAL(14,2),
                    launch_date DATE
                ) USING iceberg""",
                """CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_sales (
                    order_id LONG,
                    order_date TIMESTAMP,
                    channel STRING,
                    customer_id LONG,
                    product_id LONG,
                    quantity INT,
                    unit_price_at_sale DECIMAL(14,2),
                    discount_pct DOUBLE,
                    gross_amount DECIMAL(14,2),
                    cost_amount DECIMAL(14,2)
                ) USING iceberg""",
            ]:
                spark.sql(ddl)
        build_dim_products(spark)
        build_dim_customers(spark)
        build_fact_sales(spark)
        print("silver complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
