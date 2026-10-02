import os

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.11.0"

ICEBERG_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
)

NESSIE_URI = os.getenv("NESSIE_URI", "http://localhost:19120/api/v2")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9100")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
WAREHOUSE = os.getenv("WAREHOUSE", "s3a://warehouse/")

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", ".data/raw")


def build_spark(app_name: str = "spark-lakehouse") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.memoryOverhead", "384m")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.executor.memory", "1g")
        .config("spark.jars.packages", ICEBERG_PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_URI)
        .config("spark.sql.catalog.lakehouse.authentication.type", "none")
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.lakehouse.gc.enabled", "true")
        .config("spark.sql.catalog.lakehouse.default-properties.gc.enabled", "true")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", S3_ACCESS_KEY)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", S3_SECRET_KEY)
        .config("spark.driver.extraJavaOptions", "-Daws.region=eu-central-1")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", "eu-central-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
