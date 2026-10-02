# Spark + Iceberg Lakehouse

Batch lakehouse pipeline: **Spark + Apache Iceberg + Nessie + MinIO**, built as
bronze/silver/gold with SCD Type 2, time travel, and compaction. Runs entirely
locally without Docker (Java 21 + Python 3.12 + single binaries).

## Architecture

```mermaid
flowchart LR
    GEN[Data generator<br/>retail: customers/products/orders] -->|CSV batches| BRZ

    subgraph Spark[Spark Session · local[*]]
        BRZ[Bronze<br/>append-only Iceberg tables]
        SIL[Silver<br/>SCD2 dims · cleaned fact_sales]
        GLD[Gold<br/>daily_sales · customer_segments]
        MNT[Maintenance<br/>compaction · expiry · time travel]
        BRZ --> SIL --> GLD
        MNT -.-> BRZ
    end

    subgraph Catalog[Nessie · :19120]
        REF[(git-like versioned refs)]
    end
    subgraph Store[MinIO · :9100]
        W[(s3a://warehouse/<br/>parquet + metadata)]
    end

    Spark --> Catalog
    Spark --> Store
```

## Stack

| Layer | Technology |
|---|---|
| Processing | PySpark 3.5.9 (Java 21) |
| Table format | Apache Iceberg 1.11 (`iceberg-spark-runtime-3.5_2.12` + `iceberg-aws-bundle`) |
| Catalog | Nessie 0.108.4 — git-like versioned catalog (branches, commits, refs) |
| Storage | MinIO (S3-compatible), path-style access |
| Pipeline | bronze → silver → gold, CLI steps + `make full` |

## What it demonstrates

- **Bronze** — append-only ingestion from CSV batches; re-runs append new
  snapshots instead of overwriting (idempotent by snapshot design)
- **Silver** — **SCD Type 2** customer dimension (hash-diff + close/open rows,
  `valid_from/valid_to/is_current`); product dim with insert-if-new guard;
  `fact_sales` with dedup and completed-status filter
- **Gold** — aggregated marts: daily sales by date/channel/category with margin,
  customer segments with AOV
- **Time travel** — query any table `VERSION AS OF <snapshot_id>`; the demo
  shows the bronze orders table at its previous snapshot vs now
- **Compaction** — `rewrite_data_files` collapses small files; snapshot history
  shows the `replace` operation
- **Snapshot expiry** — `expire_snapshots` GCs old snapshots (Nessie sets
  `gc.enabled=false` on tables; the pipeline enables it explicitly)
- **Data quality gate** — 41 declarative checks across all 9 tables
  (`spark_lakehouse/dq.py` + `dq/specs.json`), CI-blocking on error severity,
  with a static HTML report

## Data quality framework

Declarative, expectation-style checks executed via Spark SQL — built in-house
rather than GX/Soda because the lakehouse has no SQL engine and GX's Spark
integration is fragile in CI. Check types:

| Type | Meaning |
|---|---|
| `not_null` / `unique` | missing values / duplicate groups |
| `positive` / `non_negative` | value bounds |
| `in_set` | whitelist (enums, booleans, numbers) |
| `row_count` | volume band (monitor) |
| `min_date_before` / `max_date_after` | data coverage (monitor) |
| `max_age_days` / `future_beyond` | freshness / future-dated garbage |
| `fk` | referential integrity against another table |
| `sql` | arbitrary violating-rows query |

- Severity `error` fails the pipeline (`make full` and CI); severity `warn`
  flags without blocking (e.g., the intentional 114 SCD2 customer histories)
- Outputs `dq/latest.json` (machine-readable) + `dq/report.html` (static,
  self-contained — uploaded as a CI artifact)

The gate has already caught real bugs during development: item prices that
didn't match product costs (nonsense margins) and cross-batch price drift for
the same `product_id`.

## Quick start (local, no Docker)

```bash
uv sync --group dev

# 1. Install services (once): Java 21, MinIO, Nessie runner jar
#    see docs/SETUP.md

# 2. Start MinIO + Nessie (systemd user units)
./scripts/start-services.sh

# 3. Create the warehouse bucket (once)
mc alias set local http://localhost:9100 minioadmin minioadmin
mc mb local/warehouse

# 4. Full pipeline: generate -> bronze -> silver -> gold
make full

# 5. Lakehouse maintenance + time travel demo
make maintenance

# 6. DQ report (also runs at the end of `make full`)
make dq
make maintenance
```

## Verify

```bash
make lint    # ruff
make test    # generator determinism tests
```

## CI

`.github/workflows/ci.yml` — lint + unit tests, then the full
bronze/silver/gold pipeline against MinIO + Nessie service containers on
GitHub Actions. The lakehouse is proven end-to-end in CI, not just locally.

## Project layout

```
spark_lakehouse/
├── config.py        # Spark session builder (catalog, S3 creds, packages)
├── generate.py      # deterministic retail data generator (2 batches)
├── bronze.py        # CSV -> Iceberg bronze tables
├── silver.py        # SCD2 dims + fact_sales
├── gold.py          # aggregated marts
└── maintenance.py   # snapshots / time travel / compaction / expiry
```

## Notes

- Nessie tables default to `gc.enabled=false`; compaction/expiry set it
  explicitly (`docs/SETUP.md` explains why).
- MinIO uses API port **9100** (ClickHouse owns 9000) and region
  `eu-central-1` (MinIO's default).
- The generator is deterministic: batch 2 mutates exactly 114 of 500 customers
  (10% segment + 14% city) so SCD2 produces a predictable, testable history.
