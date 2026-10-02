.PHONY: install start stop generate bronze silver gold full maintenance dq lint test

install:
	uv sync --group dev

start:
	./scripts/start-services.sh

stop:
	./scripts/stop-services.sh

generate:
	uv run python -m spark_lakehouse.generate --batch 1
	uv run python -m spark_lakehouse.generate --batch 2

bronze:
	uv run python -m spark_lakehouse.bronze --batch 1
	uv run python -m spark_lakehouse.bronze --batch 2

silver:
	uv run python -m spark_lakehouse.silver --init
	uv run python -m spark_lakehouse.silver

gold:
	uv run python -m spark_lakehouse.gold

# full pipeline: generate -> bronze -> silver -> gold -> DQ gate
full: generate bronze silver gold dq

# time-travel + compaction + snapshot expiry demo
maintenance:
	uv run python -m spark_lakehouse.maintenance --table lakehouse.bronze.orders --snapshots
	uv run python -m spark_lakehouse.maintenance --table lakehouse.bronze.orders --demo
	uv run python -m spark_lakehouse.maintenance --table lakehouse.bronze.orders --compact
	uv run python -m spark_lakehouse.maintenance --table lakehouse.bronze.orders --expire

# data-quality gate: 41 checks across 9 tables; fails on error severity
dq:
	uv run python -m spark_lakehouse.dq --spec dq/specs.json --json dq/latest.json --report dq/report.html

lint:
	uv run ruff check .

test:
	uv run pytest -q
