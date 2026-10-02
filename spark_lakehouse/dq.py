"""Declarative data-quality engine for the lakehouse.

Runs expectation-style checks against Iceberg tables via Spark SQL,
emits machine-readable JSON + a static HTML report, and fails the process
(CI gate) when any error-severity check fails.

Check types:
  not_null       column has no NULLs
  unique         no duplicate values across column(s)
  positive       values > 0
  non_negative   values >= 0
  in_set         all values within a whitelist
  row_count      count within [min, max]
  min_date_before  min(column) <= date
  max_date_after   max(column) >= date
  max_age_days   max(column) not older than N days vs the run date
  future_beyond  max(column) not more than N days in the future
  fk             every value in column exists in ref_table.ref_column
  sql            arbitrary SQL returning the number of violating rows
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from spark_lakehouse.config import build_spark

SEVERITIES = ("error", "warn")


def evaluate(measured, spec: dict) -> tuple[bool, str]:
    """Pure comparison logic: (passed, description)."""
    check_type = spec["type"]
    if check_type == "not_null":
        n = measured
        return n == 0, f"{n} NULL rows"
    if check_type == "unique":
        n = measured
        return n == 0, f"{n} duplicate groups"
    if check_type == "positive":
        n = measured
        return n == 0, f"{n} non-positive rows"
    if check_type == "non_negative":
        n = measured
        return n == 0, f"{n} negative rows"
    if check_type == "in_set":
        n = measured
        return n == 0, f"{n} values outside whitelist"
    if check_type == "row_count":
        lo, hi = spec.get("min", 0), spec.get("max", float("inf"))
        return lo <= measured <= hi, f"count={measured} (expected [{lo}, {hi}])"
    if check_type == "min_date_before":
        limit = date.fromisoformat(spec["date"])
        ok = measured is None or measured <= limit
        return ok, f"min={measured} (must be <= {limit})"
    if check_type == "max_date_after":
        limit = date.fromisoformat(spec["date"])
        ok = measured is not None and measured >= limit
        return ok, f"max={measured} (must be >= {limit})"
    if check_type == "max_age_days":
        limit = date.today() - timedelta(days=spec["days"])
        ok = measured is not None and measured >= limit
        return ok, f"max={measured} (must be >= {limit})"
    if check_type == "future_beyond":
        limit = date.today() + timedelta(days=spec["days"])
        ok = measured is None or measured <= limit
        return ok, f"max={measured} (must be <= {limit})"
    if check_type == "fk":
        return measured == 0, f"{measured} orphaned values"
    if check_type == "sql":
        return measured == 0, f"{measured} violating rows"
    raise ValueError(f"unknown check type: {check_type}")


def build_queries(table: str, spec: dict) -> str:
    check_type = spec["type"]
    cols = ", ".join(f"`{c}`" for c in (spec.get("columns") or []))
    col = spec.get("column")
    if check_type == "not_null":
        return f"SELECT count(*) AS n FROM {table} WHERE `{col}` IS NULL"
    if check_type == "unique":
        return f"SELECT count(*) AS n FROM (SELECT {cols} FROM {table} GROUP BY {cols} HAVING count(*) > 1)"
    if check_type == "positive":
        return f"SELECT count(*) AS n FROM {table} WHERE `{col}` <= 0"
    if check_type == "non_negative":
        return f"SELECT count(*) AS n FROM {table} WHERE `{col}` < 0"
    if check_type == "in_set":
        def lit(v):
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, (int, float)):
                return str(v)
            return f"'{v}'"

        allowed = ", ".join(lit(v) for v in spec["values"])
        return f"SELECT count(*) AS n FROM {table} WHERE NOT `{col}` IN ({allowed})"
    if check_type == "row_count":
        return f"SELECT count(*) AS n FROM {table}"
    if check_type == "min_date_before":
        return f"SELECT min(`{col}`) AS v FROM {table}"
    if check_type == "max_date_after":
        return f"SELECT max(`{col}`) AS v FROM {table}"
    if check_type == "max_age_days":
        return f"SELECT max(`{col}`) AS v FROM {table}"
    if check_type == "future_beyond":
        return f"SELECT max(`{col}`) AS v FROM {table}"
    if check_type == "fk":
        ref = spec["ref_table"]
        ref_col = spec.get("ref_column", spec["column"])
        return (
            f"SELECT count(*) AS n FROM {table} AS t "
            f"WHERE NOT EXISTS (SELECT 1 FROM {ref} AS r WHERE r.`{ref_col}` = t.`{col}`)"
        )
    if check_type == "sql":
        return spec["query"]
    raise ValueError(f"unknown check type: {check_type}")


def run_checks(spark: SparkSession, spec: dict) -> dict:
    table = spec["table"]
    checks = []
    for check in spec["checks"]:
        try:
            query = build_queries(table, check)
            row = spark.sql(query).collect()[0]
            measured = row[0]
            if isinstance(measured, datetime):
                measured = measured.date()
            passed, detail = evaluate(measured, check)
        except Exception as exc:  # noqa: BLE001 - a failed check must not kill the run
            passed, detail, measured = False, f"error evaluating: {exc}", None
        checks.append(
            {
                "name": check["name"],
                "type": check["type"],
                "severity": check.get("severity", "error"),
                "passed": bool(passed),
                "measured": _serialize(measured),
                "detail": detail,
            }
        )
    return {"table": table, "checks": checks}


def _serialize(value) -> str | None:
    if value is None:
        return None
    return str(value)


def render_report(runs: list[dict], output: Path, status: str) -> None:
    total = sum(len(r["checks"]) for r in runs)
    passed = sum(1 for r in runs for c in r["checks"] if c["passed"])

    def row_html(run):
        table_rows = []
        for c in run["checks"]:
            if c["passed"]:
                badge = "pass"
            elif c["severity"] == "warn":
                badge = "warn"
            else:
                badge = "fail"
            table_rows.append(
                f"<tr class='{badge}'>"
                f"<td><code>{c['name']}</code></td>"
                f"<td>{c['type']} · {c['severity']}</td>"
                f"<td>{c.get('measured', '-')}</td>"
                f"<td>{c['detail']}</td>"
                f"<td><span class='badge {badge}'>{'PASS' if c['passed'] else badge.upper()}</span></td>"
                f"</tr>"
            )
        return (
            f"<section><h2>{run['table']}</h2>"
            f"<table><thead><tr><th>check</th><th>type</th><th>measured</th>"
            f"<th>detail</th><th>status</th></tr></thead>"
            f"<tbody>{''.join(table_rows)}</tbody></table></section>"
        )

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Lakehouse DQ report</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 960px;
         padding: 0 1rem; color: #1f2937; }}
  h1 {{ font-size: 1.5rem; }} h2 {{ font-size: 1.1rem; margin-top: 2rem; }}
  .status {{ font-weight: 700; }}
  .PASS {{ color: #15803d; }} .FAIL {{ color: #b91c1c; }} .WARN {{ color: #b45309; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
  th, td {{ border: 1px solid #e5e7eb; padding: 6px 10px; text-align: left; }}
  tr.fail {{ background: #fef2f2; }} tr.pass {{ background: #ffffff; }} tr.warn {{ background: #fffbeb; }}
  .badge {{ padding: 2px 8px; border-radius: 999px; font-size: 0.7rem; font-weight: 700; }}
  .badge.pass {{ background: #dcfce7; color: #15803d; }}
  .badge.fail {{ background: #fee2e2; color: #b91c1c; }}
  .badge.warn {{ background: #fef3c7; color: #b45309; }}
  .meta {{ color: #6b7280; font-size: 0.8rem; }}
</style></head><body>
<h1>Lakehouse Data Quality Report</h1>
<p class="meta">run: {datetime.now().isoformat(timespec='seconds')}</p>
<p>status: <span class="status {status}">{status}</span> · {passed}/{total} checks passed</p>
{''.join(row_html(r) for r in runs)}
</body></html>"""
    output.write_text(html)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=Path("dq/specs.json"))
    parser.add_argument("--json", type=Path, default=Path("dq/latest.json"))
    parser.add_argument("--report", type=Path, default=Path("dq/report.html"))
    args = parser.parse_args()

    specs = json.loads(args.spec.read_text())
    spark = build_spark("dq")
    try:
        runs = [run_checks(spark, spec) for spec in specs["tables"]]
    finally:
        spark.stop()

    errors = [c for r in runs for c in r["checks"] if not c["passed"] and c["severity"] == "error"]
    warns = [c for r in runs for c in r["checks"] if not c["passed"] and c["severity"] == "warn"]
    overall = "FAIL" if errors else ("WARN" if warns else "PASS")
    json_out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall,
        "tables": runs,
    }
    args.json.write_text(json.dumps(json_out, indent=2))
    render_report(runs, args.report, overall)

    n_failed = sum(
        1
        for r in runs
        for c in r["checks"]
        if not c["passed"] and c["severity"] == "error"
    )
    n_warn = sum(
        1
        for r in runs
        for c in r["checks"]
        if not c["passed"] and c["severity"] == "warn"
    )
    print(
        f"DQ: {json_out['overall_status']} — {n_failed} error(s), "
        f"{n_warn} warning(s), report -> {args.report}"
    )
    sys.exit(1 if n_failed else 0)


if __name__ == "__main__":
    main()
