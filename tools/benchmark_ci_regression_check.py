#!/usr/bin/env python3
import csv
import json
import sys
from pathlib import Path


def fail(msg: str) -> None:
    print(f"benchmark_ci_regression_check: {msg}", file=sys.stderr)
    sys.exit(1)


def load_csv_rows(path: Path):
    text = path.read_text(encoding="utf-8", errors="strict")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        fail("empty CSV output")
    return list(csv.DictReader(lines))


def load_thresholds(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not data:
        fail("threshold JSON must be a non-empty object")

    normalized = {}
    for bench, cfg in data.items():
        if not isinstance(cfg, dict):
            fail(f"threshold for '{bench}' must be an object")
        if "min_rows_per_sec" not in cfg or "max_ns_per_row" not in cfg:
            fail(f"threshold for '{bench}' must include min_rows_per_sec and max_ns_per_row")
        try:
            min_rps = float(cfg["min_rows_per_sec"])
            max_npr = float(cfg["max_ns_per_row"])
        except (TypeError, ValueError) as exc:
            fail(f"threshold for '{bench}' has invalid numeric values: {exc}")
        if min_rps <= 0 or max_npr <= 0:
            fail(f"threshold for '{bench}' must be > 0")
        normalized[bench] = {
            "min_rows_per_sec": min_rps,
            "max_ns_per_row": max_npr,
        }
    return normalized


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: benchmark_ci_regression_check.py <csv-path> <threshold-json>")

    csv_path = Path(sys.argv[1])
    thresholds_path = Path(sys.argv[2])
    if not csv_path.exists():
        fail(f"file not found: {csv_path}")
    if not thresholds_path.exists():
        fail(f"file not found: {thresholds_path}")

    rows = load_csv_rows(csv_path)
    thresholds = load_thresholds(thresholds_path)

    seen = set()
    for row in rows:
        bench = row.get("benchmark", "")
        if bench not in thresholds:
            fail(f"unexpected benchmark in CSV: {bench}")
        seen.add(bench)

        try:
            rows_per_sec = float(row["rows_per_sec"])
            ns_per_row = float(row["ns_per_row"])
        except (KeyError, ValueError) as exc:
            fail(f"benchmark '{bench}' has invalid numeric fields: {exc}")

        bench_threshold = thresholds[bench]
        if rows_per_sec < bench_threshold["min_rows_per_sec"]:
            fail(
                f"{bench}: rows_per_sec={rows_per_sec:.3f} below threshold {bench_threshold['min_rows_per_sec']:.3f}"
            )
        if ns_per_row > bench_threshold["max_ns_per_row"]:
            fail(
                f"{bench}: ns_per_row={ns_per_row:.3f} above threshold {bench_threshold['max_ns_per_row']:.3f}"
            )

    missing = set(thresholds.keys()) - seen
    if missing:
        fail(f"missing benchmark rows in CSV: {sorted(missing)}")

    print("benchmark_ci_regression_check: OK")


if __name__ == "__main__":
    main()
