#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

EXPECTED_HEADER = [
    "benchmark",
    "rows",
    "iterations",
    "elapsed_ns",
    "rows_per_sec",
    "ns_per_row",
    "checksum",
    "git_sha",
    "timestamp",
]

EXPECTED_BENCHES = {
    "primitive_builder",
    "record_batch_builder",
    "struct_builder",
}


def fail(msg: str) -> None:
    print(f"benchmark_ci_check: {msg}", file=sys.stderr)
    sys.exit(1)


def load_rows(path: Path):
    text = path.read_text(encoding="utf-8", errors="strict")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        fail("empty CSV output")
    return lines


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: benchmark_ci_check.py <csv-path>")

    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        fail(f"file not found: {csv_path}")

    lines = load_rows(csv_path)
    reader = csv.reader(lines)
    rows = list(reader)
    if not rows:
        fail("CSV parse produced no rows")

    if rows[0] != EXPECTED_HEADER:
        fail(f"invalid header: {rows[0]} expected: {EXPECTED_HEADER}")

    seen = set()
    for i, row in enumerate(rows[1:], start=2):
        if len(row) != len(EXPECTED_HEADER):
            fail(f"line {i}: expected {len(EXPECTED_HEADER)} columns, got {len(row)}")

        bench, rows_s, iters_s, elapsed_s, rps_s, npr_s, checksum_s, git_sha, ts_s = row
        if bench not in EXPECTED_BENCHES:
            fail(f"line {i}: unexpected benchmark '{bench}'")
        seen.add(bench)

        try:
            if int(rows_s) <= 0:
                fail(f"line {i}: rows must be > 0")
            if int(iters_s) <= 0:
                fail(f"line {i}: iterations must be > 0")
            if int(elapsed_s) <= 0:
                fail(f"line {i}: elapsed_ns must be > 0")
            float(rps_s)
            float(npr_s)
            int(checksum_s)
            if int(ts_s) <= 0:
                fail(f"line {i}: timestamp must be > 0")
        except ValueError as exc:
            fail(f"line {i}: numeric parse failed: {exc}")

        if not git_sha:
            fail(f"line {i}: git_sha must be non-empty")

    missing = EXPECTED_BENCHES - seen
    if missing:
        fail(f"missing benchmark rows for: {sorted(missing)}")

    print("benchmark_ci_check: OK")


if __name__ == "__main__":
    main()
