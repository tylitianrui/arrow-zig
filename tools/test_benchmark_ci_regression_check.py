#!/usr/bin/env python3
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("benchmark_ci_regression_check.py")

VALID_HEADER = "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum,git_sha,timestamp"
VALID_ROWS = [
    "primitive_builder,1000,10,1000000,20000.0,50000.0,42,abc123,1700000000",
    "record_batch_builder,1000,10,1000000,12000.0,80000.0,43,abc123,1700000000",
    "struct_builder,1000,10,1000000,11000.0,90000.0,44,abc123,1700000000",
]
THRESHOLDS = {
    "primitive_builder": {"min_rows_per_sec": 10000.0, "max_ns_per_row": 1000000.0},
    "record_batch_builder": {"min_rows_per_sec": 5000.0, "max_ns_per_row": 2000000.0},
    "struct_builder": {"min_rows_per_sec": 5000.0, "max_ns_per_row": 2000000.0},
}


def run_check(csv_text: str, thresholds: dict):
    with tempfile.TemporaryDirectory() as td:
        csv_path = Path(td) / "bench.csv"
        thresholds_path = Path(td) / "thresholds.json"
        csv_path.write_text(csv_text, encoding="utf-8")
        thresholds_path.write_text(json.dumps(thresholds), encoding="utf-8")
        return subprocess.run(
            ["python3", str(SCRIPT), str(csv_path), str(thresholds_path)],
            capture_output=True,
            text=True,
        )


def make_csv(*rows: str) -> str:
    return "\n".join(rows)


class BenchmarkRegressionCheckTests(unittest.TestCase):
    def test_valid_csv_passes(self):
        r = run_check(make_csv(VALID_HEADER, *VALID_ROWS), THRESHOLDS)
        self.assertEqual(0, r.returncode, r.stderr)

    def test_rows_per_sec_below_threshold_fails(self):
        bad_rows = list(VALID_ROWS)
        bad_rows[0] = "primitive_builder,1000,10,1000000,100.0,50000.0,42,abc123,1700000000"
        r = run_check(make_csv(VALID_HEADER, *bad_rows), THRESHOLDS)
        self.assertNotEqual(0, r.returncode)
        self.assertIn("below threshold", r.stderr)

    def test_ns_per_row_above_threshold_fails(self):
        bad_rows = list(VALID_ROWS)
        bad_rows[2] = "struct_builder,1000,10,1000000,11000.0,90000000.0,44,abc123,1700000000"
        r = run_check(make_csv(VALID_HEADER, *bad_rows), THRESHOLDS)
        self.assertNotEqual(0, r.returncode)
        self.assertIn("above threshold", r.stderr)

    def test_missing_threshold_entry_fails(self):
        bad_thresholds = dict(THRESHOLDS)
        bad_thresholds.pop("struct_builder")
        r = run_check(make_csv(VALID_HEADER, *VALID_ROWS), bad_thresholds)
        self.assertNotEqual(0, r.returncode)
        self.assertIn("unexpected benchmark", r.stderr)


if __name__ == "__main__":
    unittest.main()
