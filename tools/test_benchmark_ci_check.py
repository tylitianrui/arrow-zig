#!/usr/bin/env python3
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("benchmark_ci_check.py")


def run_check(csv_text: str):
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "bench.csv"
        p.write_text(csv_text, encoding="utf-8")
        return subprocess.run(["python3", str(SCRIPT), str(p)], capture_output=True, text=True)


class BenchmarkCsvCheckTests(unittest.TestCase):
    def test_valid_csv_passes(self):
        csv_text = "\n".join([
            "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum,git_sha,timestamp",
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,1700000000",
            "record_batch_builder,1000,10,1000000,1000.0,1000.0,43,abc123,1700000000",
            "struct_builder,1000,10,1000000,1000.0,1000.0,44,abc123,1700000000",
        ])
        r = run_check(csv_text)
        self.assertEqual(0, r.returncode, r.stderr)

    def test_invalid_header_fails(self):
        csv_text = "\n".join([
            "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum",
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42",
        ])
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)

    def test_missing_benchmark_fails(self):
        csv_text = "\n".join([
            "benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum,git_sha,timestamp",
            "primitive_builder,1000,10,1000000,1000.0,1000.0,42,abc123,1700000000",
        ])
        r = run_check(csv_text)
        self.assertNotEqual(0, r.returncode)


if __name__ == "__main__":
    unittest.main()
