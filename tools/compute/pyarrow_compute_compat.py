#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


EXPECTED_PYARROW_VERSION = "23.0.1"


def array(values: list[int | None]) -> dict[str, Any]:
    return {"kind": "array", "values": values}


def chunked(values: list[int | None], chunks: list[int]) -> dict[str, Any]:
    return {"kind": "chunked", "values": values, "chunks": chunks}


def scalar(value: int | None) -> dict[str, Any]:
    if value is None:
        return {"kind": "scalar", "scalar_is_null": True}
    return {"kind": "scalar", "scalar": value}


CASES: list[dict[str, Any]] = [
    {
        "name": "numeric_add_array",
        "suite": "numeric",
        "operation": "add_i64",
        "lhs": array([1, 2, 3]),
        "rhs": array([4, 5, 6]),
    },
    {
        "name": "numeric_divide_array",
        "suite": "numeric",
        "operation": "divide_i64",
        "lhs": array([8, 9, 10]),
        "rhs": array([2, 3, 5]),
    },
    {
        "name": "nulls_add_array",
        "suite": "nulls",
        "operation": "add_i64",
        "lhs": array([1, None, 3]),
        "rhs": array([10, 20, None]),
    },
    {
        "name": "nulls_divide_array",
        "suite": "nulls",
        "operation": "divide_i64",
        "lhs": array([8, None, 10]),
        "rhs": array([2, 3, 5]),
    },
    {
        "name": "boundary_divide_by_zero_error",
        "suite": "boundary",
        "operation": "divide_i64",
        "lhs": array([1, 2]),
        "rhs": array([1, 0]),
        "expected_error": "DivideByZero",
    },
    {
        "name": "boundary_cast_i64_to_i32_ok",
        "suite": "boundary",
        "operation": "cast_i64_to_i32",
        "lhs": array([2147483647, -2147483648, 0]),
        "cast_options": {"safe": True},
    },
    {
        "name": "boundary_cast_i64_to_i32_invalid",
        "suite": "boundary",
        "operation": "cast_i64_to_i32",
        "lhs": array([2147483648]),
        "cast_options": {"safe": True},
        "expected_error": "InvalidCast",
    },
    {
        "name": "chunked_add_misaligned",
        "suite": "chunked",
        "operation": "add_i64",
        "lhs": chunked([1, None, 3, 4, 5], [2, 3]),
        "rhs": chunked([10, 20, None, 40, 50], [1, 4]),
    },
    {
        "name": "chunked_add_scalar_broadcast",
        "suite": "chunked",
        "operation": "add_i64",
        "lhs": scalar(5),
        "rhs": chunked([1, None, 3, 4], [1, 3]),
    },
    {
        "name": "chunked_divide_scalar_broadcast",
        "suite": "chunked",
        "operation": "divide_i64",
        "lhs": chunked([20, 30, 40, 50], [3, 1]),
        "rhs": scalar(10),
    },
    {
        "name": "chunked_cast_i64_to_i32_ok",
        "suite": "chunked",
        "operation": "cast_i64_to_i32",
        "lhs": chunked([1, 2, 2147483647, -2147483648], [3, 1]),
        "cast_options": {"safe": True},
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute compatibility check against pyarrow.compute")
    parser.add_argument(
        "--suite",
        choices=["all", "numeric", "nulls", "boundary", "chunked"],
        default="all",
        help="subset of compatibility cases to run",
    )
    return parser.parse_args()


def build_pyarrow_datum(spec: dict[str, Any], pa: Any) -> Any:
    kind = spec["kind"]
    if kind == "array":
        return pa.array(spec["values"], type=pa.int64())
    if kind == "chunked":
        values = spec["values"]
        chunk_sizes = spec["chunks"]
        if sum(chunk_sizes) != len(values):
            raise ValueError("chunk sizes must sum to values length")
        chunks = []
        cursor = 0
        for size in chunk_sizes:
            chunks.append(pa.array(values[cursor : cursor + size], type=pa.int64()))
            cursor += size
        return pa.chunked_array(chunks, type=pa.int64())
    if kind == "scalar":
        if spec.get("scalar_is_null", False):
            return pa.scalar(None, type=pa.int64())
        return pa.scalar(spec["scalar"], type=pa.int64())
    raise ValueError(f"unsupported datum kind: {kind}")


def pyarrow_values(result: Any, pa: Any) -> list[int | None]:
    if isinstance(result, pa.ChunkedArray):
        return result.to_pylist()
    if isinstance(result, pa.Array):
        return result.to_pylist()
    if isinstance(result, pa.Scalar):
        return [result.as_py()]
    raise TypeError(f"unsupported pyarrow result type: {type(result)!r}")


def run_pyarrow_case(case: dict[str, Any], pa: Any, pc: Any) -> dict[str, Any]:
    lhs = build_pyarrow_datum(case["lhs"], pa)
    rhs_spec = case.get("rhs")
    rhs = build_pyarrow_datum(rhs_spec, pa) if rhs_spec is not None else None
    op = case["operation"]

    try:
        if op == "add_i64":
            result = pc.add(lhs, rhs)
        elif op == "divide_i64":
            divide_fn = getattr(pc, "divide_checked", pc.divide)
            result = divide_fn(lhs, rhs)
        elif op == "cast_i64_to_i32":
            safe = case.get("cast_options", {}).get("safe", True)
            result = pc.cast(lhs, pa.int32(), safe=safe)
        else:
            raise ValueError(f"unsupported operation: {op}")
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        return {"status": "error", "error": str(exc)}

    return {"status": "ok", "values": pyarrow_values(result, pa)}


def runner_payload(cases: list[dict[str, Any]]) -> dict[str, Any]:
    allowed = {"name", "operation", "lhs", "rhs", "arithmetic_options", "cast_options"}
    return {
        "cases": [{k: v for k, v in case.items() if k in allowed} for case in cases],
    }


def run_zig_runner(cases: list[dict[str, Any]]) -> dict[str, Any]:
    payload = runner_payload(cases)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        json.dump(payload, tmp)
        tmp_path = Path(tmp.name)

    try:
        completed = subprocess.run(
            ["zig", "build", "compute-compat-check", "--", str(tmp_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        tmp_path.unlink(missing_ok=True)

    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(
            "compute-compat-runner produced no JSON output.\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return json.loads(lines[-1])


def assert_alignment(cases: list[dict[str, Any]], runner_result: dict[str, Any], pa: Any, pc: Any) -> None:
    results_by_name = {item["name"]: item for item in runner_result["results"]}
    for case in cases:
        name = case["name"]
        if name not in results_by_name:
            raise AssertionError(f"runner result missing case: {name}")
        runner_case = results_by_name[name]
        py_case = run_pyarrow_case(case, pa, pc)
        expected_error = case.get("expected_error")

        if expected_error is not None:
            if py_case["status"] != "error":
                raise AssertionError(
                    f"{name}: expected pyarrow error for {expected_error}, got values={py_case.get('values')}"
                )
            if runner_case["status"] != "error":
                raise AssertionError(
                    f"{name}: expected runner error {expected_error}, got status={runner_case['status']} values={runner_case.get('values')}"
                )
            if runner_case.get("error") != expected_error:
                raise AssertionError(
                    f"{name}: expected runner error {expected_error}, got {runner_case.get('error')}"
                )
            continue

        if py_case["status"] != "ok":
            raise AssertionError(f"{name}: unexpected pyarrow error: {py_case.get('error')}")
        if runner_case["status"] != "ok":
            raise AssertionError(f"{name}: unexpected runner error: {runner_case.get('error')}")
        if runner_case.get("values") != py_case.get("values"):
            raise AssertionError(
                f"{name}: values mismatch\n"
                f"  pyarrow={py_case.get('values')}\n"
                f"  zarrow ={runner_case.get('values')}"
            )


def main() -> None:
    args = parse_args()
    import pyarrow as pa  # delayed import for clearer CLI failures
    import pyarrow.compute as pc

    if pa.__version__ != EXPECTED_PYARROW_VERSION:
        raise SystemExit(f"PyArrow version must be {EXPECTED_PYARROW_VERSION}; got {pa.__version__}")

    selected = CASES if args.suite == "all" else [case for case in CASES if case["suite"] == args.suite]
    if not selected:
        raise SystemExit(f"no cases selected for suite={args.suite!r}")

    runner_result = run_zig_runner(selected)
    assert_alignment(selected, runner_result, pa, pc)
    print(f"compute compatibility passed: suite={args.suite} cases={len(selected)}")


if __name__ == "__main__":
    main()
