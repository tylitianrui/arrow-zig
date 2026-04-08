#!/usr/bin/env python3
"""Generate canonical IPC stream fixture via PyArrow."""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def generate_canonical(out_path: pathlib.Path) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["alice", None, "bob"], type=pa.string()),
        ],
        schema=schema,
    )
    with ipc.new_stream(out_path, schema) as writer:
        writer.write_batch(batch)


def generate_dict_delta(out_path: pathlib.Path) -> None:
    dtype = pa.dictionary(pa.int32(), pa.string())
    schema = pa.schema([pa.field("color", dtype, nullable=False)])

    dict_1 = pa.array(["red", "blue"], type=pa.string())
    idx_1 = pa.array([0, 1], type=pa.int32())
    col_1 = pa.DictionaryArray.from_arrays(idx_1, dict_1)
    batch_1 = pa.record_batch([col_1], schema=schema)

    dict_2 = pa.array(["red", "blue", "green"], type=pa.string())
    idx_2 = pa.array([2], type=pa.int32())
    col_2 = pa.DictionaryArray.from_arrays(idx_2, dict_2)
    batch_2 = pa.record_batch([col_2], schema=schema)

    with ipc.new_stream(out_path, schema) as writer:
        writer.write_batch(batch_1)
        writer.write_batch(batch_2)


def generate_ree(out_path: pathlib.Path) -> None:
    run_ends = pa.array([2, 5], type=pa.int32())
    values = pa.array([100, 200], type=pa.int32())
    col = pa.RunEndEncodedArray.from_arrays(run_ends, values)
    schema = pa.schema([pa.field("ree", col.type, nullable=True)])
    batch = pa.record_batch([col], schema=schema)
    with ipc.new_stream(out_path, schema) as writer:
        writer.write_batch(batch)


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print("usage: pyarrow_generate.py <out.arrow> [canonical|dict-delta|ree]", file=sys.stderr)
        return 2

    out_path = pathlib.Path(sys.argv[1])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = sys.argv[2] if len(sys.argv) == 3 else "canonical"

    if mode == "canonical":
        generate_canonical(out_path)
        return 0
    if mode == "dict-delta":
        generate_dict_delta(out_path)
        return 0
    if mode == "ree":
        generate_ree(out_path)
        return 0
    print(f"unknown mode: {mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
