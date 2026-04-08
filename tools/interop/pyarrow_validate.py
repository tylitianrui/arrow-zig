#!/usr/bin/env python3
"""Validate canonical IPC stream fixture semantics via PyArrow."""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def validate_canonical(in_path: pathlib.Path) -> None:
    with ipc.open_stream(in_path) as reader:
        schema = reader.schema
        if len(schema) != 2:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "id" or schema[0].type != pa.int32():
            raise RuntimeError("invalid id field")
        if schema[1].name != "name" or schema[1].type != pa.string():
            raise RuntimeError("invalid name field")

        batches = list(reader)
    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")
    batch = batches[0]
    if batch.num_rows != 3:
        raise RuntimeError("invalid row count")

    ids = batch.column(0).to_pylist()
    names = batch.column(1).to_pylist()
    if ids != [1, 2, 3]:
        raise RuntimeError(f"invalid ids: {ids!r}")
    if names != ["alice", None, "bob"]:
        raise RuntimeError(f"invalid names: {names!r}")


def validate_dict_delta(in_path: pathlib.Path) -> None:
    with ipc.open_stream(in_path) as reader:
        schema = reader.schema
        if len(schema) != 1:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "color":
            raise RuntimeError("invalid color field")
        if not pa.types.is_dictionary(schema[0].type):
            raise RuntimeError("color field must be dictionary type")

        batches = list(reader)
    if len(batches) != 2:
        raise RuntimeError("expected exactly two batches")

    first = batches[0]
    second = batches[1]
    if first.num_rows != 2 or second.num_rows != 1:
        raise RuntimeError("invalid row counts")
    if first.column(0).to_pylist() != ["red", "blue"]:
        raise RuntimeError("invalid first batch values")
    if second.column(0).to_pylist() != ["green"]:
        raise RuntimeError("invalid second batch values")

    first_dict = first.column(0).dictionary.to_pylist()
    second_dict = second.column(0).dictionary.to_pylist()
    if first_dict != ["red", "blue"]:
        raise RuntimeError(f"invalid first dictionary: {first_dict!r}")
    if second_dict != ["red", "blue", "green"]:
        raise RuntimeError(f"invalid second dictionary: {second_dict!r}")


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print("usage: pyarrow_validate.py <in.arrow> [canonical|dict-delta]", file=sys.stderr)
        return 2

    in_path = pathlib.Path(sys.argv[1])
    mode = sys.argv[2] if len(sys.argv) == 3 else "canonical"
    if mode == "canonical":
        validate_canonical(in_path)
        return 0
    if mode == "dict-delta":
        validate_dict_delta(in_path)
        return 0
    print(f"unknown mode: {mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
