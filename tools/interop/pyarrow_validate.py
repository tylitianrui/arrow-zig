#!/usr/bin/env python3
"""Validate canonical IPC stream fixture semantics via PyArrow."""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def decode_dictionary_values(column: pa.Array) -> list[str]:
    if not pa.types.is_dictionary(column.type):
        raise RuntimeError("column must be dictionary type")
    dictionary = column.dictionary.to_pylist()
    indices = column.indices.to_pylist()
    out: list[str] = []
    for index in indices:
        if index is None:
            raise RuntimeError("dictionary index is null")
        if index < 0 or index >= len(dictionary):
            raise RuntimeError("dictionary index out of range")
        out.append(dictionary[index])
    return out


def validate_canonical(in_path: pathlib.Path) -> None:
    # Expected decoded content:
    # - one batch with 3 rows
    # - id=[1, 2, 3]
    # - name=["alice", null, "bob"]
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
    # Expected decoded content:
    # - two batches
    #   batch1 values=["red", "blue"]
    #   batch2 values=["green"]
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
    if decode_dictionary_values(first.column(0)) != ["red", "blue"]:
        raise RuntimeError("invalid first batch values")
    if decode_dictionary_values(second.column(0)) != ["green"]:
        raise RuntimeError("invalid second batch values")


def validate_ree(in_path: pathlib.Path) -> None:
    # Expected decoded content:
    # - one batch with 5 rows
    # - logical values=[100, 100, 200, 200, 200]
    with ipc.open_stream(in_path) as reader:
        schema = reader.schema
        if len(schema) != 1:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "ree":
            raise RuntimeError("invalid ree field name")
        ree_type = schema[0].type
        if not pa.types.is_run_end_encoded(ree_type):
            raise RuntimeError("ree field must be run_end_encoded type")
        if ree_type.run_end_type != pa.int32():
            raise RuntimeError("ree run_end_type must be int32")
        if ree_type.value_type != pa.int32():
            raise RuntimeError("ree value_type must be int32")

        batches = list(reader)
    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")

    batch = batches[0]
    if batch.num_rows != 5:
        raise RuntimeError("invalid row count")

    values = batch.column(0).to_pylist()
    if values != [100, 100, 200, 200, 200]:
        raise RuntimeError(f"invalid ree values: {values!r}")


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print("usage: pyarrow_validate.py <in.arrow> [canonical|dict-delta|ree]", file=sys.stderr)
        return 2

    in_path = pathlib.Path(sys.argv[1])
    mode = sys.argv[2] if len(sys.argv) == 3 else "canonical"
    if mode == "canonical":
        validate_canonical(in_path)
        return 0
    if mode == "dict-delta":
        validate_dict_delta(in_path)
        return 0
    if mode == "ree":
        validate_ree(in_path)
        return 0
    print(f"unknown mode: {mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
