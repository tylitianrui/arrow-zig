#!/usr/bin/env python3
"""Generate IPC fixtures via PyArrow."""

from __future__ import annotations

import pathlib
import sys
from decimal import Decimal

import pyarrow as pa
import pyarrow.ipc as ipc


def _new_writer(out_path: pathlib.Path, schema: pa.Schema, container: str):
    if container == "stream":
        return ipc.new_stream(out_path, schema)
    if container == "file":
        return ipc.new_file(out_path, schema)
    raise ValueError(f"unknown container mode: {container}")


def generate_canonical(out_path: pathlib.Path, container: str) -> None:
    # Writes one stream with:
    # - schema: id: int32 (non-null), name: utf8 (nullable)
    # - one record batch (3 rows)
    #   id=[1, 2, 3]
    #   name=["alice", null, "bob"]
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
    with _new_writer(out_path, schema, container) as writer:
        writer.write_batch(batch)


def generate_dict_delta(out_path: pathlib.Path, container: str) -> None:
    # Writes one stream with dictionary-encoded column "color":
    # - schema: color: dictionary<int32, utf8>
    # - two record batches
    #   batch1 decoded values=["red", "blue"]
    #   batch2 decoded values=["green"]
    # Note: PyArrow may materialize the second dictionary as a superset
    # (for example ["red", "blue", "green"] with index 2).
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

    with _new_writer(out_path, schema, container) as writer:
        writer.write_batch(batch_1)
        writer.write_batch(batch_2)


def generate_ree(out_path: pathlib.Path, container: str) -> None:
    # Writes one stream with:
    # - schema: ree: run_end_encoded<int32, int32>
    # - one record batch (5 rows)
    #   run_ends=[2, 5], values=[100, 200]
    #   decoded logical values=[100, 100, 200, 200, 200]
    run_ends = pa.array([2, 5], type=pa.int32())
    values = pa.array([100, 200], type=pa.int32())
    col = pa.RunEndEncodedArray.from_arrays(run_ends, values)
    schema = pa.schema([pa.field("ree", col.type, nullable=True)])
    batch = pa.record_batch([col], schema=schema)
    with _new_writer(out_path, schema, container) as writer:
        writer.write_batch(batch)


def generate_complex(out_path: pathlib.Path, container: str) -> None:
    list_type = pa.list_(pa.field("item", pa.int32(), nullable=True))
    struct_type = pa.struct(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    map_type = pa.map_(pa.int32(), pa.int32())
    union_codes = [5, 7]
    dense_union_type = pa.union(
        [pa.field("i", pa.int32()), pa.field("b", pa.bool_())],
        mode="dense",
        type_codes=union_codes,
    )
    dec_type = pa.decimal128(10, 2)
    ts_type = pa.timestamp("ms", tz="UTC")

    schema = pa.schema(
        [
            pa.field("list_i32", list_type, nullable=True),
            pa.field("struct_pair", struct_type, nullable=True),
            pa.field("map_i32_i32", map_type, nullable=True),
            pa.field("u_dense", dense_union_type, nullable=True),
            pa.field("dec", dec_type, nullable=False),
            pa.field("ts", ts_type, nullable=False),
        ]
    )

    list_col = pa.array([[1, 2], None, [3]], type=list_type)
    struct_col = pa.array(
        [
            {"id": 10, "name": "aa"},
            None,
            {"id": 30, "name": "cc"},
        ],
        type=struct_type,
    )
    map_col = pa.array([{1: 10, 2: 20}, None, {3: 30}], type=map_type)

    union_type_ids = pa.array([5, 7, 5], type=pa.int8())
    union_offsets = pa.array([0, 0, 1], type=pa.int32())
    union_i = pa.array([100, 200], type=pa.int32())
    union_b = pa.array([True], type=pa.bool_())
    union_col = pa.UnionArray.from_dense(
        union_type_ids,
        union_offsets,
        [union_i, union_b],
        field_names=["i", "b"],
        type_codes=union_codes,
    )

    dec_col = pa.array([Decimal("123.45"), Decimal("-0.42"), Decimal("0.00")], type=dec_type)
    ts_col = pa.array([1700000000000, 1700000001000, 1700000002000], type=ts_type)

    batch = pa.record_batch([list_col, struct_col, map_col, union_col, dec_col, ts_col], schema=schema)
    with _new_writer(out_path, schema, container) as writer:
        writer.write_batch(batch)


def generate_extension(out_path: pathlib.Path, container: str) -> None:
    schema = pa.schema(
        [
            pa.field(
                "ext_i32",
                pa.int32(),
                nullable=True,
                metadata={
                    b"ARROW:extension:name": b"com.example.int32_ext",
                    b"ARROW:extension:metadata": b"v1",
                    b"owner": b"interop",
                },
            )
        ]
    )
    batch = pa.record_batch([pa.array([7, None, 11], type=pa.int32())], schema=schema)
    with _new_writer(out_path, schema, container) as writer:
        writer.write_batch(batch)


def generate_view(out_path: pathlib.Path, container: str) -> None:
    schema = pa.schema(
        [
            pa.field("sv", pa.string_view(), nullable=True),
            pa.field("bv", pa.binary_view(), nullable=True),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array(["short", None, "tiny", "this string is longer than twelve"], type=pa.string_view()),
            pa.array([b"ab", b"this-binary-view-is-long", None, b"xy"], type=pa.binary_view()),
        ],
        schema=schema,
    )
    with _new_writer(out_path, schema, container) as writer:
        writer.write_batch(batch)


def main() -> int:
    if len(sys.argv) not in (2, 3, 4):
        print(
            "usage: pyarrow_generate.py <out.arrow> [canonical|dict-delta|ree|complex|extension|view] [stream|file]",
            file=sys.stderr,
        )
        return 2

    out_path = pathlib.Path(sys.argv[1])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = sys.argv[2] if len(sys.argv) >= 3 else "canonical"
    container = sys.argv[3] if len(sys.argv) == 4 else "stream"
    if container not in ("stream", "file"):
        print(f"unknown container mode: {container}", file=sys.stderr)
        return 2
    if mode == "dict-delta" and container == "file":
        print(
            "dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches",
            file=sys.stderr,
        )
        return 2

    if mode == "canonical":
        generate_canonical(out_path, container)
        return 0
    if mode == "dict-delta":
        generate_dict_delta(out_path, container)
        return 0
    if mode == "ree":
        generate_ree(out_path, container)
        return 0
    if mode == "complex":
        generate_complex(out_path, container)
        return 0
    if mode == "extension":
        generate_extension(out_path, container)
        return 0
    if mode == "view":
        generate_view(out_path, container)
        return 0
    print(f"unknown mode: {mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
