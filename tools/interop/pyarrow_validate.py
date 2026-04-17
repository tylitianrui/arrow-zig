#!/usr/bin/env python3
"""Validate IPC fixture semantics via PyArrow."""

from __future__ import annotations

import pathlib
import sys
from decimal import Decimal

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


def _open_reader(in_path: pathlib.Path, container: str):
    if container == "stream":
        return ipc.open_stream(in_path)
    if container == "file":
        return ipc.open_file(in_path)
    raise ValueError(f"unknown container mode: {container}")


def _read_all_batches(reader) -> list[pa.RecordBatch]:
    # Stream readers are iterable, file readers expose random-access batch APIs.
    if hasattr(reader, "num_record_batches"):
        return [reader.get_batch(i) for i in range(reader.num_record_batches)]
    return list(reader)


def _extract_stream_payload(in_path: pathlib.Path, container: str) -> bytes:
    raw = in_path.read_bytes()
    if container == "stream":
        return raw
    if container != "file":
        raise ValueError(f"unknown container mode: {container}")

    # Arrow file layout:
    # [magic(6) + pad(2)] [stream payload] [footer flatbuffer] [footer_len u32] [magic(6)]
    if len(raw) < 8 + 4 + 6:
        raise RuntimeError("file too small")
    if raw[0:6] != b"ARROW1" or raw[6:8] != b"\x00\x00":
        raise RuntimeError("invalid file header magic")
    if raw[-6:] != b"ARROW1":
        raise RuntimeError("invalid file footer magic")

    footer_len = int.from_bytes(raw[-10:-6], "little", signed=False)
    footer_start = len(raw) - 10 - footer_len
    if footer_start < 8:
        raise RuntimeError("invalid file footer offset")
    return raw[8:footer_start]


def _read_all_messages_from_payload(payload: bytes) -> list[ipc.Message]:
    reader = ipc.MessageReader.open_stream(pa.BufferReader(payload))
    out: list[ipc.Message] = []
    while True:
        try:
            out.append(reader.read_next_message())
        except StopIteration:
            break
    return out


def validate_canonical(in_path: pathlib.Path, container: str) -> None:
    # Expected decoded content:
    # - one batch with 3 rows
    # - id=[1, 2, 3]
    # - name=["alice", null, "bob"]
    with _open_reader(in_path, container) as reader:
        schema = reader.schema
        if len(schema) != 2:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "id" or schema[0].type != pa.int32():
            raise RuntimeError("invalid id field")
        if schema[1].name != "name" or schema[1].type != pa.string():
            raise RuntimeError("invalid name field")

        batches = _read_all_batches(reader)
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


def validate_dict_delta(in_path: pathlib.Path, container: str) -> None:
    # Expected decoded content:
    # - two batches
    #   batch1 values=["red", "blue"]
    #   batch2 values=["green"]
    with _open_reader(in_path, container) as reader:
        schema = reader.schema
        if len(schema) != 1:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "color":
            raise RuntimeError("invalid color field")
        if not pa.types.is_dictionary(schema[0].type):
            raise RuntimeError("color field must be dictionary type")

        batches = _read_all_batches(reader)
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


def validate_ree(in_path: pathlib.Path, container: str) -> None:
    # Expected decoded content:
    # - one batch with 5 rows
    # - logical values=[100, 100, 200, 200, 200]
    with _open_reader(in_path, container) as reader:
        schema = reader.schema
        if len(schema) != 1:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "ree":
            raise RuntimeError("invalid ree field name")
        ree_type = schema[0].type
        if not pa.types.is_run_end_encoded(ree_type):
            raise RuntimeError("ree field must be run_end_encoded type")
        if ree_type.run_end_type not in (pa.int16(), pa.int32(), pa.int64()):
            raise RuntimeError("ree run_end_type must be int16/int32/int64")
        if ree_type.value_type != pa.int32():
            raise RuntimeError("ree value_type must be int32")

        batches = _read_all_batches(reader)
    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")

    batch = batches[0]
    if batch.num_rows != 5:
        raise RuntimeError("invalid row count")

    values = batch.column(0).to_pylist()
    if values != [100, 100, 200, 200, 200]:
        raise RuntimeError(f"invalid ree values: {values!r}")


def validate_complex(in_path: pathlib.Path, container: str) -> None:
    with _open_reader(in_path, container) as reader:
        schema = reader.schema
        if len(schema) != 6:
            raise RuntimeError("invalid schema field count")
        expected_names = ["list_i32", "struct_pair", "map_i32_i32", "u_dense", "dec", "ts"]
        if [f.name for f in schema] != expected_names:
            raise RuntimeError("invalid field names")
        if not pa.types.is_list(schema[0].type):
            raise RuntimeError("list_i32 must be list type")
        if not pa.types.is_struct(schema[1].type):
            raise RuntimeError("struct_pair must be struct type")
        if not pa.types.is_map(schema[2].type):
            raise RuntimeError("map_i32_i32 must be map type")
        if not pa.types.is_union(schema[3].type):
            raise RuntimeError("u_dense must be union type")
        if not pa.types.is_decimal(schema[4].type):
            raise RuntimeError("dec must be decimal type")
        if not pa.types.is_timestamp(schema[5].type):
            raise RuntimeError("ts must be timestamp type")

        batches = _read_all_batches(reader)
    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")
    batch = batches[0]
    if batch.num_rows != 3:
        raise RuntimeError("invalid row count")

    if batch.column(0).to_pylist() != [[1, 2], None, [3]]:
        raise RuntimeError("invalid list values")
    if batch.column(1).to_pylist() != [{"id": 10, "name": "aa"}, None, {"id": 30, "name": "cc"}]:
        raise RuntimeError("invalid struct values")
    if batch.column(2).to_pylist() != [[(1, 10), (2, 20)], None, [(3, 30)]]:
        raise RuntimeError("invalid map values")
    if batch.column(3).to_pylist() != [100, True, 200]:
        raise RuntimeError("invalid union values")
    if batch.column(4).to_pylist() != [Decimal("123.45"), Decimal("-0.42"), Decimal("0.00")]:
        raise RuntimeError("invalid decimal values")
    ts_values = batch.column(5).to_pylist()
    if [int(v.timestamp() * 1000) for v in ts_values] != [1700000000000, 1700000001000, 1700000002000]:
        raise RuntimeError("invalid timestamp values")


def validate_extension(in_path: pathlib.Path, container: str) -> None:
    with _open_reader(in_path, container) as reader:
        schema = reader.schema
        if len(schema) != 1:
            raise RuntimeError("invalid schema field count")
        field = schema[0]
        if field.name != "ext_i32":
            raise RuntimeError("invalid extension field name")
        if field.type != pa.int32():
            raise RuntimeError("invalid extension storage type")
        md = field.metadata
        if md is None:
            raise RuntimeError("missing extension metadata")
        if md.get(b"ARROW:extension:name") != b"com.example.int32_ext":
            raise RuntimeError("invalid extension name")
        if md.get(b"ARROW:extension:metadata") != b"v1":
            raise RuntimeError("invalid extension metadata")
        if md.get(b"owner") != b"interop":
            raise RuntimeError("invalid owner metadata")
        batches = _read_all_batches(reader)

    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")
    batch = batches[0]
    if batch.num_rows != 3:
        raise RuntimeError("invalid row count")
    if batch.column(0).to_pylist() != [7, None, 11]:
        raise RuntimeError("invalid extension values")


def validate_view(in_path: pathlib.Path, container: str) -> None:
    with _open_reader(in_path, container) as reader:
        schema = reader.schema
        if len(schema) != 2:
            raise RuntimeError("invalid schema field count")
        if schema[0].name != "sv" or schema[0].type != pa.string_view():
            raise RuntimeError("invalid sv field")
        if schema[1].name != "bv" or schema[1].type != pa.binary_view():
            raise RuntimeError("invalid bv field")
        batches = _read_all_batches(reader)

    if len(batches) != 1:
        raise RuntimeError("expected exactly one batch")
    batch = batches[0]
    if batch.num_rows != 4:
        raise RuntimeError("invalid row count")
    if batch.column(0).to_pylist() != ["short", None, "tiny", "this string is longer than twelve"]:
        raise RuntimeError("invalid sv values")
    if batch.column(1).to_pylist() != [b"ab", b"this-binary-view-is-long", None, b"xy"]:
        raise RuntimeError("invalid bv values")


def validate_tensor(in_path: pathlib.Path, container: str) -> None:
    payload = _extract_stream_payload(in_path, container)
    messages = _read_all_messages_from_payload(payload)
    if len(messages) != 2:
        raise RuntimeError("expected exactly schema + tensor messages")
    if messages[0].type != "schema":
        raise RuntimeError("first message must be schema")
    if messages[1].type != "tensor":
        raise RuntimeError("second message must be tensor")

    tensor_bytes = messages[1].serialize().to_pybytes()
    tensor = ipc.read_tensor(pa.BufferReader(tensor_bytes))
    if tensor.type != pa.int32():
        raise RuntimeError("tensor type must be int32")
    if tuple(tensor.shape) != (2, 3):
        raise RuntimeError(f"invalid tensor shape: {tensor.shape!r}")
    if tuple(tensor.strides) != (12, 4):
        raise RuntimeError(f"invalid tensor strides: {tensor.strides!r}")
    if tuple(tensor.dim_names) != ("rows", "cols"):
        raise RuntimeError(f"invalid tensor dim_names: {tensor.dim_names!r}")
    if tensor.size != 6:
        raise RuntimeError(f"invalid tensor size: {tensor.size!r}")


def main() -> int:
    if len(sys.argv) not in (2, 3, 4):
        print(
            "usage: pyarrow_validate.py <in.arrow> [canonical|dict-delta|ree|ree-int16|ree-int64|complex|extension|view|tensor] [stream|file]",
            file=sys.stderr,
        )
        return 2

    in_path = pathlib.Path(sys.argv[1])
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
        validate_canonical(in_path, container)
        return 0
    if mode == "dict-delta":
        validate_dict_delta(in_path, container)
        return 0
    if mode == "ree":
        validate_ree(in_path, container)
        return 0
    if mode == "ree-int16":
        validate_ree(in_path, container)
        return 0
    if mode == "ree-int64":
        validate_ree(in_path, container)
        return 0
    if mode == "complex":
        validate_complex(in_path, container)
        return 0
    if mode == "extension":
        validate_extension(in_path, container)
        return 0
    if mode == "view":
        validate_view(in_path, container)
        return 0
    if mode == "tensor":
        validate_tensor(in_path, container)
        return 0
    print(f"unknown mode: {mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
