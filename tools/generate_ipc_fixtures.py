#!/usr/bin/env python3
"""Generate Arrow IPC stream fixtures using PyArrow."""

from __future__ import annotations

from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

REQUIRED_PYARROW_VERSION = "23.0.1"


def write_stream(path: Path, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    options = ipc.IpcWriteOptions(use_legacy_format=False)
    with pa.OSFile(str(path), "wb") as sink:
        with ipc.new_stream(sink, schema, options=options) as writer:
            for batch in batches:
                writer.write_batch(batch)


def make_simple() -> tuple[pa.Schema, list[pa.RecordBatch]]:
    fields = [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
    ]
    schema = pa.schema(fields)
    batch = pa.record_batch(
        [pa.array([1, 2], type=pa.int32()), pa.array(["a", None], type=pa.string())],
        schema=schema,
    )
    return schema, [batch]


def make_metadata() -> tuple[pa.Schema, list[pa.RecordBatch]]:
    field_md = {b"alpha": b"1", b"z": b"9"}
    schema_md = {
        b"owner": b"core",
        b"version": b"1",
        b"pad": b"padpadpad",
    }
    fields = [
        pa.field("id", pa.int32(), nullable=False, metadata=field_md),
    ]
    schema = pa.schema(fields, metadata=schema_md)
    batch = pa.record_batch([pa.array([1, 2], type=pa.int32())], schema=schema)
    return schema, [batch]


def make_multi_batch() -> tuple[pa.Schema, list[pa.RecordBatch]]:
    fields = [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("payload", pa.binary(), nullable=True),
    ]
    schema = pa.schema(fields)
    batch1 = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([b"x", None, b"zz"], type=pa.binary()),
        ],
        schema=schema,
    )
    batch2 = pa.record_batch(
        [
            pa.array([4, 5], type=pa.int32()),
            pa.array([b"", b"y"], type=pa.binary()),
        ],
        schema=schema,
    )
    return schema, [batch1, batch2]


def main() -> None:
    if sys.version_info[:2] != (3, 11):
        raise SystemExit(
            f"Python 3.11 is required to generate stable fixtures; got {sys.version.split()[0]}"
        )

    if pa.__version__ != REQUIRED_PYARROW_VERSION:
        raise SystemExit(
            "PyArrow version must be "
            f"{REQUIRED_PYARROW_VERSION}; got {pa.__version__}"
        )

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "src" / "ipc" / "testdata"

    schema, batches = make_simple()
    write_stream(out_dir / "pyarrow_simple_stream.arrow", schema, batches)

    schema, batches = make_metadata()
    write_stream(out_dir / "pyarrow_metadata_stream.arrow", schema, batches)

    schema, batches = make_multi_batch()
    write_stream(out_dir / "pyarrow_multi_batch_stream.arrow", schema, batches)

    version_path = out_dir / "pyarrow_version.txt"
    version_path.write_text(
        f"python {sys.version.split()[0]}\npyarrow {pa.__version__}\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
