# ipc_schema

Pre-generated Zig bindings for the Arrow IPC FlatBuffers schema.

## Background

The Arrow IPC wire format uses [FlatBuffers](https://flatbuffers.dev/) to
encode metadata (schemas, record batch headers, file footers, tensor
descriptors, etc.).  The official schema files live in `src/format/*.fbs`.

These Zig bindings were generated once with
[flatc-zig](https://github.com/CalmSystem/flatc-zig) and committed to the
repository so that:

- **No code-generation tooling is required at build time.**  Users only need
  a supported Zig compiler; `flatc` and `flatc-zig` are not required.
- The generated code is stable and auditable — changes are visible in git
  history like any other source file.
- Cross-platform builds (including Windows) are not affected by `flatc`
  path-handling quirks.

## Structure

```
lib.zig                          # Module entry — @import all 57 generated files
ipc_schema/
  Schema.fb.zig                  # Schema, Field, DictionaryEncoding, …
  Message.fb.zig                 # Message, MessageHeader, MetadataVersion
  RecordBatch.fb.zig             # RecordBatch, FieldNode, Buffer, BodyCompression
  DictionaryBatch.fb.zig
  Footer.fb.zig                  # Footer, Block (IPC file format)
  Tensor.fb.zig                  # Tensor, TensorDim
  SparseTensor.fb.zig            # SparseTensor and index variants
  *.fb.zig                       # One file per FlatBuffers table/enum/union
```

Each `*.fb.zig` file exports two types following the flatc-zig convention:

- `FooT` — an unpacked, heap-allocated struct used for construction and mutation.
- `Foo`  — a zero-copy view into a raw FlatBuffers byte buffer.

## Regenerating

If the Arrow schema files in `src/format/` change, regenerate with:

```sh
# requires flatc-zig and flatc to be installed
flatc-zig --schema src/format/Schema.fbs --out src/format/.zig-cache/flatc-zig
flatc-zig --schema src/format/Message.fbs --out src/format/.zig-cache/flatc-zig
# … (see tools/generate_ipc_fixtures.py for the full invocation)

# patch Windows path bugs and missing semicolons in generated Union files
zig run tools/fix_flatc_lib.zig -- src/format/.zig-cache/flatc-zig

# copy the patched output over the committed files
cp -r src/format/.zig-cache/flatc-zig/. src/arrow_fbs/
```

The `fbs_runtime` module referenced by `@import("fbs_runtime")` in each
generated file is provided by the local `src/fbs_runtime/` module (see its
own README for details).
