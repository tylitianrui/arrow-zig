# zarrow

[English](../README.md) | [中文](README-ZH.md)
Apache Arrow 的 Zig 实现。

## 目标

zarrow 致力于提供 Apache Arrow 内存模型的 Zig 实现，重点关注：

- 清晰的所有权与生命周期语义
- 正确表达 Arrow 物理布局
- 显式校验数组结构与缓冲区布局
- 零拷贝切片与共享只读缓冲区
- 在 Zig 中构建 Arrow 数组的易用 Builder

## 项目状态

zarrow 目前处于积极开发阶段。

所有权模型和 API 仍在演进，但 IPC 路径现在对非法输入的处理已更严格、更明确。

当前状态（2026 年 4 月）：

- 已实现 Arrow 核心内存模型、数组 Builder、布局校验与零拷贝切片。
- IPC 流读取器支持解析 schema + record batch + dictionary batch，支持 V4/V5 元数据版本（仅支持小端 schema）。
- IPC 读取器对 body 长度、buffer 偏移/长度、node 长度执行显式范围/转换/溢出检查；损坏输入会映射为 `StreamError`/校验错误，而不是触发 trap。
- RecordBatch 元数据现在要求“完全消费”（`nodes`、`buffers`、`variadicBufferCounts` 都必须耗尽），避免接受尾部垃圾元数据。
- Dictionary schema 元数据已严格校验索引位宽（仅允许 `8/16/32/64`）。

已知限制：

- IPC writer 仍不支持输出 view 类型（`string_view`、`binary_view`、`list_view`、`large_list_view`）及 `extension` 类型。
- Dictionary delta 更新尚未完整实现；当 dictionary id 已存在时，delta 会被拒绝。
- Arrow IPC 的完整特性覆盖仍在推进中（当前尚未覆盖所有可选 IPC 特性）。

验证基线：

- 合并前应确保仓库内 `zig build test` 通过。

## 生成 IPC Fixtures（PyArrow）

IPC 兼容性测试可使用由 PyArrow 生成的 fixture。可在本地按以下步骤生成：

1. 创建独立虚拟环境（要求 Python 3.11）：  
   `python3.11 -m venv .venv-pyarrow`
2. 安装 PyArrow：  
   `./.venv-pyarrow/bin/python -m pip install pyarrow==23.0.1`
3. 生成 fixture：  
   `./.venv-pyarrow/bin/python tools/generate_ipc_fixtures.py`

该脚本会把 `.arrow` fixture 写入 `src/ipc/testdata/`，并把 PyArrow 版本写入 `src/ipc/testdata/pyarrow_version.txt`。  
当 fixture 变更时，嵌入这些 fixture 的 IPC reader 测试也会变化。若脚本或 PyArrow 输出变化，请重新生成并提交更新后的 `.arrow` 文件，以保持测试同步。

## 基准测试

- 运行全部基准（默认）：`zig build benchmark -Doptimize=ReleaseFast`
- 运行全部基准（smoke）：`zig build benchmark-smoke -Doptimize=ReleaseFast`
- 运行全部基准（full）：`zig build benchmark-full -Doptimize=ReleaseFast`
- 运行全部基准（matrix CSV）：`zig build benchmark-matrix -Doptimize=ReleaseFast`
- 运行全部基准（CI CSV）：`zig build benchmark-ci -Doptimize=ReleaseFast`
- 运行单个基准：`zig build benchmark-primitive_builder_benchmark -Doptimize=ReleaseFast`
- 以指定模式运行单个基准：`zig build benchmark-primitive_builder_benchmark -Doptimize=ReleaseFast -- matrix`

基准测试位于 `benchmarks/`，与示例代码分离维护。
