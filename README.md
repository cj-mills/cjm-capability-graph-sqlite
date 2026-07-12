# cjm-capability-graph-sqlite

<!-- generated from the context graph by `cjm-context-graph readme` — do not edit by hand; edit the graph (the urge to hand-edit = move it on-graph) -->

A local, file-backed context-graph storage capability for the cjm-substrate runtime that implements graph storage, traversal, and querying using SQLite.

## Modules

- **`cjm_capability_graph_sqlite.capability`**
- **`cjm_capability_graph_sqlite.query_translation`** — The per-backend translation of the typed query expressions (pass-2 Thread 5; stage 4): NodeQuery/EdgeQuery -> parameterized SQLite SQL over the nodes/edges schema. THIS module is what makes the typed surface portable - the expression is domain- and backend-neutral; every backend tool owns a translation like this one (the ratified stage-4 split: backend owns translation; the adapter stays generic). Pure functions, unit-tested against an in-memory DB with the production schema - no capability runtime needed.
- **`tests_manual.validate_stage4_graph_storage_e2e`** — Stage-4 graph-storage stress validation (re-runnable; the OOM-script pattern).

## API

### `cjm_capability_graph_sqlite.capability`

- `SQLiteGraphCapability` _class_ — Local, file-backed Context Graph implementation using SQLite.
- `SQLiteGraphCapabilityConfig` _class_ — Configuration for SQLite Graph Capability.

### `cjm_capability_graph_sqlite.query_translation`

- `translate_edge_query` _function_ — Translate an `EdgeQuery` to parameterized SQLite SQL.
- `translate_node_query` _function_ — Translate a `NodeQuery` to parameterized SQLite SQL.

### `tests_manual.validate_stage4_graph_storage_e2e`

- `banner` _function_
- `main` _function_
- `make_nodes` _function_
- `task_call` _function_

## Dependencies

**Depends on:** `cjm-context-graph-primitives`, `cjm-substrate`
