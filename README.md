# cjm-capability-graph-sqlite

<!-- generated from the context graph by `cjm-context-graph readme` — do not edit by hand; edit the graph (the urge to hand-edit = move it on-graph) -->

A local, file-backed context-graph storage capability for the cjm-substrate runtime that implements graph storage, traversal, and querying using SQLite.

## Modules

- **`cjm_capability_graph_sqlite.capability`** — Capability implementation for Context Graph using SQLite.
- **`cjm_capability_graph_sqlite.query_translation`** — The per-backend translation of the typed query expressions (pass-2 Thread 5; stage 4): NodeQuery/EdgeQuery -> parameterized SQLite SQL over the nodes/edges schema. THIS module is what makes the typed surface portable - the expression is domain- and backend-neutral; every backend tool owns a translation like this one (the ratified stage-4 split: backend owns translation; the adapter stays generic). Pure functions, unit-tested against an in-memory DB with the production schema - no capability runtime needed.

## API

### `cjm_capability_graph_sqlite.capability`

- `SQLiteGraphCapability` _class_ — Local, file-backed Context Graph implementation using SQLite.
- `SQLiteGraphCapabilityConfig` _class_ — Configuration for SQLite Graph Capability.
- `integrity_check` _function_ — Backend self-check (stage 4; the G3 corruption find institutionalized):
- `query_edges` _function_ — Execute a typed edge query (stage 4) — same contract as `query_nodes`.
- `query_nodes` _function_ — Execute a typed node query (stage 4) — translated to parameterized SQL
- `raw_query` _function_ — Execute the raw escape — refuses backend mismatches (non-portable by

### `cjm_capability_graph_sqlite.query_translation`

- `translate_edge_query` _function_ — Translate an `EdgeQuery` to parameterized SQLite SQL.
- `translate_node_query` _function_ — Translate a `NodeQuery` to parameterized SQLite SQL.

## Dependencies

**Depends on:** `cjm-context-graph-primitives`, `cjm-substrate`
