# Tombstone — `test_sqlite_graph.py` (RETIRED 2026-06-18, stage 9)

**Origin:** `cjm-graph-plugin-sqlite/tests_manual/test_sqlite_graph.py` (pre-overhaul).
**Retired because:** imported the graph DTOs (`GraphNode`/`GraphEdge`/`GraphContext`/`SourceRef`) from the now-dissolved `cjm-graph-plugin-system` (GitHub-archived 2026-06-18; the DTOs now live in `cjm-context-graph-primitives`), and drove the tool directly via `initialize({"db_path"})` + the pre-Option-C `execute(action=…)` surface. Per the stage-9 decision the pre-overhaul `tests_manual` cohort is **retired, not patched**.

**What it validated (direct SQLiteGraphPlugin lifecycle):**
- `initialize({"db_path": …})` creates the DB.
- Build `GraphNode`/`GraphEdge` with a content-hash `SourceRef` (`SourceRef.compute_hash(content)`), e.g. a transcript Source → Person node.
- Push nodes/edges into the graph and read them back (node/edge CRUD + query).

**Coverage status:** PARTIALLY UNIQUE — the cores exercise sqlite-graph **only indirectly** (through the `graph-storage` task channel). This was the **direct, low-level** tool test (node/edge CRUD, `SourceRef` content-hash identity, query round-trip).

**Reimplementation target (first principles):** the `GenericGraphStorageAdapter` + the typed `NodeQuery`/`EdgeQuery`/`RawQuery` expression (the stage-4 portability layer) — exercise the sqlite backend through the adapter contract, not the dissolved domain DTOs or `execute(action=…)`. Fold into the graph-storage adapter's own tests or a core loop-back.
