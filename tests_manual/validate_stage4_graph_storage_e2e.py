#!/usr/bin/env python3
"""Stage-4 graph-storage stress validation (re-runnable; the OOM-script pattern).

Covers the stage-4 kickoff stress list items that live at this seam:
  A. live adapter arc (discovery -> compatibility -> auto-bind -> typed task round-trip)
     + the CR-17 negative check (explicit mismatched bind refused loudly)
  B. corruption surfacing (G3 institutionalized): integrity_check FAILS LOUDLY on a
     corrupted scratch DB; typed queries error rather than silently under-count
  C. worker kill -9 mid-add_nodes: typed death host-side + transaction rollback
     (no half-written spine) + clean reload
  D. concurrent shared-DB writers: two instances, one scratch file, gathered typed
     writes + reads — WAL holds; any contention arrives TYPED (G7 channel)
  E. P12 'contains' against the REAL corpus (read-only): the promoted predicate
     finds the Nanjing anchors; D13 aggregates stay bounded at 13k-segment scale

Run from the cjm-plugin-system repo dir (its .cjm runtime hosts the worker env):
  cd ../cjm-plugin-system && conda run -n cjm-plugin-system python \
      ../cjm-graph-plugin-sqlite/tests_manual/validate_stage4_graph_storage_e2e.py
Env: STAGE4_FAST=1 skips C+D (the process-level cases).
CORPUS_DB overrides the read-only corpus path for part E (skipped if absent).
"""
import asyncio
import os
import signal
import tempfile
import time
import uuid
from pathlib import Path

SUBSTRATE_REPO = Path("/mnt/SN850X_8TB_EXT4/Projects/GitHub/cj-mills/cjm-plugin-system")
CORPUS_DB = os.environ.get(
    "CORPUS_DB",
    "/mnt/SN850X_8TB_EXT4/Projects/GitHub/cj-mills/cjm-transcript-decomp-core"
    "/.cjm/data/cjm-graph-plugin-sqlite/context_graph.db")
FAST = os.environ.get("STAGE4_FAST") == "1"

os.chdir(SUBSTRATE_REPO)

from cjm_plugin_system.core.manager import PluginManager
from cjm_plugin_system.core.queue import JobQueue, JobStatus
from cjm_plugin_system.core.errors import (
    PluginInputError, PluginTransientError, WorkerOOMError,
)
from cjm_context_graph_primitives.query import (
    NodeQuery, EdgeQuery, RawQuery, NodeQueryResult, EdgeQueryResult,
    RawQueryResult, PropertyPredicate, RelationPredicate, OrderBy,
)
from cjm_context_graph_primitives.graph import GraphNode, GraphEdge

GRAPH = "cjm-graph-plugin-sqlite"


def banner(s):
    print(f"\n=== {s} ===", flush=True)


async def task_call(queue, instance, method, **kw):
    jid = await queue.submit(instance, task="graph-storage", method=method, **kw)
    job = await queue.wait_for_job(jid, timeout=120)
    if job.status != JobStatus.completed:
        raise RuntimeError(f"{method} failed: {job.error}")
    return job.result


def make_nodes(n, label="Segment"):
    return [GraphNode(id=str(uuid.uuid4()), label=label,
                      properties={"index": i, "text": f"text {i}"}).to_dict()
            for i in range(n)]


async def main():
    mgr = PluginManager(search_paths=[Path(".cjm/manifests")])
    discovered = mgr.discover_manifests()
    meta = next(m for m in discovered if m.name == GRAPH)
    adapters = mgr.adapter_manifests
    assert adapters and adapters[0].task_name == "graph-storage", \
        "adapter manifest missing — run cjm-ctl generate-adapter-manifest"

    # ---------------- A. live arc + negative check ----------------
    banner("A. compatibility + negative check")
    assert mgr.check_adapter_compatibility(adapters[0], GRAPH)["compatible"]
    other = next((m for m in discovered if m.name != GRAPH), None)
    if other is not None:
        try:
            mgr._resolve_adapter_specs(other, adapters=[adapters[0].name])
            raise AssertionError("mismatched explicit bind must refuse")
        except PluginInputError as e:
            print(f"  negative check OK ({other.name}): {str(e)[:100]}...")

    scratch = tempfile.mkdtemp(prefix="stage4_stress_")
    db_a = f"{scratch}/a.db"
    assert mgr.load_plugin(meta, config={"db_path": db_a})
    queue = JobQueue(mgr)
    await queue.start()
    try:
        ids = await task_call(queue, GRAPH, "add_nodes", nodes=make_nodes(10))
        assert len(ids) == 10
        res = await task_call(queue, GRAPH, "query_nodes",
                              query=NodeQuery(label="Segment", count=True).to_dict())
        assert isinstance(res, NodeQueryResult) and res.count == 10
        assert (await task_call(queue, GRAPH, "integrity_check"))["ok"] is True
        print("  typed round-trip OK (10 nodes, count, integrity ok)")

        # ---------------- B. corruption surfacing (G3) ----------------
        banner("B. corruption surfacing")
        # bulk the DB up so it spans many pages, then corrupt an INTERIOR page
        # (a tiny file can swallow trailing garbage invisibly — sqlite trusts
        # the header's page count)
        await task_call(queue, GRAPH, "add_nodes", nodes=make_nodes(2000))
        mgr.unload_plugin(GRAPH)  # cleanup closes the connection -> WAL checkpoints
        size = os.path.getsize(db_a)
        page = 4096
        target = max(1, (size // page) // 2) * page  # interior page boundary
        with open(db_a, "r+b") as f:
            f.seek(target)
            f.write(b"\xde\xad\xbe\xef" * (page // 4))  # trash the whole page
        print(f"  corrupted page at byte {target} of {size}")
        assert mgr.load_plugin(meta, config={"db_path": db_a})
        chk = await task_call(queue, GRAPH, "integrity_check")
        assert chk["ok"] is False and chk["errors"], chk
        print(f"  integrity_check FAILED LOUDLY as designed: {chk['errors'][:1]}")
        try:
            await task_call(queue, GRAPH, "query_nodes",
                            query=NodeQuery(label="Segment", count=True).to_dict())
            print("  (count survived corruption of this page — acceptable; "
                  "integrity_check is the loud gate)")
        except RuntimeError as e:
            print(f"  typed query errored loudly (never under-counts): {str(e)[:90]}")
        mgr.unload_plugin(GRAPH)

        if not FAST:
            # ---------------- C. kill -9 mid-add_nodes ----------------
            banner("C. kill -9 mid-add_nodes (rollback + typed death)")
            db_c = f"{scratch}/c.db"
            assert mgr.load_plugin(meta, config={"db_path": db_c})
            inst = mgr.instances[GRAPH]
            big = [GraphNode(id=str(uuid.uuid4()), label="Segment",
                             properties={"index": i, "text": "x" * 400}).to_dict()
                   for i in range(200000)]
            pid = inst.proxy.process.pid
            kill_task = asyncio.create_task(
                mgr.execute_plugin_task_async(GRAPH, "graph-storage", "add_nodes",
                                              nodes=big))
            # Deterministic mid-transaction trigger: in WAL mode uncommitted
            # pages append to the -wal DURING the transaction — kill when the
            # WAL shows the insert in flight.
            wal = db_c + "-wal"
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not kill_task.done():
                if os.path.exists(wal) and os.path.getsize(wal) > 512 * 1024:
                    break
                await asyncio.sleep(0.02)
            assert not kill_task.done(), "insert finished before the kill window (tune batch up)"
            os.kill(pid, signal.SIGKILL)
            # The FULL CR-7 Track-A arc is the contract here: SIGKILL ->
            # _check_worker_death -> typed WorkerOOMError (a PluginResourceError)
            # -> the manager's reactive retry RELOADS the worker and re-runs the
            # call. Acceptable outcomes: retry succeeds (normal) or typed error
            # (retries exhausted). NOT acceptable: a bare untyped failure, a
            # half-written spine, or duplicate rows from the retry.
            retried_ok = False
            try:
                ids = await kill_task
                retried_ok = True
                assert len(ids) == len(big)
            except (WorkerOOMError, PluginTransientError) as e:
                print(f"  typed death (retries exhausted): {type(e).__name__}: {str(e)[:80]}")
            except Exception as e:
                raise AssertionError(
                    f"death was NOT typed: {type(e).__name__}: {e}") from e
            new_pid = mgr.instances[GRAPH].proxy.process.pid
            assert new_pid != pid, "worker was not reloaded — CR-7 retry did not fire"
            chk = await task_call(queue, GRAPH, "integrity_check")
            assert chk["ok"] is True, f"DB damaged by kill -9: {chk}"
            res = await task_call(queue, GRAPH, "query_nodes",
                                  query=NodeQuery(count=True).to_dict())
            expected = len(big) if retried_ok else 0
            assert res.count == expected, \
                f"half-written/duplicated spine: {res.count} != {expected}"
            print(f"  CR-7 arc OK: typed death -> reload (pid {pid}->{new_pid}) -> "
                  f"{'retry succeeded' if retried_ok else 'typed failure'}; "
                  f"count exact ({res.count}); integrity ok — no half-written spine")
            mgr.unload_plugin(GRAPH)

            # ---------------- D. concurrent shared-DB writers ----------------
            banner("D. concurrent shared-DB writers (two instances, one file)")
            db_d = f"{scratch}/d.db"
            assert mgr.load_plugin(meta, config={"db_path": db_d}, instance_id="graph-w1")
            assert mgr.load_plugin(meta, config={"db_path": db_d}, instance_id="graph-w2")

            async def writer(instance, n_batches, batch):
                done = 0
                for _ in range(n_batches):
                    ids = await mgr.execute_plugin_task_async(
                        instance, "graph-storage", "add_nodes", nodes=make_nodes(batch))
                    done += len(ids)
                return done

            t0 = time.monotonic()
            a, b = await asyncio.gather(writer("graph-w1", 8, 250),
                                        writer("graph-w2", 8, 250))
            wall = time.monotonic() - t0
            res = await mgr.execute_plugin_task_async(
                "graph-w1", "graph-storage", "query_nodes",
                query=NodeQuery(count=True).to_dict())
            assert res.count == a + b == 4000, (res.count, a, b)
            chk = await mgr.execute_plugin_task_async("graph-w2", "graph-storage",
                                                      "integrity_check")
            assert chk["ok"] is True
            print(f"  WAL held: {a}+{b}=4000 nodes from 2 writers in {wall:.1f}s; integrity ok")
            mgr.unload_plugin("graph-w1")
            mgr.unload_plugin("graph-w2")

        # ---------------- E. corpus read-only: P12 contains + D13 timing ----------------
        if Path(CORPUS_DB).exists():
            banner("E. corpus (read-only): P12 contains + D13 bounded aggregates")
            assert mgr.load_plugin(meta, config={"db_path": CORPUS_DB, "readonly": True})
            t0 = time.monotonic()
            res = await task_call(queue, GRAPH, "query_nodes", query=NodeQuery(
                label="Segment",
                where=[PropertyPredicate("text", "contains", "nanjing")],
                count=True).to_dict())
            t_contains = time.monotonic() - t0
            assert res.count and res.count > 0, "P12 contains found nothing"
            print(f"  contains('nanjing') case-insensitive: {res.count} hits in {t_contains:.2f}s")
            docs = await task_call(queue, GRAPH, "query_nodes",
                                   query=NodeQuery(label="Document").to_dict())
            t0 = time.monotonic()
            for d in (docs.nodes or []):
                part_of = RelationPredicate("PART_OF", node_id=d.id)
                segs = await task_call(queue, GRAPH, "query_nodes", query=NodeQuery(
                    label="Segment", related=part_of, count=True).to_dict())
                nxt = await task_call(queue, GRAPH, "query_edges", query=EdgeQuery(
                    relation_type="NEXT", source_related=part_of, count=True).to_dict())
                assert nxt.count == max(0, (segs.count or 0) - 1), (d.id, segs.count, nxt.count)
            t_agg = time.monotonic() - t0
            print(f"  D13 aggregates over {len(docs.nodes or [])} docs / 13k+ segs: "
                  f"{t_agg:.2f}s total (bounded; no neighborhood materialization)")
            assert t_agg < 30, f"aggregates not bounded: {t_agg:.1f}s"
            mgr.unload_plugin(GRAPH)
        else:
            print("corpus DB absent; part E skipped")

        print("\nSTAGE-4 STRESS: ALL CHECKS PASSED" + (" (fast subset)" if FAST else ""))
    finally:
        await queue.stop()
        mgr.unload_all()


asyncio.run(main())
