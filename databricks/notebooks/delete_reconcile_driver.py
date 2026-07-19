# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Delete-Reconcile Driver — Hard-Delete Detection (snapshot_reconcile)
# Purpose   : CDC deletes are received; HARD deletes leave no event (a key just
#             stops appearing). This actively reconciles a COMPLETE source key
#             snapshot against Silver/Gold and SOFT-DELETES (tombstones) the rows
#             whose keys have disappeared — with a delete-rate circuit breaker so
#             a truncated/stale snapshot can't mass-tombstone the table.
#
# Design    : docs/hard-delete-reconciliation.md
# Sibling of: right_to_delete_driver.py (standalone Delta maintenance workflow).
#
# Safety    : soft-delete only (never physical delete) · guarded (abort on mass
#             delete) · freshness-gated · race-guarded · resurrection-aware · dry-run.
# ═══════════════════════════════════════════════════════════════════════════════


# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# ── Core ──────────────────────────────────────────────────────────────────────
dbutils.widgets.text("registry_path",
    "/Volumes/rideflow_dev_demo/nondelta/_contracts/marketplace/rideflow/_system.yaml",
    "Registry",
)
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Env")
dbutils.widgets.text("entity_filter", "", "Entities (blank = all with a deletion block)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run")

# ── Snapshot source ─────────────────────────────────────────────────────────────
# The COMPLETE current source key set. Either a `_keys` manifest table/path
# (model a) or a full-refresh landing of the source (model b). A partial/stale
# snapshot is the failure mode — the guards below defend against it.
dbutils.widgets.text("snapshot_table", "", "Snapshot key table/path (overrides deletion.key_source)")
dbutils.widgets.text("snapshot_ts_column", "", "Snapshot as-of column (for freshness + race guard)")

# ── Guards ──────────────────────────────────────────────────────────────────────
dbutils.widgets.text("max_delete_pct", "20", "Abort if >N% of active rows would tombstone")
dbutils.widgets.text("min_snapshot_rows", "1", "Abort if snapshot has fewer than N rows")
dbutils.widgets.text("max_snapshot_age_hours", "26", "Abort if snapshot older than N hours (0 = skip)")

# ── Tombstone columns (match the OSS soft-delete convention) ────────────────────
dbutils.widgets.text("flag_field", "_lakelogic_is_deleted", "Tombstone flag column")
dbutils.widgets.text("timestamp_field", "_lakelogic_deleted_at", "Tombstone timestamp column")
dbutils.widgets.text("reason_field", "_lakelogic_delete_reason", "Tombstone reason column")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Config

# COMMAND ----------

from datetime import datetime, timedelta, timezone

try:
    spark = spark
except NameError:
    spark = None

try:
    dbutils = dbutils
except NameError:
    class DBUtilsMock:
        class WidgetsMock:
            def get(self, *a, **k): return ""
        widgets = WidgetsMock()
    dbutils = DBUtilsMock()


def _w(name, default=""):
    v = dbutils.widgets.get(name)
    return v.strip() if isinstance(v, str) else v


REGISTRY_PATH = _w("registry_path")
ENVIRONMENT = _w("environment") or "dev"
ENTITY_FILTER = {e.strip().lower() for e in _w("entity_filter").split(",") if e.strip()}
DRY_RUN = _w("dry_run").lower() != "false"        # default TRUE — safe by default

SNAPSHOT_TABLE = _w("snapshot_table")
SNAPSHOT_TS_COLUMN = _w("snapshot_ts_column")

MAX_DELETE_PCT = float(_w("max_delete_pct") or "20")
MIN_SNAPSHOT_ROWS = int(_w("min_snapshot_rows") or "1")
MAX_SNAPSHOT_AGE_HOURS = float(_w("max_snapshot_age_hours") or "26")

FLAG_FIELD = _w("flag_field") or "_lakelogic_is_deleted"
TS_FIELD = _w("timestamp_field") or "_lakelogic_deleted_at"
REASON_FIELD = _w("reason_field") or "_lakelogic_delete_reason"
REASON_VALUE = "snapshot_reconcile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Reconciliation core
# MAGIC
# MAGIC For one entity: resolve its target + key from the contract, load the complete
# MAGIC key snapshot, run the three guards, then apply a **soft-delete MERGE** that
# MAGIC tombstones keys missing from the snapshot and resurrects keys that came back.

# COMMAND ----------

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def _is_path(t: str) -> bool:
    return "/" in t or t.startswith(("dbfs:", "s3", "abfss"))


def _delta_for(target: str) -> "DeltaTable":
    """A path-like target -> forPath; otherwise a catalog.schema.table -> forName."""
    t = target[len("table:"):] if target.startswith("table:") else target
    return DeltaTable.forPath(spark, t) if _is_path(t) else DeltaTable.forName(spark, t)


def _sql_ident(target: str) -> str:
    """The identifier for ALTER TABLE — `delta.`/path`` for paths, the name otherwise."""
    t = target[len("table:"):] if target.startswith("table:") else target
    return f"delta.`{t}`" if _is_path(t) else t


def _ensure_tombstone_columns(dt: "DeltaTable", target: str) -> None:
    """Add missing tombstone columns in-place via ALTER TABLE ADD COLUMNS (cheap,
    metadata-only — no table rewrite). New columns backfill to NULL on existing
    rows; the flag is read as ``coalesce(flag, false)`` everywhere so NULL == active."""
    cols = set(dt.toDF().columns)
    specs = []
    if FLAG_FIELD not in cols:
        specs.append(f"{FLAG_FIELD} BOOLEAN")
    if TS_FIELD not in cols:
        specs.append(f"{TS_FIELD} TIMESTAMP")
    if REASON_FIELD not in cols:
        specs.append(f"{REASON_FIELD} STRING")
    if specs:
        spark.sql(f"ALTER TABLE {_sql_ident(target)} ADD COLUMNS ({', '.join(specs)})")


def reconcile_entity(*, entity: str, target: str, primary_key, snapshot_df,
                     snapshot_ts: "datetime | None") -> dict:
    """Guarded hard-delete reconciliation for one entity. Returns a report dict."""
    pk = primary_key if isinstance(primary_key, (list, tuple)) else [primary_key]
    dt = _delta_for(target)
    _ensure_tombstone_columns(dt, target)
    dt = _delta_for(target)  # refresh handle after any schema change

    target_df = dt.toDF()
    active = target_df.filter(~F.coalesce(F.col(FLAG_FIELD), F.lit(False)))
    active_cnt = active.count()
    snap_keys = snapshot_df.select(*pk).dropDuplicates()
    snap_cnt = snap_keys.count()

    join_cond = [active[c] == snap_keys[c] for c in pk]
    candidates = active.join(snap_keys, join_cond, "left_anti")
    if snapshot_ts is not None and SNAPSHOT_TS_COLUMN:  # race guard — never tombstone rows newer than the snapshot
        if "updated_at" in target_df.columns:
            candidates = candidates.filter(F.col("updated_at") <= F.lit(snapshot_ts))
    cand_cnt = candidates.count()

    report = {"entity": entity, "target": target, "active": active_cnt,
              "snapshot": snap_cnt, "candidates": cand_cnt, "applied": 0,
              "aborted": None, "dry_run": DRY_RUN}

    # ── Guard 1: near-empty snapshot (a broken/truncated extract) ──
    if snap_cnt < MIN_SNAPSHOT_ROWS:
        report["aborted"] = f"snapshot has {snap_cnt} rows (< min {MIN_SNAPSHOT_ROWS}) — refusing to reconcile"
        return report
    # ── Guard 2: staleness ──
    if MAX_SNAPSHOT_AGE_HOURS and snapshot_ts is not None:
        age_h = (datetime.now(timezone.utc) - snapshot_ts).total_seconds() / 3600.0
        if age_h > MAX_SNAPSHOT_AGE_HOURS:
            report["aborted"] = f"snapshot is {age_h:.1f}h old (> {MAX_SNAPSHOT_AGE_HOURS}h) — refusing stale reconcile"
            return report
    # ── Guard 3: delete-rate circuit breaker (the headline guard) ──
    if active_cnt and (cand_cnt / active_cnt * 100.0) > MAX_DELETE_PCT:
        report["aborted"] = (f"{cand_cnt}/{active_cnt} ({cand_cnt/active_cnt*100:.1f}%) would tombstone "
                             f"> {MAX_DELETE_PCT}% — likely a bad snapshot, not reality. Raising an incident.")
        return report

    if DRY_RUN or cand_cnt == 0:
        return report

    # ── Apply: soft-delete MERGE (never physical delete) ──
    on = " AND ".join([f"t.{c} = s.{c}" for c in pk])
    ts_race = f" AND t.updated_at <= timestamp('{snapshot_ts.isoformat()}')" if (snapshot_ts is not None and "updated_at" in target_df.columns) else ""
    (dt.alias("t")
       .merge(snap_keys.alias("s"), on)
       # resurrection — a previously-tombstoned key reappeared at source
       .whenMatchedUpdate(condition=f"t.{FLAG_FIELD} = true",
                          set={FLAG_FIELD: "false", TS_FIELD: "null", REASON_FIELD: "null"})
       # hard-delete inferred — key present in target, absent from the snapshot.
       # coalesce so a freshly-added (NULL) flag column is treated as active.
       .whenNotMatchedBySourceUpdate(condition=f"coalesce(t.{FLAG_FIELD}, false) = false{ts_race}",
                                     set={FLAG_FIELD: "true",
                                          TS_FIELD: "current_timestamp()",
                                          REASON_FIELD: f"'{REASON_VALUE}'"})
       .execute())
    report["applied"] = cand_cnt
    return report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Run reconciliation
# MAGIC
# MAGIC Entities are selected from the registry: those whose contract declares
# MAGIC `deletion.strategy: snapshot_reconcile` (or matching the entity filter). The
# MAGIC snapshot source comes from the contract's `deletion.key_source`, or the
# MAGIC `snapshot_table` widget override.

# COMMAND ----------

from lakelogic.core.registry import DomainRegistry  # noqa: E402


def _load_snapshot(source: str):
    """Load the complete key snapshot (a table name, a path, or a view)."""
    if "/" in source or source.startswith(("dbfs:", "s3", "abfss")):
        return spark.read.format("delta").load(source)
    return spark.table(source)


def _snapshot_ts(df):
    if not SNAPSHOT_TS_COLUMN or SNAPSHOT_TS_COLUMN not in df.columns:
        return None
    row = df.agg(F.max(SNAPSHOT_TS_COLUMN).alias("m")).collect()
    m = row[0]["m"] if row else None
    if m is None:
        return None
    return m if getattr(m, "tzinfo", None) else m.replace(tzinfo=timezone.utc)


try:
    print(f"Loading registry: {REGISTRY_PATH} (env: {ENVIRONMENT})")
    registry = DomainRegistry.from_yaml(REGISTRY_PATH, environment=ENVIRONMENT)
    contracts = registry.get_active_contracts()

    selected = []
    for c in contracts:
        cd = c.contract_dict or {}
        deletion = cd.get("deletion") or {}
        opted_in = str(deletion.get("strategy", "")).lower() == "snapshot_reconcile"
        if ENTITY_FILTER:
            if c.entity.lower() in ENTITY_FILTER:
                selected.append((c, cd, deletion))
        elif opted_in:
            selected.append((c, cd, deletion))

    if not selected:
        print("⚠️ No entities selected. Add `deletion: {strategy: snapshot_reconcile, ...}` to a "
              "contract, or pass an entity filter + snapshot_table.")

    reports = []
    for c, cd, deletion in selected:
        entity = c.entity
        pk = deletion.get("primary_key") or cd.get("primary_key") or cd.get("natural_key")
        target = (cd.get("materialization") or {}).get("target_path") or c.resolved_path or c.path
        source = SNAPSHOT_TABLE or (deletion.get("key_source") or {}).get("table") \
            or (deletion.get("key_source") or {}).get("path")

        print("\n" + "─" * 70)
        print(f"▶ {entity}  (layer={c.layer})")
        if not pk:
            print("   ⏭️  skipped — no primary_key on the contract"); continue
        if not target:
            print("   ⏭️  skipped — could not resolve target table"); continue
        if not source:
            print("   ⏭️  skipped — no snapshot source (deletion.key_source or snapshot_table)"); continue

        snap = _load_snapshot(source)
        rep = reconcile_entity(entity=entity, target=target, primary_key=pk,
                               snapshot_df=snap, snapshot_ts=_snapshot_ts(snap))
        reports.append(rep)

        if rep["aborted"]:
            print(f"   🛑 ABORTED — {rep['aborted']}")
        else:
            verb = "would tombstone" if rep["dry_run"] else "tombstoned"
            print(f"   active={rep['active']}  snapshot={rep['snapshot']}  {verb}={rep['candidates']}"
                  + ("" if rep["dry_run"] else f"  applied={rep['applied']}"))

    print("\n" + "=" * 70)
    print(f"DELETE-RECONCILE COMPLETE — {len(reports)} entity(ies)"
          + (f"  [DRY RUN]" if DRY_RUN else ""))
    aborted = [r for r in reports if r["aborted"]]
    if aborted:
        print(f"⚠️ {len(aborted)} entity(ies) aborted on a guard — see above (treat as incidents).")
    print("=" * 70)

except Exception as e:
    print(f"\n❌ Delete-reconcile failed: {e}")
    raise
