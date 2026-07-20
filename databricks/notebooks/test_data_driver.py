# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Test Data Driver — Generate landing-zone test data from contracts
# Purpose   : Generates realistic 24-hour streaming data into the ADLS landing
#             zone using the native RideFlow StreamingSimulator. Also injects 
#             surgical edge cases (TC-001 to TC-008) to validate the pipeline's
#             quarantine logic.
#
# Called by:
#   Databricks workflow or run interactively for development/testing.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %pip install lakelogic==1.40.0 pyyaml polars deltalake reportlab

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
    "Config - Registry",
)
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Config - Environment")
dbutils.widgets.text("num_windows", "24", "Config - Hours (Windows)")
dbutils.widgets.dropdown("inject_edge_cases", "true", ["true", "false"], "Config - Edge Cases")
# single  = generate only the system named by registry_path (called per-orchestrator).
# all     = discover every domain, topo-sort by cross-domain FK edges, then for each
#           wave generate landing data + run bronze+silver so downstream domains read
#           REAL upstream ids (hubspot→rider_id, zendesk→charge_id). FK-consistent seed.
dbutils.widgets.dropdown("seed_mode", "single", ["single", "all"], "Config - Seed mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Config

# COMMAND ----------

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import polars as pl
from lakelogic.core.streaming import StreamingSimulator
from lakelogic.core.registry import DomainRegistry

try:
    dbutils = dbutils
except NameError:
    class DBUtilsMock:
        class WidgetsMock:
            def get(self, *a, **k): return ""
        widgets = WidgetsMock()
    dbutils = DBUtilsMock()

ENVIRONMENT = dbutils.widgets.get("environment").strip() or "dev"
NUM_WINDOWS = int(dbutils.widgets.get("num_windows").strip() or "24")
INJECT_EDGE_CASES = dbutils.widgets.get("inject_edge_cases").lower() == "true"
REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
SEED_MODE = (dbutils.widgets.get("seed_mode").strip() or "single").lower()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Inject Domain Secrets (Interactive Fallback)
# MAGIC When running interactively outside of a Databricks Job, `spark_env_vars` are not automatically injected.
# MAGIC This cell manually pulls the secret scope and maps them to environment variables (e.g., `rideflow-dev-storage-account` → `RIDEFLOW_DEV_STORAGE_ACCOUNT`).

# COMMAND ----------

# ── Manual Override ───────────────────────────────────────────────────────────
# If you do not have access to the Key Vault or Databricks Secrets, you can 
# uncomment the lines below to pass your variables directly:
#
# os.environ["RIDEFLOW_DEV_STORAGE_ACCOUNT"] = "your_storage_account"
#
# # Azure Storage Credentials (Direct mode - no Databricks Unity Catalog credentials detected):
# # Set either SPN or Storage Key
# # os.environ["AZURE_CLIENT_ID"] = "your_spn_client_id"
# # os.environ["AZURE_CLIENT_SECRET"] = "your_spn_client_secret"
# # os.environ["AZURE_TENANT_ID"] = "your_tenant_id"
# # OR
# # os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "your_storage_key"
# ──────────────────────────────────────────────────────────────────────────────

env_upper = ENVIRONMENT.upper()
if f"RIDEFLOW_{env_upper}_STORAGE_ACCOUNT" not in os.environ:
    scope = "adbrideflow_secret_scope_001"
    print(f"🔄 Interactive mode detected. Mapping Databricks scope '{scope}' to OS environment variables...")
    try:
        # Only pull the specific secrets we actually need for the pipeline
        relevant_secrets = {
            f"rideflow-{ENVIRONMENT}-storage-account",
            "azure-client-id",
            "azure-client-secret",
            "azure-tenant-id",
            "azure-storage-account-key"
        }
        
        for secret in dbutils.secrets.list(scope=scope):
            if secret.key in relevant_secrets:
                # Key Vault secrets use hyphens (e.g., 'rideflow-dev-storage-account')
                # Environment variables use underscores (e.g., 'RIDEFLOW_DEV_STORAGE_ACCOUNT')
                env_key = secret.key.upper().replace("-", "_")
                os.environ[env_key] = dbutils.secrets.get(scope=scope, key=secret.key)
                print(f"  ✓ Mapped secret: {secret.key:<30} → os.environ['{env_key}']")
                
        # Polars object-store specific toggle for Azure Key authentication
        os.environ["POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY"] = "1"
        print(f"  ✓ Mapped secret: polars-auth-toggle             → os.environ['POLARS_AUTO_USE_AZURE_STORAGE_ACCOUNT_KEY']")
        print()
    except Exception as e:
        print(f"  ⚠ Could not map secrets: {e}\n")

storage_account = os.environ.get(f"RIDEFLOW_{env_upper}_STORAGE_ACCOUNT")
if not storage_account:
    print(f"Storage account name not found. Ensure RIDEFLOW_{env_upper}_STORAGE_ACCOUNT is set.")

# Catalog — derived from the registry Volume path (/Volumes/<catalog>/...) so it
# always matches the catalog this run targets. One source of truth; no ADLS needed.
_derived_catalog = "rideflow_dev_demo"
if REGISTRY_PATH.startswith("/Volumes/"):
    _p = REGISTRY_PATH.split("/")
    if len(_p) > 2 and _p[2]:
        _derived_catalog = _p[2]
CATALOG = os.environ.get(f"RIDEFLOW_{env_upper}_CATALOG") or _derived_catalog
# Ensure the catalog env var is set so the registry resolves {catalog} correctly
os.environ[f"RIDEFLOW_{env_upper}_CATALOG"] = CATALOG

if os.path.exists("/Volumes"):
    SYSTEM_YAML = REGISTRY_PATH or f"/Volumes/{CATALOG}/nondelta/_contracts/marketplace/rideflow/_system.yaml"
    mode = "uc"
else:
    if REGISTRY_PATH and os.path.exists(REGISTRY_PATH):
        SYSTEM_YAML = REGISTRY_PATH
    else:
        repo_root = Path(os.getcwd()).resolve()
        while not (repo_root / "domains_rideflow").exists() and repo_root.parent != repo_root:
            repo_root = repo_root.parent
        SYSTEM_YAML = str(repo_root / "domains_rideflow" / "marketplace" / "rideflow" / "_system.yaml")
    mode = "direct"

print(f"📖 Loading DomainRegistry from: {SYSTEM_YAML}")
registry = DomainRegistry.from_yaml(
    path=SYSTEM_YAML,
    environment=ENVIRONMENT,
    storage_mode=mode
)

if mode == "uc":
    LANDING_URI = registry.storage.landing_root
    print(f"✨ Databricks detected. Using Unity Catalog Volume.")
else:
    LANDING_URI = registry.storage.landing_path
    print(f"💻 Local mode detected. Using local path.")

print(f"🚀 Landing URI: {LANDING_URI}")
print(f"⏱ Windows: {NUM_WINDOWS} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌊 Per-domain data generators
# MAGIC
# MAGIC This driver is called once per system (its `registry_path` widget). It looks
# MAGIC up a generator for `(domain, system)` and writes that system's landing data.
# MAGIC - **marketplace/rideflow** → native `StreamingSimulator` + 800 edge cases
# MAGIC - **marketing / payments / operations** → synthetic CSV/PDF generators
# MAGIC   (ported from the RA repo's `test_data_driver_all`). Cross-domain FK pools
# MAGIC   (e.g. hubspot→rider_id) are read from **UC silver via Spark**, so run the
# MAGIC   marketplace orchestrator first for referential integrity; otherwise the
# MAGIC   readers fall back to synthetic IDs and the pipeline still runs.

# COMMAND ----------

# Derive (domain, system) from the registry path: .../_contracts/<domain>/<system>/_system.yaml
_parts = [p for p in SYSTEM_YAML.replace("\\", "/").split("/") if p]
DOMAIN, SYSTEM = (_parts[-3], _parts[-2]) if len(_parts) >= 3 else ("marketplace", "rideflow")
print(f"🧭 Target: {DOMAIN}/{SYSTEM}")


def _uc_fk_ids(domain, table, col, fallback_prefix, n=60):
    """Distinct FK values from a UC silver table via Spark; synthetic fallback.
    Lets cross-domain generators (hubspot→rider_id, zendesk→charge_id, …) key off
    REAL marketplace/payments ids when those layers have already run."""
    import uuid
    try:
        rows = spark.table(f"`{CATALOG}`.{domain}.{table}").select(col).distinct().limit(5000).collect()
        vals = [r[0] for r in rows if r[0] is not None]
        if vals:
            return vals
    except Exception as e:  # table missing / not yet built
        print(f"   WARN could not read {domain}.{table}.{col} ({e}); using synthetic IDs")
    return [f"{fallback_prefix}-{uuid.uuid4().hex[:8]}" for _ in range(n)]


def _marketing_partition_hour(now=None):
    now = now or datetime.now(timezone.utc)
    return f"y_{now.strftime('%Y')}/m_{now.strftime('%m')}/d_{now.strftime('%d')}/h_{now.strftime('%H')}"


def _write_csv(landing_root, entity, rows):
    """Write rows to {landing_root}/{entity}/y_/m_/d_/h_/<entity>_seed.csv."""
    import csv
    out_dir = Path(landing_root) / entity / _marketing_partition_hour()
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{entity}_seed.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"   {entity}: {len(rows)} rows -> {out}")


def _gen_trip_events(landing_uri):
    """Derive trip_requests (JSON) + trip_cancellations (CSV) from the simulator's
    already-written trip_completed landing, so both are FK-consistent with trips /
    riders / drivers. Demonstrates the THREE bronze ingest modalities alongside the
    CSV feeds and the checkr PDF extraction:
      - trip_requests      → JSON  (semi-structured request events, one array file)
      - trip_cancellations → CSV
    """
    import glob, json, random, uuid
    random.seed(43)
    files = glob.glob(f"{landing_uri}/trip_completed/**/*.csv", recursive=True)
    trips, seen = [], set()
    for fp in files:
        try:
            for t in pl.read_csv(fp, infer_schema_length=0).to_dicts():
                tid = t.get("trip_id")
                if tid and tid not in seen:
                    seen.add(tid); trips.append(t)
        except Exception as e:
            print(f"   WARN could not read {fp}: {e}")
    if not trips:
        print("   trip_events: no completed trips found — skipping")
        return

    # ── trip_requests (JSON): one request per completed trip + 30% unconverted,
    #    guaranteeing requested_trips >= completed_trips per (kpi_date, city) ──
    reqs = []
    def _req(t, uid):
        return {
            "request_id": uid,
            "rider_id": t.get("rider_id") or f"R-{uuid.uuid4().hex[:8]}",
            "trip_type": t.get("trip_type") or "ride",
            "pickup_lat": t.get("pickup_lat", ""), "pickup_lng": t.get("pickup_lng", ""),
            "dropoff_lat": t.get("dropoff_lat", ""), "dropoff_lng": t.get("dropoff_lng", ""),
            "city_code": t.get("city_code") or "LON",
            "requested_at": t.get("requested_at") or t.get("pickup_at") or "",
            "estimated_fare": t.get("fare_amount", ""),
            "estimated_eta_minutes": str(random.randint(2, 12)),
            "surge_multiplier": t.get("surge_multiplier", "1.0"),
        }
    for t in trips:
        reqs.append(_req(t, f"REQ-{(t.get('trip_id') or uuid.uuid4().hex)[:12]}"))
    for _ in range(max(1, int(len(trips) * 0.3))):
        reqs.append(_req(random.choice(trips), f"REQ-U-{uuid.uuid4().hex[:10]}"))
    part = _marketing_partition_hour()
    rdir = Path(landing_uri) / "trip_requests" / part
    rdir.mkdir(parents=True, exist_ok=True)
    # JSON array file (Spark reads with multiLine=true)
    (rdir / "trip_requests_seed.json").write_text(json.dumps(reqs), encoding="utf-8")

    # ── trip_cancellations (CSV): ~10% of trips, cancelled at/after request ──
    cancels = []
    for t in random.sample(trips, max(1, int(len(trips) * 0.1))):
        ts = t.get("requested_at") or t.get("pickup_at") or ""
        cancels.append({
            "cancellation_id": f"CAN-{uuid.uuid4().hex[:10]}",
            "trip_id": t.get("trip_id", ""),
            "rider_id": t.get("rider_id", ""),
            "driver_id": t.get("driver_id", ""),
            "cancelled_by": random.choice(["rider", "driver"]),
            "cancel_reason_code": random.choice(["rider_no_show", "driver_cancel", "long_wait", "price_change", "other"]),
            "city_code": t.get("city_code", "LON"),
            "requested_at": ts,
            "cancelled_at": ts,
            "cancellation_fee": str(round(random.uniform(0, 5), 2)),
        })
    _write_csv(landing_uri, "trip_cancellations", cancels)
    print(f"   trip_requests(JSON): {len(reqs)} | trip_cancellations(CSV): {len(cancels)}")


# ── marketplace/rideflow — native simulator + edge cases ─────────────────────
def gen_marketplace_rideflow(registry, landing_uri):
    import uuid, random
    sim = StreamingSimulator.rideflow_marketplace(
        landing_root=landing_uri, window_minutes=60,
        start_time=datetime.now(timezone.utc) - timedelta(hours=NUM_WINDOWS),
        seed=42, initial_riders=200, initial_drivers=100,
    )
    total_rows, windows = 0, []
    for window in sim.run(num_windows=NUM_WINDOWS, micro_batches=10, up_to=datetime.now(timezone.utc), resume=True):
        windows.append(window); total_rows += window.total_rows
    print(f"   {total_rows:,} rows across {len(windows)} window(s)" if windows else "   (simulator caught up — no new windows)")
    fk = sim.validate_fk_consistency()
    print(f"   🔗 FK pools: {fk['total_riders']} riders / {fk['total_drivers']} drivers  healthy={fk['fk_pools_healthy']}")

    # Derive request/cancellation events from the clean simulator trips (before the
    # deliberately-broken edge cases below). trip_requests lands as JSON.
    _gen_trip_events(landing_uri)

    if not INJECT_EDGE_CASES:
        return
    random.seed(42)
    cities = ["LON", "NYC", "BER", "PAR", "TYO", "SYD"]
    base_time = datetime.now(timezone.utc).replace(hour=8, minute=0, second=0, microsecond=0)
    records = []
    for i in range(800):
        req_dt = base_time.replace(hour=random.randint(7, 22), minute=random.randint(0, 59))
        records.append({
            "trip_id": str(uuid.uuid4()), "rider_id": f"R-{uuid.uuid4().hex[:8]}", "driver_id": f"D-{uuid.uuid4().hex[:8]}",
            "trip_type": random.choice(["ride", "eats_delivery"]),
            "pickup_lat": str(round(random.uniform(51.4, 51.6), 6)), "pickup_lng": str(round(random.uniform(-0.2, 0.1), 6)),
            "dropoff_lat": str(round(random.uniform(51.4, 51.6), 6)), "dropoff_lng": str(round(random.uniform(-0.2, 0.1), 6)),
            "city_code": random.choice(cities),
            "requested_at": req_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pickup_at": (req_dt + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dropoff_at": (req_dt + timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "distance_km": str(round(random.uniform(1, 25), 1)), "duration_minutes": str(random.randint(5, 45)),
            "fare_amount": str(round(random.uniform(5, 80), 2)), "surge_multiplier": str(round(random.uniform(1.0, 3.5), 2)),
            "tip_amount": str(round(random.uniform(0, 10), 2)),
            "payment_method": random.choice(["card", "apple_pay", "google_pay", "cash"]),
            "rider_rating": str(random.randint(1, 5)), "driver_rating": str(random.randint(1, 5)), "notes": "",
        })
    for i in range(0, 120):   records[i]["fare_amount"] = "-15.50"                       # TC-001 negative fare
    for i in range(120, 200): records[i]["driver_id"] = records[i]["rider_id"]           # TC-002 self-service
    for i in range(200, 350):                                                            # TC-003 time travel
        records[i]["pickup_at"]  = (base_time + timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
        records[i]["dropoff_at"] = (base_time + timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for i in range(350, 450): records[i]["distance_km"] = "650.5"                        # TC-004 impossible trip
    for i in range(450, 600): records[i]["notes"] = "DROP TABLE trips; SELECT * FROM users;"  # TC-005 injection
    for i in range(600, 680): records[i]["surge_multiplier"] = "5.8"                     # TC-006 cap violation
    for i in range(680, 740): records[i]["city_code"] = "XYZ"                            # TC-007 unknown market
    dup_id = records[0]["trip_id"]
    for i in range(740, 800): records[i]["trip_id"] = dup_id                             # TC-008 duplicate id
    random.shuffle(records)
    part_str = f"y_{base_time.strftime('%Y')}/m_{base_time.strftime('%m')}/d_{base_time.strftime('%d')}/h_99"
    edge_path = f"{landing_uri}/trip_completed/{part_str}/edge_cases.csv"
    os.makedirs(os.path.dirname(edge_path), exist_ok=True)
    pl.DataFrame(records).write_csv(edge_path)
    print(f"   injected 800 edge cases (TC-001..TC-008) → {edge_path}")


# ── marketing wave ───────────────────────────────────────────────────────────
def gen_marketing_google_ads(registry, landing_uri):
    import random, uuid
    random.seed(42); now = datetime.now(timezone.utc)
    campaign_pool = [("camp_" + uuid.uuid4().hex[:8], n) for n in ["RideFlow Brand UK", "Eats Delivery US", "Driver Recruit DE", "Surge Promo EU"]]
    rows = []
    for c_id, c_name in campaign_pool:
        for d_offset in range(14):
            date = (now - timedelta(days=d_offset)).date().isoformat()
            for ag in range(random.randint(2, 4)):
                rows.append({
                    "campaign_id": c_id, "campaign_name": c_name,
                    "ad_group_id": f"ag_{uuid.uuid4().hex[:8]}", "ad_group_name": f"{c_name} - AG{ag+1}",
                    "date": date, "impressions": str(random.randint(1000, 50000)), "clicks": str(random.randint(20, 2500)),
                    "cost": f"{random.uniform(50, 800):.2f}", "conversions": str(random.randint(0, 80)),
                    "conversion_value": f"{random.uniform(0, 1500):.2f}", "currency": random.choice(["GBP", "USD", "EUR"]),
                })
    _write_csv(landing_uri, "campaigns", rows)


def gen_marketing_google_analytics(registry, landing_uri):
    import random, uuid
    random.seed(43); now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    user_pool = [uuid.uuid4().hex for _ in range(80)]
    rider_ids = _uc_fk_ids("marketplace", "silver_rideflow_rider_profiles", "rider_id", "R", 80)
    events = []
    for _ in range(400):
        ts = now - timedelta(minutes=random.randint(0, 60*24*7))
        events.append({
            "event_name": random.choice(["app_open", "trip_requested", "trip_completed", "payment_method_added", "signup"]),
            "event_timestamp": str(int(ts.timestamp() * 1_000_000)), "event_date": ts.date().isoformat(),
            "user_pseudo_id": random.choice(user_pool), "user_id": random.choice(rider_ids) if rider_ids else "",
            "device_id": uuid.uuid4().hex[:16], "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "device_category": random.choice(["mobile", "desktop", "tablet"]), "device_os": random.choice(["iOS", "Android", "Web"]),
            "geo_country": random.choice(["GB", "US", "DE", "FR", "JP"]), "geo_city": random.choice(["London", "NYC", "Berlin", "Paris", "Tokyo"]),
            "app_version": random.choice(["4.18.0", "4.18.1", "4.19.0"]), "event_params_json": '{"source":"seed"}',
        })
    _write_csv(landing_uri, "app_events", events)
    sessions = []
    for _ in range(200):
        start = now - timedelta(minutes=random.randint(0, 60*24*7))
        sessions.append({
            "session_id": uuid.uuid4().hex, "user_pseudo_id": random.choice(user_pool), "user_id": random.choice(rider_ids) if rider_ids else "",
            "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}", "session_start": iso(start),
            "session_duration_seconds": str(random.randint(15, 1800)), "page_views": str(random.randint(1, 20)),
            "traffic_source": random.choice(["google", "facebook", "direct", "email"]), "traffic_medium": random.choice(["cpc", "organic", "referral", "email"]),
            "traffic_campaign": random.choice(["summer-2026", "london-brand", "", "driver-recruit"]),
            "device_category": random.choice(["mobile", "desktop", "tablet"]), "geo_country": random.choice(["GB", "US", "DE", "FR", "JP"]),
            "landing_page": random.choice(["/home", "/signup", "/drivers", "/eats", "/pricing"]),
        })
    _write_csv(landing_uri, "sessions", sessions)


def gen_marketing_hubspot(registry, landing_uri):
    import random, uuid
    random.seed(44); now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    rider_ids = _uc_fk_ids("marketplace", "silver_rideflow_rider_profiles", "rider_id", "R", 80)
    campaigns_pool = [(uuid.uuid4().hex[:12], n) for n in ["Weekly Digest", "Driver Bonus", "Surge Alert", "Welcome Series"]]
    email_rows = []
    for _ in range(180):
        c_id, c_name = random.choice(campaigns_pool)
        sent = now - timedelta(hours=random.randint(0, 24*14))
        opened = sent + timedelta(hours=random.randint(1, 24)) if random.random() < 0.45 else None
        clicked = (opened or sent) + timedelta(minutes=random.randint(1, 90)) if random.random() < 0.25 else None
        email_rows.append({
            "campaign_id": c_id, "recipient_email": f"user{random.randint(1,9999)}@example.com",
            "recipient_name": random.choice(["Aisha", "Carlos", "Mei", "Femi", "Liam"]) + " " + random.choice(["Patel", "Smith", "Chen", "Okafor", "Brown"]),
            "campaign_name": c_name, "subject_line": random.choice(["Your weekly RideFlow update", "Earn 2x this weekend", "Surge alert near you", "Welcome aboard"]),
            "sent_at": iso(sent), "opened_at": iso(opened) if opened else "", "clicked_at": iso(clicked) if clicked else "",
            "bounced": "true" if random.random() < 0.03 else "false", "unsubscribed": "true" if random.random() < 0.02 else "false",
        })
    _write_csv(landing_uri, "email_campaigns", email_rows)
    push_rows = []
    for _ in range(220):
        sent = now - timedelta(hours=random.randint(0, 24*7))
        delivered = sent + timedelta(seconds=random.randint(1, 30)) if random.random() < 0.95 else None
        opened = (delivered or sent) + timedelta(minutes=random.randint(1, 120)) if random.random() < 0.30 else None
        push_rows.append({
            "notification_id": uuid.uuid4().hex, "rider_id": random.choice(rider_ids), "device_token": uuid.uuid4().hex,
            "campaign_name": random.choice([n for _, n in campaigns_pool]), "sent_at": iso(sent),
            "delivered_at": iso(delivered) if delivered else "", "opened_at": iso(opened) if opened else "", "platform": random.choice(["ios", "android"]),
        })
    _write_csv(landing_uri, "push_notifications", push_rows)


def gen_marketing_meta_ads(registry, landing_uri):
    import random, uuid, json as _json
    random.seed(45); now = datetime.now(timezone.utc)
    campaign_pool = [("meta_" + uuid.uuid4().hex[:8], n) for n in ["FB Reels Brand", "IG Stories Driver", "Audience Network Eats", "Lookalike Riders"]]
    rows = []
    for c_id, c_name in campaign_pool:
        for d_offset in range(14):
            date = (now - timedelta(days=d_offset)).date().isoformat()
            for adset in range(random.randint(2, 4)):
                impressions = random.randint(2000, 90000)
                clicks = int(impressions * random.uniform(0.005, 0.04))
                conversions = int(clicks * random.uniform(0.01, 0.08))
                rows.append({
                    "campaign_id": c_id, "campaign_name": c_name, "adset_id": f"as_{uuid.uuid4().hex[:8]}", "adset_name": f"{c_name} - AS{adset+1}",
                    "date": date, "impressions": str(impressions), "clicks": str(clicks), "spend": f"{random.uniform(80, 1200):.2f}",
                    "actions_json": f"link_click|{clicks}",  # pipe, not JSON: CSV reader has no escape opt
                    "conversions": str(conversions), "currency": random.choice(["GBP", "USD", "EUR"]),
                })
    _write_csv(landing_uri, "campaigns", rows)


# ── payments wave ────────────────────────────────────────────────────────────
def gen_payments_stripe(registry, landing_uri):
    import csv, random, uuid
    random.seed(42); landing_root = Path(landing_uri)
    rider_ids  = _uc_fk_ids("marketplace", "silver_rideflow_rider_profiles",  "rider_id",  "R", 80)
    driver_ids = _uc_fk_ids("marketplace", "silver_rideflow_driver_profiles", "driver_id", "D", 60)
    trip_ids   = _uc_fk_ids("marketplace", "silver_rideflow_trips",           "trip_id",   "T", 200)
    print(f"   marketplace pool: {len(rider_ids)} riders / {len(driver_ids)} drivers / {len(trip_ids)} trips")
    now = datetime.now(timezone.utc)
    part = f"y_{now.strftime('%Y')}/m_{now.strftime('%m')}/d_{now.strftime('%d')}"
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"

    def _write(entity, rows):
        out_dir = landing_root / entity / part; out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{entity}_seed.csv"
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
        print(f"   ↳ {entity}: {len(rows)} rows → {out}")

    CCYS = ["GBP", "USD", "EUR"]; PM = ["card", "apple_pay", "google_pay"]
    charges = [{
        "charge_id": f"ch_{uuid.uuid4().hex[:24]}", "rider_id": random.choice(rider_ids), "trip_id": random.choice(trip_ids) if trip_ids else "",
        "amount": str(random.randint(500, 8500)), "currency": random.choice(CCYS),
        "status": random.choices(["succeeded", "failed", "pending"], weights=[92, 5, 3])[0],
        "payment_method_type": random.choice(PM), "card_last_four": f"{random.randint(0,9999):04d}",
        "billing_email": f"user{random.randint(1,9999)}@example.com", "billing_address": f"{random.randint(1,200)} Demo St, London",
        "created_at": iso(now - timedelta(minutes=random.randint(0, 60*24*14))), "metadata_json": '{"source":"seed"}',
    } for _ in range(min(800, max(200, len(trip_ids))))]
    _write("charges", charges)
    payouts = [{
        "payout_id": f"po_{uuid.uuid4().hex[:24]}", "driver_id": random.choice(driver_ids),
        "amount": str(random.randint(2000, 35000)), "currency": random.choice(CCYS),
        "status": random.choices(["paid", "pending", "failed"], weights=[88, 9, 3])[0],
        "payout_email": f"driver{random.randint(1,9999)}@example.com", "bank_account_last_four": f"{random.randint(0,9999):04d}",
        "arrival_date": (now - timedelta(days=random.randint(0,14)) + timedelta(days=2)).date().isoformat(),
        "created_at": iso(now - timedelta(days=random.randint(0,14))),
    } for _ in range(min(300, max(50, len(driver_ids)*3)))]
    _write("payouts", payouts)
    refunds = []
    for ch in random.sample(charges, k=max(20, len(charges)//16)):
        ts = datetime.fromisoformat(ch["created_at"].rstrip("Z")) + timedelta(hours=random.randint(1, 72))
        refunds.append({
            "refund_id": f"re_{uuid.uuid4().hex[:24]}", "charge_id": ch["charge_id"], "rider_id": ch["rider_id"],
            "amount": str(int(int(ch["amount"]) * random.uniform(0.3, 1.0))), "currency": ch["currency"],
            "status": random.choices(["succeeded", "pending", "failed"], weights=[90, 7, 3])[0],
            "reason": random.choice(["requested_by_customer", "duplicate", "fraudulent"]),
            "refund_reason": "Customer reported double-charge", "created_at": iso(ts),
        })
    _write("refunds", refunds)


# ── operations wave ──────────────────────────────────────────────────────────
def gen_operations_twilio(registry, landing_uri):
    import csv, random, uuid
    random.seed(46); now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    out_dir = Path(landing_uri) / "sms_logs" / _marketing_partition_hour(now); out_dir.mkdir(parents=True, exist_ok=True)
    twilio_number = "+15558675309"
    bodies = {
        "outbound": ["RideFlow: Your driver is 3 mins away.", "RideFlow: Driver has arrived at pickup.", "RideFlow Eats: Order on its way!", "Driver dispatch: New trip available, surge 1.4x.", "Verification code: 482910"],
        "inbound": ["Driver was late but courteous", "I left my phone in the car, can you help?", "STOP", "Why was I charged twice?", "Great trip, 5 stars"],
    }
    rows = []
    for _ in range(300):
        direction = random.choices(["outbound", "inbound"], weights=[70, 30])[0]
        sent = now - timedelta(minutes=random.randint(0, 60*24*7))
        delivered = sent + timedelta(seconds=random.randint(1, 30)) if random.random() < 0.95 else None
        user_phone = f"+1{random.randint(2000000000, 9999999999)}"
        rows.append({
            "message_sid": "SM" + uuid.uuid4().hex[:30],
            "recipient_phone": user_phone if direction == "outbound" else twilio_number,
            "sender_phone": twilio_number if direction == "outbound" else user_phone,
            "message_body": random.choice(bodies[direction]),
            "status": random.choices(["delivered", "sent", "failed", "undelivered"], weights=[88, 7, 3, 2])[0],
            "direction": direction, "num_segments": str(random.choice([1, 1, 1, 2])),
            "price": f"-{random.uniform(0.005, 0.025):.4f}", "price_unit": "USD",
            "sent_at": iso(sent), "delivered_at": iso(delivered) if delivered else "", "error_code": "30003" if random.random() < 0.03 else "",
        })
    out = out_dir / "sms_logs_seed.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    print(f"   sms_logs: {len(rows)} rows -> {out}")


def gen_operations_zendesk(registry, landing_uri):
    import csv, random, uuid, json as _json
    random.seed(47); now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    out_dir = Path(landing_uri) / "support_tickets" / _marketing_partition_hour(now); out_dir.mkdir(parents=True, exist_ok=True)
    rider_ids  = _uc_fk_ids("marketplace", "silver_rideflow_rider_profiles",  "rider_id",  "R", 80)
    driver_ids = _uc_fk_ids("marketplace", "silver_rideflow_driver_profiles", "driver_id", "D", 60)
    trip_ids   = _uc_fk_ids("marketplace", "silver_rideflow_trips",           "trip_id",   "T", 200)
    charge_ids = _uc_fk_ids("payments",    "silver_stripe_charges",           "charge_id", "ch", 200)
    subjects = {
        "trip": ["Trip never arrived", "Driver took a longer route", "Lost item in vehicle", "Driver cancelled at last minute"],
        "payment": ["Double charge on my card", "Surge pricing seems wrong", "Refund request", "Promo code didn't apply"],
        "safety": ["Safety concern with driver", "Inappropriate behaviour", "Vehicle was unsafe"],
        "account": ["Can't log in to app", "Phone number change", "Update payment method"],
        "general": ["Other feedback", "App crashing on launch", "Where is my receipt?"],
    }
    rows = []
    for _ in range(220):
        topic = random.choices(list(subjects), weights=[40, 30, 5, 15, 10])[0]
        created = now - timedelta(hours=random.randint(0, 24*14))
        status = random.choices(["new", "open", "pending", "solved", "closed"], weights=[5, 12, 8, 55, 20])[0]
        priority = random.choices(["low", "normal", "high", "urgent"], weights=[30, 50, 18, 2])[0]
        solved_at = created + timedelta(hours=random.randint(1, 96)) if status in ("solved", "closed") else None
        rows.append({
            "ticket_id": "zd_" + uuid.uuid4().hex[:24], "entry_point": random.choice(["in_app_help", "email", "web_form", "twitter"]),
            "rider_id": random.choice(rider_ids) if rider_ids else "",
            "trip_id": random.choice(trip_ids) if trip_ids and topic in ("trip", "safety") else "",
            "driver_id": random.choice(driver_ids) if driver_ids and topic in ("trip", "safety") else "",
            "charge_id": random.choice(charge_ids) if charge_ids and topic == "payment" else "",
            "requester_email": f"user{random.randint(1, 9999)}@example.com",
            "requester_name": random.choice(["Aisha", "Carlos", "Mei", "Femi", "Liam"]) + " " + random.choice(["Patel", "Smith", "Chen", "Okafor", "Brown"]),
            "requester_phone": f"+1{random.randint(2000000000, 9999999999)}", "subject": random.choice(subjects[topic]),
            "ticket_description": random.choice(subjects[topic]) + " — additional context added by rider.",
            "status": status, "priority": priority, "channel": random.choice(["chat", "email", "phone"]),
            "assignee": random.choice(["support_t1_01", "support_t1_02", "support_t2_01", ""]),
            "tags_json": f"{topic}|{priority}", "created_at": iso(created),  # pipe, not JSON (CSV-safe)
            "updated_at": iso(created + timedelta(minutes=random.randint(5, 60))), "solved_at": iso(solved_at) if solved_at else "",
            "satisfaction_rating": random.choice(["good", "bad", ""]) if status in ("solved", "closed") else "",
        })
    out = out_dir / "support_tickets_seed.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    print(f"   support_tickets: {len(rows)} rows -> {out}")


def gen_operations_checkr(registry, landing_uri):
    import csv, random, uuid
    random.seed(48); now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    driver_ids = _uc_fk_ids("marketplace", "silver_rideflow_driver_profiles", "driver_id", "D", 60)
    first_names = ["Aisha", "Carlos", "Dmitri", "Elena", "Femi", "Hiroshi", "Isla", "Jamal", "Kira", "Liam", "Mei", "Noor"]
    last_names  = ["Patel", "Smith", "Nakamura", "Okafor", "Rodriguez", "Chen", "Kowalski", "Garcia", "Singh", "Brown", "Adeyemi", "Volkov"]
    # background_checks CSV (FK -> driver_id)
    part = f"y_{now.strftime('%Y')}/m_{now.strftime('%m')}/d_{now.strftime('%d')}"
    bc_dir = Path(landing_uri) / "background_checks" / part; bc_dir.mkdir(parents=True, exist_ok=True)
    bc_rows = []
    for driver_id in driver_ids:
        completed = now - timedelta(days=random.randint(0, 365))
        result = random.choices(["clear", "consider", "suspended"], weights=[88, 10, 2])[0]
        adjud = {"clear": "eligible", "consider": "manual_review", "suspended": "ineligible"}[result]
        bc_rows.append({
            "report_id": "chk_" + uuid.uuid4().hex[:24], "driver_id": driver_id,
            "full_name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "date_of_birth": f"{random.randint(1960,2002)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "ssn_last_four": f"{random.randint(0,9999):04d}", "licence_number": f"DL-{uuid.uuid4().hex[:8].upper()}",
            "address": f"{random.randint(1,9999)} {random.choice(['Main','Oak','High','Park'])} St, London",
            "status": "complete", "result": result, "completed_at": iso(completed),
            "created_at": iso(completed - timedelta(days=random.randint(1, 7))), "adjudication": adjud,
        })
    if bc_rows:
        out = bc_dir / "background_checks_seed.csv"
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(bc_rows[0].keys())); w.writeheader(); w.writerows(bc_rows)
        print(f"   background_checks: {len(bc_rows)} rows -> {out}")
    # driver_licence PDFs (parsed by the pdfplumber extraction contract)
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
    except ImportError:
        print("   WARN reportlab not installed; skipping licence PDFs"); return
    pdf_dir = Path(landing_uri) / "driver_licences" / part; pdf_dir.mkdir(parents=True, exist_ok=True)
    classes_pool = ["B", "C1", "C+E", "D1", "PCO"]
    for idx in range(30):
        licence_no = f"{random.choice(['CHK','UK','DL'])}-{idx:04d}{random.randint(1000,9999):04d}"
        licence_text = "MISSING" if random.random() < 0.05 else licence_no
        c = canvas.Canvas(str(pdf_dir / f"licence_{licence_no}.pdf"), pagesize=letter)
        c.setFont("Helvetica-Bold", 16); c.drawString(72, 720, "UK DRIVER LICENCE (Synthetic - Test Data)")
        c.setFont("Helvetica", 12)
        c.drawString(72, 680, f"Name: {random.choice(first_names)} {random.choice(last_names)}")
        c.drawString(72, 660, f"Licence No: {licence_text}")
        c.drawString(72, 640, f"DOB: {random.randint(1960,2002)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}")
        c.drawString(72, 620, f"EXP: {random.randint(2024,2032)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}")
        c.drawString(72, 600, f"Classes: {', '.join(random.sample(classes_pool, k=random.randint(1,3)))}")
        c.save()
    print(f"   30 synthetic licence PDFs -> {pdf_dir}")


# ── reference wave — static lookup/seed CSVs ─────────────────────────────────
def gen_reference_internal(registry, landing_uri):
    """Synthetic reference-data seed CSVs (cities, fx rates, gold-from-landing seed
    dimensions). One dir per entity so the bronze/gold contracts that read
    {landing_root}/<entity> pick them up."""
    import csv, calendar
    root = Path(landing_uri)

    def _w(entity, rows):
        out_dir = root / entity
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{entity}_seed.csv"
        with out.open("w", encoding="utf-8", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=list(rows[0].keys())); wr.writeheader(); wr.writerows(rows)
        print(f"   {entity}: {len(rows)} rows -> {out}")

    cities = [("LON","London","GB","Europe/London","GBP","2018-04-01"),("NYC","New York","US","America/New_York","USD","2017-06-15"),
              ("BER","Berlin","DE","Europe/Berlin","EUR","2019-02-01"),("PAR","Paris","FR","Europe/Paris","EUR","2019-05-20"),
              ("TYO","Tokyo","JP","Asia/Tokyo","JPY","2020-01-10"),("SYD","Sydney","AU","Australia/Sydney","AUD","2018-09-12"),
              ("DXB","Dubai","AE","Asia/Dubai","AED","2021-03-01"),("SAO","Sao Paulo","BR","America/Sao_Paulo","BRL","2019-11-04")]
    _w("city_master", [{"city_code":c,"city_name":n,"country_code":cc,"timezone":tz,"default_currency":ccy,
        "geofence_wkt":"POLYGON((-0.5 51.3,-0.5 51.7,0.3 51.7,0.3 51.3,-0.5 51.3))","launched_at":la,"sunset_at":""}
        for c,n,cc,tz,ccy,la in cities])

    now = datetime.now(timezone.utc); fx=[]
    for d in range(30):
        rd=(now-timedelta(days=d)).date().isoformat()
        for q,mid in {"USD":1.27,"EUR":1.17,"JPY":191.0,"AUD":1.93}.items():
            drift=((d*7)%11-5)*0.001
            fx.append({"rate_date":rd,"base_currency":"GBP","quote_currency":q,"rate":round(mid+mid*drift,4),"source_feed":"ecb_synthetic"})
    _w("fx_rates_daily", fx)

    _w("dim_country_seed", [{"country_alpha3":a3,"country_alpha2":a2,"country_name":n,"country_numeric":num,"region":rg,"sub_region":sr}
        for a3,a2,n,num,rg,sr in [("GBR","GB","United Kingdom",826,"Europe","Northern Europe"),("USA","US","United States",840,"Americas","Northern America"),
        ("DEU","DE","Germany",276,"Europe","Western Europe"),("FRA","FR","France",250,"Europe","Western Europe"),("JPN","JP","Japan",392,"Asia","Eastern Asia"),
        ("AUS","AU","Australia",36,"Oceania","Australia and New Zealand"),("ARE","AE","United Arab Emirates",784,"Asia","Western Asia"),
        ("BRA","BR","Brazil",76,"Americas","South America"),("IND","IN","India",356,"Asia","Southern Asia"),("CAN","CA","Canada",124,"Americas","Northern America")]])
    _w("dim_currency_seed", [{"currency_code":c,"currency_numeric":num,"currency_name":n,"symbol":s,"minor_units":mu}
        for c,num,n,s,mu in [("GBP",826,"Pound Sterling","GBP",2),("USD",840,"US Dollar","USD",2),("EUR",978,"Euro","EUR",2),("JPY",392,"Yen","JPY",0),
        ("AUD",36,"Australian Dollar","AUD",2),("AED",784,"UAE Dirham","AED",2),("BRL",986,"Brazilian Real","BRL",2),("INR",356,"Indian Rupee","INR",2),("CAD",124,"Canadian Dollar","CAD",2)]])
    _w("dim_payment_method_seed", [{"payment_method_code":c,"payment_method_name":n,"rail":r,"requires_strong_auth":sca,"settles_instantly":si}
        for c,n,r,sca,si in [("card","Card (Visa/MC/Amex)","card_network","true","false"),("apple_pay","Apple Pay","card_network","true","false"),
        ("google_pay","Google Pay","card_network","true","false"),("cash","Cash","cash","false","true"),("sepa","SEPA Direct Debit","bank","false","false"),("pix","Pix (BR)","instant_bank","true","true")]])
    _w("dim_surge_tier_seed", [{"surge_tier_code":c,"multiplier_min":mn,"multiplier_max":mx,"display_label":l,"notify_rider":nt}
        for c,mn,mx,l,nt in [("NONE",1.0,1.0,"No surge","false"),("LOW",1.1,1.5,"Mild surge","false"),("MED",1.5,2.5,"High demand","true"),("HIGH",2.5,4.0,"Surge pricing","true"),("PEAK",4.0,6.0,"Peak surge","true")]])
    _w("dim_vehicle_type_seed", [{"vehicle_type_code":c,"vehicle_type_name":n,"passenger_capacity":cap,"product_tier":t,"requires_inspection":ins}
        for c,n,cap,t,ins in [("standard","RideFlow Standard",4,"economy","false"),("comfort","RideFlow Comfort",4,"select","true"),("xl","RideFlow XL",6,"premium","true"),("black","RideFlow Black",4,"premium","true"),("eats","Eats Delivery",1,"delivery","false")]])
    _w("dim_cancellation_reason_seed", [{"reason_code":c,"reason_label":l,"actor":a,"charge_cancellation_fee":fee,"counts_against_driver_rating":pen,"severity":sv}
        for c,l,a,fee,pen,sv in [("rider_changed_plans","Rider changed plans","rider","true","false","low"),("rider_no_show","Rider no-show","rider","true","false","medium"),
        ("driver_no_show","Driver no-show","driver","false","true","high"),("driver_long_eta","Driver took too long","driver","false","true","medium"),
        ("system_unable_to_match","System unable to match","system","false","false","low"),("safety_concern","Safety concern raised","rider","false","false","high")]])

    date_rows=[]; year=now.year
    for od in range(1,366):
        dd=datetime(year,1,1,tzinfo=timezone.utc)+timedelta(days=od-1)
        if dd.year!=year: break
        date_rows.append({"date_key":int(dd.strftime("%Y%m%d")),"full_date":dd.date().isoformat(),"year":dd.year,"quarter":(dd.month-1)//3+1,
            "month":dd.month,"month_name":calendar.month_name[dd.month],"day":dd.day,"day_of_week":dd.isoweekday(),"day_name":calendar.day_name[dd.weekday()],
            "is_weekend":"true" if dd.weekday()>=5 else "false","iso_week":dd.isocalendar()[1],"is_holiday_us":"false","is_holiday_uk":"false"})
    _w("dim_date_seed", date_rows)


GENERATORS = {
    ("marketplace", "rideflow"):        gen_marketplace_rideflow,
    ("reference",   "internal"):         gen_reference_internal,
    ("payments",    "stripe"):          gen_payments_stripe,
    ("operations",  "checkr"):          gen_operations_checkr,
    ("operations",  "twilio"):          gen_operations_twilio,
    ("operations",  "zendesk"):         gen_operations_zendesk,
    ("marketing",   "google_ads"):       gen_marketing_google_ads,
    ("marketing",   "google_analytics"): gen_marketing_google_analytics,
    ("marketing",   "hubspot"):          gen_marketing_hubspot,
    ("marketing",   "meta_ads"):         gen_marketing_meta_ads,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Dispatch
# MAGIC - **single** → generate just this system (called by each orchestrator's test_data task)
# MAGIC - **all** → FK-consistent seed of every domain in dependency order

# COMMAND ----------

def _run_bronze_silver(reg):
    """Run bronze+silver inline (spark on UC) so downstream waves find real ids."""
    try:
        from lakelogic.pipeline.runner import LakehousePipeline
    except ImportError:
        from lakelogic.pipeline import LakehousePipeline
    _engine = "spark" if mode == "uc" else "polars"
    _kw = {"registry": reg, "engine": _engine}
    if mode == "uc":
        _kw["spark"] = spark  # noqa: F821  (Databricks global)
    LakehousePipeline(**_kw).run(target_layers="bronze,silver")


if SEED_MODE != "all":
    # ── single-system generation ─────────────────────────────────────────────
    _gen = GENERATORS.get((DOMAIN, SYSTEM))
    if _gen is None:
        print(f"⏩ No generator registered for {DOMAIN}/{SYSTEM} — nothing to generate.")
    else:
        print(f"⚙️  Generating {DOMAIN}/{SYSTEM} with {_gen.__name__} → {LANDING_URI}")
        _gen(registry, LANDING_URI)
        print(f"✅ Landing data written for {DOMAIN}/{SYSTEM}")
else:
    # ── all-domains, FK-ordered seed (mirrors RA test_data_driver_all) ────────
    import glob as _glob, re as _re, yaml as _yaml
    from collections import defaultdict as _dd, deque as _deque

    # 1) Discover every system from the staged contract registry
    _contracts_root = f"/Volumes/{CATALOG}/nondelta/_contracts" if mode == "uc" else str(Path(SYSTEM_YAML).parents[3])
    SKIP = {"shared"}
    _sysyamls = sorted(_glob.glob(f"{_contracts_root}/*/*/_system.yaml"))
    systems = []
    for p in _sysyamls:
        try:
            data = _yaml.safe_load(open(p, encoding="utf-8")) or {}
        except Exception:
            continue
        d, s = data.get("domain"), data.get("system")
        if not d or not s or s in SKIP:
            continue
        systems.append((d, s, p))
    print(f"🔎 Discovered {len(systems)} systems: " + ", ".join(f"{d}/{s}" for d, s, _ in systems))

    # 2) Map produced contract names → owning (domain, system)
    owner = {}
    for d, s, p in systems:
        for c in _glob.glob(f"{Path(p).parent}/contracts/*/*.yaml"):
            try:
                data = _yaml.safe_load(open(c, encoding="utf-8")) or {}
            except Exception:
                continue
            tbl = (data.get("info") or {}).get("table_name") or ""
            resolved = (tbl.replace("{bronze_layer}", "bronze").replace("{silver_layer}", "silver")
                        .replace("{gold_layer}", "gold").replace("{system}", s).replace("{domain}", d))
            if resolved:
                owner[resolved] = (d, s)
            owner.setdefault(_re.sub(r"_v\d+\.\d+$", "", Path(c).stem), (d, s))

    # 3) Cross-system FK edges from bronze contracts' foreign_key refs
    nodes = {(d, s) for d, s, _ in systems}
    edges = _dd(set)
    for d, s, p in systems:
        for c in _glob.glob(f"{Path(p).parent}/contracts/bronze/*.yaml"):
            try:
                data = _yaml.safe_load(open(c, encoding="utf-8")) or {}
            except Exception:
                continue
            for field in ((data.get("model") or {}).get("fields") or []):
                fk = field.get("foreign_key")
                ref = fk.get("contract") if fk else None
                if not ref:
                    continue
                for cand in {ref, ref.replace("{bronze_layer}", "bronze").replace("{silver_layer}", "silver").replace("{system}", s).replace("{domain}", d)}:
                    if cand in owner and owner[cand] != (d, s):
                        edges[(d, s)].add(owner[cand])

    # 4) Topological sort (Kahn) — upstream first
    in_deg = {n: 0 for n in nodes}
    fwd = _dd(set)
    for down, ups in edges.items():
        for u in ups:
            if down in nodes and u in nodes:
                in_deg[down] += 1
                fwd[u].add(down)
    q = _deque(sorted(n for n in nodes if in_deg[n] == 0))
    run_order = []
    while q:
        n = q.popleft(); run_order.append(n)
        for nxt in sorted(fwd[n]):
            in_deg[nxt] -= 1
            if in_deg[nxt] == 0:
                q.append(nxt)
    if len(run_order) != len(nodes):  # cycle → fall back to discovery order
        run_order = [(d, s) for d, s, _ in systems]
    print("🧭 FK-ordered run plan:")
    for i, (d, s) in enumerate(run_order, 1):
        deps = ", ".join(f"{u[0]}/{u[1]}" for u in sorted(edges.get((d, s), set()))) or "—"
        print(f"  {i:2}. {d}/{s}  (deps: {deps})")

    # 5) Per wave: generate landing data + run bronze+silver (so downstream sees real ids)
    _paths = {(d, s): p for d, s, p in systems}
    for wave, (d, s) in enumerate(run_order, 1):
        print(f"\n{'═'*70}\n▶ WAVE {wave}/{len(run_order)}: {d}/{s}\n{'═'*70}")
        reg = DomainRegistry.from_yaml(path=_paths[(d, s)], environment=ENVIRONMENT, storage_mode=mode)
        luri = reg.storage.landing_root if mode == "uc" else reg.storage.landing_path
        g = GENERATORS.get((d, s))
        if g is None:
            print(f"   ⏩ no generator for {d}/{s} — skipping")
        else:
            print(f"   ⚙️  {g.__name__} → {luri}")
            g(reg, luri)
        try:
            print("   🏗  bronze + silver …")
            _run_bronze_silver(reg)
        except Exception as e:
            print(f"   ⚠️  bronze+silver failed for {d}/{s}: {e}")
    print(f"\n✅ All-domains seed complete — {len(run_order)} systems in FK order.")
