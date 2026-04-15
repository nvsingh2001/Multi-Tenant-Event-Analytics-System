"""
seed/generate_seed.py
Multi-Tenant Event Analytics System — Seed Data Generator

Generates realistic SQL seed files for:
  - tenants     (5 orgs across different plan tiers)
  - users       (1,000 users spread across tenants)
  - events      (1,000,000 events over the last 90 days)

Realistic distributions:
  - Event volume follows a power-law per tenant (enterprise >> free)
  - User activity follows a power-law (a few power users, many casuals)
  - Event types follow a realistic funnel (page_view >> click >> purchase)
  - Timestamps follow a daily sine curve (peak at 14:00 UTC, trough at 03:00)
  - 30% of events carry a session_id (grouped into 15-min sessions)
  - ~2% of events are purchase events with a revenue amount in properties
  - A small fraction of users (5%) are associated with multiple tenants

Output files:
  seed/01_seed_tenants.sql
  seed/02_seed_users.sql
  seed/03_seed_events.sql   (split into chunks of 100k rows each)

Usage:
  python seed/generate_seed.py

Then load into PostgreSQL:
  psql -d your_db -f seed/01_seed_tenants.sql
  psql -d your_db -f seed/02_seed_users.sql
  psql -d your_db -f seed/03_seed_events_01.sql
  ... (repeat for all chunks)
"""

import uuid
import random
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
TOTAL_EVENTS = 1_000_000
TOTAL_USERS = 1_000
CHUNK_SIZE = 100_000  # rows per events SQL file
DAYS_BACK = 90
SEED = 42
OUTPUT_DIR = Path(__file__).parent
random.seed(SEED)

NOW = datetime.now(timezone.utc)

# ── Tenants ───────────────────────────────────────────────────────────────────
TENANTS = [
    {
        "tenant_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "name": "Acme Corp",
        "slug": "acme-corp",
        "plan_type": "enterprise",
        "weight": 0.40,  # 40% of all events
    },
    {
        "tenant_id": "bbbbbbbb-0000-0000-0000-000000000002",
        "name": "Globex Inc",
        "slug": "globex-inc",
        "plan_type": "pro",
        "weight": 0.25,
    },
    {
        "tenant_id": "cccccccc-0000-0000-0000-000000000003",
        "name": "Initech",
        "slug": "initech",
        "plan_type": "pro",
        "weight": 0.20,
    },
    {
        "tenant_id": "dddddddd-0000-0000-0000-000000000004",
        "name": "Umbrella Ltd",
        "slug": "umbrella-ltd",
        "plan_type": "free",
        "weight": 0.10,
    },
    {
        "tenant_id": "eeeeeeee-0000-0000-0000-000000000005",
        "name": "Stark Industries",
        "slug": "stark-industries",
        "plan_type": "free",
        "weight": 0.05,
    },
]

TENANT_IDS = [t["tenant_id"] for t in TENANTS]
TENANT_WEIGHTS = [t["weight"] for t in TENANTS]

# ── Event type funnel (weights sum to 1.0) ────────────────────────────────────
EVENT_TYPES = [
    (
        "page_view",
        0.40,
        lambda: {
            "page": random.choice(
                ["/home", "/products", "/about", "/pricing", "/blog", "/contact"]
            )
        },
    ),
    (
        "click",
        0.25,
        lambda: {
            "element": random.choice(
                ["btn_signup", "btn_pricing", "nav_menu", "hero_cta", "footer_link"]
            )
        },
    ),
    (
        "signup",
        0.08,
        lambda: {"source": random.choice(["organic", "paid", "referral", "email"])},
    ),
    ("login", 0.10, lambda: {}),
    (
        "add_to_cart",
        0.07,
        lambda: {
            "product_id": f"prod_{random.randint(1, 200):03d}",
            "price": round(random.uniform(9.99, 299.99), 2),
        },
    ),
    (
        "purchase",
        0.02,
        lambda: {
            "amount": round(random.uniform(9.99, 999.99), 2),
            "currency": "USD",
            "product_id": f"prod_{random.randint(1, 200):03d}",
        },
    ),
    ("logout", 0.04, lambda: {}),
    (
        "search",
        0.03,
        lambda: {
            "query": random.choice(
                ["analytics", "dashboard", "pricing", "api", "integration", "export"]
            )
        },
    ),
    (
        "profile_update",
        0.01,
        lambda: {"field": random.choice(["email", "name", "password", "avatar"])},
    ),
]

EVENT_NAMES = [e[0] for e in EVENT_TYPES]
EVENT_WEIGHTS = [e[1] for e in EVENT_TYPES]
EVENT_PROPS = {e[0]: e[2] for e in EVENT_TYPES}

# ── Devices / metadata ────────────────────────────────────────────────────────
DEVICES = ["desktop", "mobile", "tablet"]
DEV_WEIGHTS = [0.55, 0.35, 0.10]
COUNTRIES = ["US", "GB", "DE", "CA", "AU", "FR", "IN", "BR", "JP", "NL"]
PLANS = ["free", "pro", "enterprise"]

# ── Helpers ───────────────────────────────────────────────────────────────────


def escape_sql(s: str) -> str:
    return s.replace("'", "''")

def make_timestamp(days_back_max: int = DAYS_BACK, safety_hours: int = 6) -> datetime:
    """
    Generate a timestamp safely within the last `days_back_max` days.
    The safety buffer prevents edge-case failures during long generation/load times.
    """
    max_seconds = max(0, int(days_back_max * 86400 - safety_hours * 3600))

    while True:
        offset_seconds = random.randint(0, max_seconds)
        ts = NOW - timedelta(seconds=offset_seconds)

        hour = ts.hour + ts.minute / 60.0
        prob = 0.2 + 0.8 * (0.5 + 0.5 * math.sin(math.pi * (hour - 3) / 12))

        if random.random() < prob:
            return ts.replace(microsecond=0)



def power_law_user_weights(n: int) -> list:
    """
    Generate power-law weights so a few users generate most events.
    w_i = 1 / i^0.7
    """
    raw = [1.0 / (i**0.7) for i in range(1, n + 1)]
    total = sum(raw)
    return [w / total for w in raw]


# def fmt_ts(dt: datetime) -> str:
#     return dt.strftime("%Y-%m-%d %Human:%M:%S+00")


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


# ── Generate users ────────────────────────────────────────────────────────────


def generate_users() -> list:
    """
    Create TOTAL_USERS users distributed across tenants.
    5% of users appear in 2 tenants (multi-tenant users).
    """
    users = []
    uid_counter = 1

    for tenant in TENANTS:
        n_users = max(50, int(TOTAL_USERS * tenant["weight"] * 1.2))
        for _ in range(n_users):
            uid = f"u{uid_counter:05d}"
            uid_counter += 1
            users.append(
                {
                    "tenant_id": tenant["tenant_id"],
                    "user_id": uid,
                    "email": f"{uid}@example.com",
                    "display_name": f"User {uid_counter - 1}",
                    "plan": random.choice(PLANS),
                    "country": random.choice(COUNTRIES),
                }
            )

    # 5% multi-tenant overlap: re-use some user_ids in a second tenant
    base_users = [u for u in users if u["tenant_id"] == TENANTS[0]["tenant_id"]]
    overlap_pool = random.sample(base_users, max(1, int(len(base_users) * 0.05)))
    for u in overlap_pool:
        second_tenant = random.choice(TENANTS[1:])
        users.append(
            {
                "tenant_id": second_tenant["tenant_id"],
                "user_id": u["user_id"],  # same user_id, different tenant
                "email": u["email"],
                "display_name": u["display_name"],
                "plan": u["plan"],
                "country": u["country"],
            }
        )

    return users


# ── SQL generators ────────────────────────────────────────────────────────────


def write_tenants_sql(path: Path):
    lines = [
        "-- 01_seed_tenants.sql",
        "-- Generated seed data: 5 tenants",
        "",
        "TRUNCATE tenants CASCADE;",
        "",
        "INSERT INTO tenants (tenant_id, name, slug, plan_type, is_active, created_at, updated_at) VALUES",
    ]
    rows = []
    for t in TENANTS:
        created = (NOW - timedelta(days=random.randint(180, 730))).strftime(
            "%Y-%m-%dT%H:%M:%S+00:00"
        )
        rows.append(
            f"  ('{t['tenant_id']}', '{escape_sql(t['name'])}', '{t['slug']}', "
            f"'{t['plan_type']}', TRUE, '{created}', '{created}')"
        )
    lines.append(",\n".join(rows) + ";")
    lines.append("")
    lines.append(f"-- Inserted {len(TENANTS)} tenants")
    path.write_text("\n".join(lines))
    print(f"  Written: {path.name}")


def write_users_sql(users: list, path: Path):
    lines = [
        "-- 02_seed_users.sql",
        f"-- Generated seed data: {len(users)} users",
        "",
        "INSERT INTO users (tenant_id, user_id, email, display_name, metadata, created_at)",
        "VALUES",
    ]
    rows = []
    for u in users:
        created = (NOW - timedelta(days=random.randint(1, DAYS_BACK))).strftime(
            "%Y-%m-%dT%H:%M:%S+00:00"
        )
        metadata = f'{{"plan": "{u["plan"]}", "country": "{u["country"]}"}}'
        rows.append(
            f"  ('{u['tenant_id']}', '{u['user_id']}', "
            f"'{escape_sql(u['email'])}', '{escape_sql(u['display_name'])}', "
            f"'{metadata}', '{created}')"
        )
    lines.append(",\n".join(rows) + ";")
    lines.append("")
    lines.append(f"-- Inserted {len(users)} users")
    path.write_text("\n".join(lines))
    print(f"  Written: {path.name}  ({len(users)} users)")


def write_events_sql(users: list, output_dir: Path):
    """
    Generate TOTAL_EVENTS events and write in CHUNK_SIZE chunks.
    """
    # Build per-tenant user pools with power-law weights
    tenant_users = {}
    for tenant in TENANTS:
        pool = [u for u in users if u["tenant_id"] == tenant["tenant_id"]]
        weights = power_law_user_weights(len(pool))
        tenant_users[tenant["tenant_id"]] = (pool, weights)

    chunk_num = 1
    row_count = 0
    buffer = []
    total_written = 0

    def flush_chunk(buf, cnum):
        fpath = output_dir / f"03_seed_events_{cnum:02d}.sql"
        header = [
            f"-- 03_seed_events_{cnum:02d}.sql",
            f"-- Events chunk {cnum} ({len(buf)} rows)",
            "",
            "INSERT INTO events (event_id, tenant_id, user_id, event_name, event_time, properties, session_id, ingested_at)",
            "VALUES",
        ]
        fpath.write_text("\n".join(header) + "\n" + ",\n".join(buf) + ";\n")
        print(f"  Written: {fpath.name}  ({len(buf):,} rows)")

    for i in range(TOTAL_EVENTS):
        # Pick tenant
        tenant = random.choices(TENANTS, weights=TENANT_WEIGHTS, k=1)[0]
        tid = tenant["tenant_id"]

        # Pick user (power-law weighted)
        pool, weights = tenant_users[tid]
        user = random.choices(pool, weights=weights, k=1)[0]

        # Pick event type
        etype = random.choices(EVENT_NAMES, weights=EVENT_WEIGHTS, k=1)[0]

        # Build properties: base + event-specific + device
        props = EVENT_PROPS[etype]()
        props["device"] = random.choices(DEVICES, weights=DEV_WEIGHTS, k=1)[0]

        # Session ID: 30% of events get one
        session_id = "NULL"
        if random.random() < 0.30:
            # sessions last ~15 min; group by user + 15-min bucket
            session_id = f"'sess_{user['user_id']}_{random.randint(1, 5000):05d}'"

        # Timestamp
        event_time = make_timestamp()
        ingested_at = event_time + timedelta(seconds=random.randint(0, 3))

        # Escape props to valid JSON string
        import json

        props_str = json.dumps(props).replace("'", "''")

        event_id = str(uuid.uuid4())

        row = (
            f"  ('{event_id}', '{tid}', '{user['user_id']}', "
            f"'{etype}', '{fmt_ts(event_time)}', "
            f"'{props_str}', {session_id}, '{fmt_ts(ingested_at)}')"
        )
        buffer.append(row)
        row_count += 1

        if len(buffer) >= CHUNK_SIZE:
            flush_chunk(buffer, chunk_num)
            total_written += len(buffer)
            buffer = []
            chunk_num += 1

        if (i + 1) % 100_000 == 0:
            print(f"  Generated {i + 1:,} / {TOTAL_EVENTS:,} events...")

    # Final partial chunk
    if buffer:
        flush_chunk(buffer, chunk_num)
        total_written += len(buffer)

    print(f"\n  Total events generated: {total_written:,}  in {chunk_num} file(s)")


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n=== Multi-Tenant Event Analytics — Seed Generator ===\n")

    print("[1/4] Generating tenants...")
    write_tenants_sql(OUTPUT_DIR / "01_seed_tenants.sql")

    print("\n[2/4] Generating users...")
    users = generate_users()
    write_users_sql(users, OUTPUT_DIR / "02_seed_users.sql")

    print(f"\n[3/4] Generating {TOTAL_EVENTS:,} events (this may take ~30s)...")
    write_events_sql(users, OUTPUT_DIR)

    print("\n[4/4] Writing loader script...")
    loader = OUTPUT_DIR / "load_all.sh"
    chunk_files = sorted(OUTPUT_DIR.glob("03_seed_events_*.sql"))
    lines = [
        "#!/bin/bash",
        "# load_all.sh — load all seed files into PostgreSQL",
        "# Usage: DB_URL=postgresql://user:pass@host/db bash load_all.sh",
        "",
        'DB_URL="${DB_URL:-postgresql://localhost/analytics}"',
        "",
        'echo "Loading tenants..."',
        'psql "$DB_URL" -f 01_seed_tenants.sql',
        'echo "Loading users..."',
        'psql "$DB_URL" -f 02_seed_users.sql',
    ]
    for f in chunk_files:
        lines.append(f'echo "Loading {f.name}..."')
        lines.append(f'psql "$DB_URL" -f {f.name}')
    lines += [
        "",
        'echo ""',
        'echo "Refreshing materialized views..."',
        'psql "$DB_URL" -c "REFRESH MATERIALIZED VIEW mv_daily_active_users;"',
        'psql "$DB_URL" -c "REFRESH MATERIALIZED VIEW mv_revenue_summary;"',
        "",
        'echo "Done."',
    ]
    loader.write_text("\n".join(lines) + "\n")
    print(f"  Written: {loader.name}")

    print("\n=== Seed generation complete ===")
    print("\nTo load into PostgreSQL:")
    print("  cd seed/")
    print("  DB_URL=postgresql://user:pass@host/dbname bash load_all.sh")
    print("")
    print("File summary:")
    for f in sorted(OUTPUT_DIR.glob("*.sql")):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name:<40} {size_kb:>8.1f} KB")


if __name__ == "__main__":
    main()
