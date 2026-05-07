
"""
epgs_cron.py  —  EPGS Incremental ETL  (Production)
=====================================================

Architecture
------------
  SOURCE  : Remote MySQL  (pe_reporting_gold)   — read-only
  TARGET  : Local  MySQL  (epgs_reporting_gold) — write

Scheduler
---------
  Built-in loop — runs every CRON_INTERVAL_MINUTES (default 15).
  No external cron required; just keep the process running
  (systemd / Docker / pm2 / nohup).

Load strategy
-------------
  FIRST RUN  : Full load  — all partner rows, no timestamp filter.
  SUBSEQUENT : Incremental — only rows where created_at OR updated_at
               falls inside  (last_run_ts, now].
  State file : JSON file persisted between runs (atomic write).
  Dim tables : INSERT IGNORE  (safe to re-run).
  Fact tables: INSERT … ON DUPLICATE KEY UPDATE (captures edits too).

Privacy notes
-------------
  dim_parker : phone_number and email are intentionally sent as NULL
               to protect personal data. To re-enable, search for
               "PRIVACY MASK" comments and uncomment the original
               column expressions, then remove the NULL replacements.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────────────────────

load_dotenv()

# ── Source DB  (remote reporting server) ─────────────────────
SRC_HOST  = os.getenv("SRC_MYSQL_HOST",     "10.0.0.1")
SRC_PORT  = int(os.getenv("SRC_MYSQL_PORT", 3306))
SRC_USER  = os.getenv("SRC_MYSQL_USER",     "etl_reader")
SRC_PASS  = os.getenv("SRC_MYSQL_PASSWORD", "")
SOURCE_DB = os.getenv("SRC_MYSQL_DB",       "pe_reporting_gold_v5")

# ── Target DB  (local server) ────────────────────────────────
TGT_HOST  = os.getenv("TGT_MYSQL_HOST",     "127.0.0.1")
TGT_PORT  = int(os.getenv("TGT_MYSQL_PORT", 3306))
TGT_USER  = os.getenv("TGT_MYSQL_USER",     "root")
TGT_PASS  = os.getenv("TGT_MYSQL_PASSWORD", "")
TARGET_DB = os.getenv("TGT_MYSQL_DB",       "epgs_reporting_gold_v5")

# ── Partner filter ───────────────────────────────────────────
CUSTOMER_ID = int(os.getenv("CUSTOMER_ID", 215900))

# ── Scheduler ────────────────────────────────────────────────
CRON_INTERVAL_MINUTES = int(os.getenv("CRON_INTERVAL_MINUTES", 15))

# ── ETL tuning ───────────────────────────────────────────────
BATCH_SIZE      = int(os.getenv("BATCH_SIZE",      2000))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT",   30))

# ── State file ───────────────────────────────────────────────
ETL_STATE_FILE = Path(os.getenv("ETL_STATE_FILE", "./etl_state.json"))

# ─────────────────────────────────────────────────────────────
# Table constants
# ─────────────────────────────────────────────────────────────

# Reference tables: load once on first run, skip on incremental.
STATIC_TABLES: set[str] = {"dim_date", "dim_time"}

# Fact tables: use upsert on incremental so edits propagate.
UPSERT_FACT_TABLES: set[str] = {
    "dim_pass", 
    "fact_parking_session",
    "fact_reservation",
    "fact_payment",
    "fact_passes",
    "fact_permit_subscription",
    "fact_validation_redemption",
    "fact_payment_sweep_transactions",
}

# Ordered: dims before facts (FK safety).
DIM_TABLES = [
    "dim_date", "dim_time",
    "dim_partner_account", "dim_facility",
    "dim_parker", "dim_vehicle", "dim_rateplan",
    "dim_promo_code", "dim_payment_method", "dim_processor",
    "dim_parking_product", "dim_device", "dim_pass",
    "dim_event", "dim_permit_plan", "dim_reason",
    "dim_source_system","dim_policy"
]
# FACT_TABLES = [
#     "fact_parking_session",
#     "fact_reservation",
#     "fact_payment",
#     "fact_passes",
#     "fact_permit_subscription",
#     "fact_validation_redemption",       # ← NEW: added redemption fact table\
#     "fact_payment_sweep_transactions",
# ]

FACT_TABLES = [
    "fact_reservation",           # ✅ 1st — no dependencies
    "fact_permit_subscription",   # ✅ 2nd
    "fact_passes",                # ✅ 3rd
    "fact_parking_session",       # ✅ 4th — depends on reservation
    "fact_validation_redemption", # ✅ 5th
    "fact_payment",               # ✅ 6th
    "fact_payment_sweep_transactions",  # ✅ 7th — depends on fact_payment
]
ALL_TABLES = DIM_TABLES + FACT_TABLES

# ─────────────────────────────────────────────────────────────
# Privacy masking
# ─────────────────────────────────────────────────────────────

# Tables that require column-level masking before insert.
# Key   = table name
# Value = dict of { column_name : replacement_expression }
#
# To REVEAL personal data for a specific table/column:
#   1. Comment out the entry below (or remove it from the dict).
#   2. Redeploy / restart the process.
#   No other code change is required.
#
MASKED_COLUMNS: dict[str, dict[str, str]] = {
    # ── dim_parker ────────────────────────────────────────────
    # PRIVACY MASK: phone_number and email replaced with NULL.
    # Original columns exist in source; they are intentionally
    # withheld from the destination DB.
    # To re-enable: remove (or comment) the two lines below.
    "dim_parker": {
        "phone_number": "NULL",   # PRIVACY MASK — original: phone_number
        "email":        "NULL",   # PRIVACY MASK — original: email
    },

    "fact_parking_session": {
        "license_plate": "NULL",   # PRIVACY MASK — original: license_plate
    },    

    "fact_reservation": {
        "license_plate": "NULL",   # PRIVACY MASK — original: license_plate
    },      
}

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("epgs_etl")


# ─────────────────────────────────────────────────────────────
# State management  (atomic read / write)
# ─────────────────────────────────────────────────────────────

def load_state() -> dict:
    """Return persisted state, or {} on first run."""
    if ETL_STATE_FILE.exists():
        try:
            with ETL_STATE_FILE.open() as fh:
                state = json.load(fh)
            log.info("State loaded — last_run_ts: %s", state.get("last_run_ts"))
            return state
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("Corrupt/unreadable state file (%s) — treating as first run.", exc)
    else:
        log.info("No state file at %s — FIRST RUN (full load).", ETL_STATE_FILE)
    return {}


def save_state(state: dict) -> None:
    """Write state atomically: temp-file → rename (crash-safe)."""
    ETL_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=ETL_STATE_FILE.parent, suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w") as fh:
            json.dump(state, fh, indent=2)
        shutil.move(tmp_path, ETL_STATE_FILE)
        log.debug("State saved → %s", ETL_STATE_FILE)
    except OSError as exc:
        log.error("Failed to save state: %s", exc)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ─────────────────────────────────────────────────────────────
# Database connections  (separate source / target)
# ─────────────────────────────────────────────────────────────

@contextmanager
def source_connection():
    """Read-only connection to the REMOTE SOURCE DB."""
    conn = None
    try:
        conn = mysql.connector.connect(
            host=SRC_HOST,
            port=SRC_PORT,
            user=SRC_USER,
            password=SRC_PASS,
            database=SOURCE_DB,
            connection_timeout=CONNECT_TIMEOUT,
            autocommit=True,          # read-only, no transactions needed
        )
        log.info("SOURCE connected  → %s:%s / %s", SRC_HOST, SRC_PORT, SOURCE_DB)
        yield conn
    except MySQLError as exc:
        log.critical("SOURCE DB connection failed: %s", exc)
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()
            log.debug("SOURCE connection closed.")


@contextmanager
def target_connection():
    """Write connection to the LOCAL TARGET DB."""
    conn = None
    try:
        conn = mysql.connector.connect(
            host=TGT_HOST,
            port=TGT_PORT,
            user=TGT_USER,
            password=TGT_PASS,
            database=TARGET_DB,
            connection_timeout=CONNECT_TIMEOUT,
            autocommit=False,         # manual commit after all tables
        )
        log.info("TARGET connected  → %s:%s / %s", TGT_HOST, TGT_PORT, TARGET_DB)
        yield conn
    except MySQLError as exc:
        log.critical("TARGET DB connection failed: %s", exc)
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()
            log.debug("TARGET connection closed.")


# ─────────────────────────────────────────────────────────────
# Key fetchers  (queried from SOURCE)
# ─────────────────────────────────────────────────────────────

def get_partner_account_keys(scur) -> list[str]:
    scur.execute(
        f"""
        SELECT partner_account_key
        FROM {SOURCE_DB}.dim_partner_account
        WHERE account_id_source = %s
          AND is_current = 1
        """,
        (CUSTOMER_ID,),
    )
    keys = [str(r[0]) for r in scur.fetchall()]
    log.info("Partner account keys found: %d", len(keys))
    if not keys:
        raise ValueError(
            f"No partner_account_key for CUSTOMER_ID={CUSTOMER_ID}. "
            "Verify CUSTOMER_ID in .env and dim_partner_account on source."
        )
    return keys


def get_facility_mappings(scur) -> tuple[list[str], list[str]]:
    scur.execute(
        f"""
        SELECT facility_key, facility_id
        FROM {SOURCE_DB}.dim_facility
        WHERE operator_id = %s
          AND is_current = 1
        """,
        (CUSTOMER_ID,),
    )
    rows  = scur.fetchall()
    fkeys = [str(r[0]) for r in rows]
    fids  = [str(r[1]) for r in rows]
    log.info("Facility mappings found: %d keys, %d ids", len(fkeys), len(fids))
    if not fkeys:
        raise ValueError(
            f"No facility_key for CUSTOMER_ID={CUSTOMER_ID}. "
            "Verify CUSTOMER_ID in .env and dim_facility on source."
        )
    return fkeys, fids


# ─────────────────────────────────────────────────────────────
# Filter builders
# ─────────────────────────────────────────────────────────────

def _in(col: str, vals: list[str]) -> str:
    return f"{col} IN ({','.join(vals)})" if vals else "1=0"


def build_partner_filter(
    table: str,
    partner_keys: list[str],
    facility_keys: list[str],
    facility_ids: list[str],
) -> str:
    """Partner-scoping WHERE clause (no timestamp window)."""
    pk  = _in("partner_account_key", partner_keys)
    fk  = _in("facility_key",        facility_keys)
    fid = _in("facility_id",         facility_ids)

    mapping = {
        "dim_partner_account":  f"account_id_source = {CUSTOMER_ID}",
        "dim_facility":         f"operator_id = {CUSTOMER_ID}",
        "dim_promo_code":       fid,
        "dim_event":            fid,

        # ── dim_pass ──────────────────────────────────────────────────────
        # FIX: use subquery on dim_facility via operator_id instead of a
        # pre-fetched facility_key IN-list, which caused mismatches when
        # the keys list differed from what the subquery would return.
        # Original (mismatched):  f"({fk}) AND ({pk})",
        "dim_pass": (
            f"facility_key IN ("
            f"  SELECT facility_key"
            f"  FROM {SOURCE_DB}.dim_facility"
            f"  WHERE operator_id = {CUSTOMER_ID}"
            f")"
        ),

        "dim_parker": (
            f"parker_key IN ("
            f"  SELECT DISTINCT parker_key"
            f"  FROM {SOURCE_DB}.fact_parking_session"
            f"  WHERE {pk}"
            f")"
        ),
        "fact_parking_session":     f"({pk}) OR ({fk})",
        "fact_reservation":         f"({pk}) OR ({fk})",
        "fact_payment":             fk,
        "fact_permit_subscription": fk,
        "fact_passes": (
            f"pass_key IN ("
            f"  SELECT pass_key FROM {SOURCE_DB}.dim_pass"
            f"  WHERE facility_key IN ("
            f"    SELECT facility_key FROM {SOURCE_DB}.dim_facility"
            f"    WHERE operator_id = {CUSTOMER_ID}"
            f"  )"
            f")"
        ),

        # ── fact_validation_redemption ────────────────────────────────────
        # NEW: filter by facility_key via dim_facility subquery.
        # Equivalent SQL:
        #   SELECT * FROM fact_validation_redemption
        #   WHERE facility_key IN (
        #     SELECT facility_key FROM pe_reporting_gold.dim_facility
        #     WHERE operator_id = 215900
        #   );
        "fact_validation_redemption": (
            f"facility_key IN ("
            f"  SELECT facility_key"
            f"  FROM {SOURCE_DB}.dim_facility"
            f"  WHERE operator_id = {CUSTOMER_ID}"
            f")"
        ),

                # ── CHANGE 2: NEW ─────────────────────────────────────────
        "dim_policy": f"partner_id = {CUSTOMER_ID}",

        # ── CHANGE 3: NEW ─────────────────────────────────────────
        "fact_payment_sweep_transactions": (
            f"facility_key IN ("
            f"  SELECT facility_key"
            f"  FROM {SOURCE_DB}.dim_facility"
            f"  WHERE operator_id = {CUSTOMER_ID}"
            f")"
        ),
    }
    return mapping.get(table, "1=1")


def build_time_window_clause(
    table: str,
    ts_from: str,
    ts_to: str,
    scur,
) -> Optional[str]:
    """
    Returns  (created_at > ts_from AND created_at <= ts_to)
          OR (updated_at > ts_from AND updated_at <= ts_to)
    for whichever timestamp columns actually exist.
    Returns None if neither column exists on the table.
    """
    scur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME   = %s
          AND COLUMN_NAME IN ('created_at', 'updated_at')
        """,
        (SOURCE_DB, table),
    )
    ts_cols = {r[0] for r in scur.fetchall()}
    if not ts_cols:
        return None

    parts = [
        f"({col} > '{ts_from}' AND {col} <= '{ts_to}')"
        for col in ("created_at", "updated_at")
        if col in ts_cols
    ]
    return " OR ".join(parts)




# ─────────────────────────────────────────────────────────────
# Empty table check
# ─────────────────────────────────────────────────────────────

def is_table_empty(tcur, table: str) -> bool:
    try:
        tcur.execute(f"SELECT 1 FROM {TARGET_DB}.`{table}` LIMIT 1")
        return tcur.fetchone() is None
    except Exception:
        return True    


# ─────────────────────────────────────────────────────────────
# Column introspection  (queried from TARGET for upsert DML)
# ─────────────────────────────────────────────────────────────




def get_non_pk_columns(tcur, table: str) -> list[str]:
    """Columns that are NOT part of the PK — used in ON DUPLICATE KEY UPDATE."""
    tcur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (TARGET_DB, table),
    )
    all_cols = [r[0] for r in tcur.fetchall()]

    tcur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
          AND CONSTRAINT_NAME = 'PRIMARY'
        """,
        (TARGET_DB, table),
    )
    pk_cols = {r[0] for r in tcur.fetchall()}
    return [c for c in all_cols if c not in pk_cols]


# ─────────────────────────────────────────────────────────────
# Privacy masking helpers
# ─────────────────────────────────────────────────────────────

def build_select_with_masks(scur, table: str) -> str:
    """
    Build a SELECT column list for `table`.

    For any column listed in MASKED_COLUMNS[table], the real column
    is replaced by its mask expression (e.g. NULL AS phone_number).
    All other columns are selected as-is.

    Returns a comma-separated string ready to embed in SQL, e.g.:
        `col1`, NULL AS `phone_number`, `col2`, NULL AS `email`, ...
    """
    masks = MASKED_COLUMNS.get(table, {})
    if not masks:
        return "*"   # no masking needed — use simple SELECT *

    # Fetch real column list from SOURCE in ordinal order
    scur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (SOURCE_DB, table),
    )
    columns = [r[0] for r in scur.fetchall()]

    parts = []
    for col in columns:
        if col in masks:
            # PRIVACY MASK applied — real column commented for reference
            # Original expression would be: `{col}`
            parts.append(f"{masks[col]} AS `{col}`")
            log.debug("  PRIVACY MASK  %-30s → %s", col, masks[col])
        else:
            parts.append(f"`{col}`")

    masked_cols = [c for c in columns if c in masks]
    if masked_cols:
        log.info("  Privacy masks applied on %-20s columns: %s", table, masked_cols)

    return ", ".join(parts)


# ─────────────────────────────────────────────────────────────
# Core copy  (source cursor → target cursor, batched)
# ─────────────────────────────────────────────────────────────

def copy_table(
    scur,
    tcur,
    table: str,
    where_clause: str,
    upsert: bool = False,
) -> int:
    """
    Stream rows from SOURCE → TARGET in BATCH_SIZE chunks.

    If the table has entries in MASKED_COLUMNS, the SELECT replaces
    those columns with NULL (or whatever mask expression is configured)
    so personal data is never written to the target DB.

    Returns total rows written.
    """
    select_cols = build_select_with_masks(scur, table)
    query = f"SELECT {select_cols} FROM {SOURCE_DB}.`{table}` WHERE {where_clause}"

    log.info("  ▶ %-30s upsert=%-5s", table, upsert)
    log.debug("    WHERE: %.200s", where_clause)
    t0 = time.perf_counter()

    scur.execute(query)
    columns      = [c[0] for c in scur.description]
    col_list     = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    if upsert:
        non_pk = get_non_pk_columns(tcur, table)
        if non_pk:
            update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in non_pk)
            dml = (
                f"INSERT INTO {TARGET_DB}.`{table}` ({col_list})"
                f" VALUES ({placeholders})"
                f" ON DUPLICATE KEY UPDATE {update_clause}"
            )
        else:
            dml = (
                f"INSERT IGNORE INTO {TARGET_DB}.`{table}` ({col_list})"
                f" VALUES ({placeholders})"
            )
    else:
        dml = (
            f"INSERT IGNORE INTO {TARGET_DB}.`{table}` ({col_list})"
            f" VALUES ({placeholders})"
        )

    total = 0
    while True:
        rows = scur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        tcur.executemany(dml, rows)
        total += len(rows)

    elapsed = time.perf_counter() - t0
    log.info("  ✓ %-30s %d rows in %.2fs", table, total, elapsed)
    return total


# ─────────────────────────────────────────────────────────────
# ETL orchestrator  (one full cycle)
# ─────────────────────────────────────────────────────────────

def run_etl() -> None:
    run_start_utc = datetime.now(timezone.utc)
    run_start_ts  = run_start_utc.strftime("%Y-%m-%d %H:%M:%S")

    state        = load_state()
    last_run_ts  = state.get("last_run_ts")   # None → first run
    is_first_run = last_run_ts is None

    mode_label = "FULL_LOAD" if is_first_run else f"INCREMENTAL since {last_run_ts}"
    log.info("═" * 65)
    log.info("EPGS ETL START | %s | customer_id=%d", mode_label, CUSTOMER_ID)
    log.info("  Source : %s:%s / %s", SRC_HOST, SRC_PORT, SOURCE_DB)
    log.info("  Target : %s:%s / %s", TGT_HOST, TGT_PORT, TARGET_DB)
    log.info("═" * 65)

    run_summary: dict[str, int] = {}

    with source_connection() as src_conn, target_connection() as tgt_conn:
        scur = src_conn.cursor(buffered=True)
        tcur = tgt_conn.cursor()

        try:
            # Fetch partner/facility keys from SOURCE
            partner_keys           = get_partner_account_keys(scur)
            facility_keys, fac_ids = get_facility_mappings(scur)

            for table in ALL_TABLES:
                partner_clause = build_partner_filter(
                    table, partner_keys, facility_keys, fac_ids
                )

                # FIX: dim_pass always reload
                if table == "dim_pass":
                    where_clause = partner_clause

                # elif is_first_run or table in STATIC_TABLES:
                #     where_clause = partner_clause


                elif is_first_run or table in STATIC_TABLES:
                    where_clause = partner_clause

                elif is_table_empty(tcur, table):
                    log.info("  ⚠  %-30s is empty — forcing full load.", table)
                    where_clause = partner_clause

                else:
                    time_clause = build_time_window_clause(
                        table, last_run_ts, run_start_ts, scur
                    )
                    if time_clause is None:
                        log.info(
                            "  ⏭  %-30s skipped (no timestamp cols, already fully loaded).",
                            table,
                        )
                        run_summary[table] = 0
                        continue

                    where_clause = f"({partner_clause}) AND ({time_clause})"

                is_upsert = (table in UPSERT_FACT_TABLES) and (not is_first_run)

                rows_written = copy_table(
                    scur, tcur, table, where_clause, upsert=is_upsert
                )

                run_summary[table] = rows_written

            tgt_conn.commit()
            log.info("TARGET commit — OK")

        except Exception as exc:
            tgt_conn.rollback()
            log.exception("ETL FAILED — rolled back. Error: %s", exc)
            raise
        finally:
            scur.close()
            tcur.close()

    # Persist state only after clean success
    save_state({
        "last_run_ts":          run_start_ts,
        "last_run_utc":         run_start_utc.isoformat(),
        "last_run_mode":        "full_load" if is_first_run else "incremental",
        "customer_id":          CUSTOMER_ID,
        "previous_last_run_ts": last_run_ts,
    })

    total_rows = sum(run_summary.values())
    elapsed    = (datetime.now(timezone.utc) - run_start_utc).total_seconds()
    log.info("═" * 65)
    log.info(
        "EPGS ETL COMPLETE | mode=%s | total_rows=%d | elapsed=%.2fs",
        "FULL_LOAD" if is_first_run else "INCREMENTAL",
        total_rows,
        elapsed,
    )
    for tbl, cnt in run_summary.items():
        log.info("   %-35s %d rows", tbl, cnt)
    log.info("═" * 65)


# ─────────────────────────────────────────────────────────────
# Built-in scheduler  (runs every CRON_INTERVAL_MINUTES)
# ─────────────────────────────────────────────────────────────

def scheduler_loop() -> None:
    interval_sec = CRON_INTERVAL_MINUTES * 60
    log.info(
        "Scheduler started — interval: %d min. Press Ctrl+C to stop.",
        CRON_INTERVAL_MINUTES,
    )

    while True:
        cycle_start = time.monotonic()

        try:
            run_etl()
        except Exception as exc:
            # Log the failure but keep the scheduler alive for the next cycle.
            log.error("ETL cycle error (will retry at next interval): %s", exc)

        elapsed   = time.monotonic() - cycle_start
        sleep_sec = max(0.0, interval_sec - elapsed)

        next_at = datetime.now(timezone.utc)
        log.info(
            "Next ETL run in %.0f min (~%s UTC).",
            sleep_sec / 60,
            next_at.strftime("%Y-%m-%d %H:%M:%S"),
        )
        time.sleep(sleep_sec)


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        scheduler_loop()
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user (KeyboardInterrupt).")
        sys.exit(0)
    except Exception:
        log.exception("Unrecoverable error — exiting.")
        sys.exit(1)