EPGS ETL Cron — Production Setup
=================================
Python 3.11+

─────────────────────────────────────
ARCHITECTURE
─────────────────────────────────────

  SOURCE  →  Remote MySQL  (pe_reporting_gold)   read-only
  TARGET  →  Local  MySQL  (epgs_reporting_gold) write

  The script opens TWO separate connections per cycle —
  one to each server — so they never need to be on the same host.

─────────────────────────────────────
1. SETUP
─────────────────────────────────────

  python3 -m venv venv
  source venv/bin/activate          # Windows: venv\Scripts\activate
  pip install -r epgs_requirements.txt

─────────────────────────────────────
2. CONFIGURE  (.env)
─────────────────────────────────────

  Edit .env and set:

  SRC_MYSQL_HOST      Remote server IP / hostname
  SRC_MYSQL_USER      Read-only user on source (recommended)
  SRC_MYSQL_PASSWORD  Source password

  TGT_MYSQL_HOST      127.0.0.1  (local)
  TGT_MYSQL_USER      Local MySQL user
  TGT_MYSQL_PASSWORD  Local password

  CUSTOMER_ID              Partner ID to filter
  CRON_INTERVAL_MINUTES    Default 15 (near real-time)

─────────────────────────────────────
3. RUN
─────────────────────────────────────

  source venv/bin/activate
  python epgs_cron.py

  The script loops forever:
    • Cycle 1  → FULL LOAD   (state file doesn't exist yet)
    • Cycle 2+ → INCREMENTAL (only rows changed since last run)

  Stop with Ctrl+C.

─────────────────────────────────────
4. RUN AS A BACKGROUND SERVICE
─────────────────────────────────────

  Option A — nohup (quick)
  ─────────────────────────
    nohup python epgs_cron.py >> /var/log/epgs_etl.log 2>&1 &

  Option B — systemd (recommended for production)
  ──────────────────────────────────────────────────
    Create /etc/systemd/system/epgs_etl.service:

      [Unit]
      Description=EPGS ETL Incremental Cron
      After=network.target

      [Service]
      User=youruser
      WorkingDirectory=/opt/epgs_etl
      ExecStart=/opt/epgs_etl/venv/bin/python epgs_cron.py
      Restart=on-failure
      RestartSec=30
      StandardOutput=journal
      StandardError=journal

      [Install]
      WantedBy=multi-user.target

    Then:
      systemctl daemon-reload
      systemctl enable epgs_etl
      systemctl start  epgs_etl
      journalctl -u epgs_etl -f     # live logs

─────────────────────────────────────
5. HOW INCREMENTAL WORKS
─────────────────────────────────────

  etl_state.json is created after every successful cycle:
    {
      "last_run_ts": "2025-04-16 10:00:00",
      "last_run_mode": "incremental",
      "customer_id": 215900,
      "previous_last_run_ts": "2025-04-16 09:45:00"
    }

  Next cycle WHERE clause:
    (partner filter)
    AND (
      (created_at > '2025-04-16 10:00:00' AND created_at <= '2025-04-16 10:15:00')
      OR
      (updated_at > '2025-04-16 10:00:00' AND updated_at <= '2025-04-16 10:15:00')
    )

  Tables with no timestamp columns (dim_date, dim_time, etc.)
  are skipped on incremental runs — already loaded on first run.

  Fact tables use INSERT … ON DUPLICATE KEY UPDATE so that
  corrected / updated rows propagate to the target.

─────────────────────────────────────
6. FORCE A FULL RELOAD
─────────────────────────────────────

  Delete the state file and restart:
    rm etl_state.json
    python epgs_cron.py

─────────────────────────────────────
7. TROUBLESHOOTING
─────────────────────────────────────

  "SOURCE DB connection failed"     → Wrong SRC_MYSQL_HOST / credentials.
                                      Check firewall — port 3306 must be
                                      open from this machine to the source.

  "TARGET DB connection failed"     → Wrong TGT_MYSQL_* credentials.

  "No partner_account_key found"    → Wrong CUSTOMER_ID in .env.

  State file permission error       → Ensure the process user can write
                                      to the ETL_STATE_FILE directory.
