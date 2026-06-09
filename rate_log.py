"""
Rate-quote log — records every Shopify carrier-service rate call so quotes can be
cross-referenced against the Shopify orders that follow (Shopify rate requests carry
no order id, but the customer/company + address + timestamp match the order).

Stored in SQLite on a Render persistent disk. Path = RATE_LOG_DIR (default ./data
for local dev). Writes are best-effort: logging must NEVER break a checkout quote,
so every public function swallows its own errors.

Usage:
    rate_log.log_rate(destination=dest, items=items, result=result,
                      status="quoted", rate=78.0, error=None)
    rate_log.recent(limit=200)  # newest first
"""

import datetime
import json
import os
import sqlite3
import threading
from pathlib import Path

_DATA_DIR = Path(os.environ.get("RATE_LOG_DIR", str(Path(__file__).parent / "data")))
_DB_PATH = _DATA_DIR / "rate_log.db"
_lock = threading.Lock()
_initialised = False


def _conn() -> sqlite3.Connection:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(_DB_PATH), timeout=5)
    c.row_factory = sqlite3.Row
    return c


def _init() -> None:
    global _initialised
    if _initialised:
        return
    with _lock, _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS rate_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           TEXT NOT NULL,        -- ISO 8601, local NZ offset
                customer     TEXT,
                company      TEXT,
                address1     TEXT,
                city         TEXT,
                postcode     TEXT,
                province     TEXT,
                country      TEXT,
                cart_cbm     REAL,
                order_value  REAL,
                carrier      TEXT,
                service      TEXT,
                rate         REAL,                 -- price shown to customer (NZD)
                status       TEXT,                 -- quoted | free_shipping | no_carrier_match | error
                cp_available INTEGER,              -- 1 if a Castle Parcels/Kiwi (courier) quote was returned
                quotes       TEXT,                 -- JSON: all carrier quotes considered
                error        TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_rate_log_id ON rate_log(id DESC)")
    _initialised = True


def _order_value(items) -> float:
    try:
        return round(sum((int(i.get("price", 0)) / 100.0) * int(i.get("quantity", 1))
                         for i in (items or [])), 2)
    except Exception:
        return None


def _cp_available(quotes) -> bool:
    """True if a courier (Castle Parcels / Post Haste / Kiwi Express) quote was returned."""
    if not quotes:
        return False
    return any(
        any(tag in (q.get("carrier") or "") for tag in ("Haste", "Castle", "Kiwi"))
        for q in quotes
    )


def log_rate(*, destination=None, items=None, result=None,
             status="quoted", rate=None, error=None) -> None:
    """Best-effort insert of one rate event. Never raises."""
    try:
        _init()
        d = destination or {}
        res = result or {}
        quotes = res.get("all_quotes")
        row = (
            datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
            d.get("name") or "",
            d.get("company_name") or d.get("company") or "",
            d.get("address1") or d.get("address") or "",
            d.get("city") or "",
            d.get("postal_code") or d.get("zip") or "",
            d.get("province") or "",
            d.get("country") or "",
            res.get("cart_cbm"),
            _order_value(items),
            res.get("chosen_carrier") or "",
            res.get("chosen_service") or "",
            rate,
            status,
            1 if _cp_available(quotes) else 0,
            json.dumps(quotes) if quotes else None,
            error,
        )
        with _lock, _conn() as c:
            c.execute("""
                INSERT INTO rate_log
                  (ts, customer, company, address1, city, postcode, province, country,
                   cart_cbm, order_value, carrier, service, rate, status, cp_available, quotes, error)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, row)
    except Exception:
        pass  # logging must never break a checkout quote


def recent(limit: int = 200) -> list:
    """Most recent rate events, newest first. Returns [] on any error."""
    try:
        _init()
        limit = max(1, min(int(limit), 2000))
        with _lock, _conn() as c:
            rows = c.execute(
                "SELECT * FROM rate_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for r in rows:
            row = dict(r)
            if row.get("quotes"):
                try:
                    row["quotes"] = json.loads(row["quotes"])
                except Exception:
                    row["quotes"] = None
            out.append(row)
        return out
    except Exception:
        return []
