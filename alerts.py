"""
Operational freight alerts → email (Resend) to the ops inbox (Kaleb).

Lightweight and in-process — the app is a warm, long-running Render service. Tracks a
rolling count of problem events and emails when a threshold is crossed within the
window, with a per-type cooldown so a sustained issue (e.g. a GoSweetSpot outage) sends
ONE email, not one per checkout. Everything is best-effort and never raises into a
checkout quote.

Event types:
  • courier_fallback   — CP dropped on a courier cart; served the % fallback. Noisy by
                         nature (transient drops), so only alerts past a threshold.
  • all_carriers_down  — NOTHING quoted; served the last-resort % of order. Serious.
  • no_rate            — couldn't even produce a last-resort rate (customer saw no rate).
  • quote_error        — an exception bubbled to the rate route (crash-guard fired).

Kill switch: FREIGHT_ALERTS_ENABLED=0.
"""

import asyncio
import logging
import os
from collections import deque
from datetime import datetime, timezone

from emailer import send_email

log = logging.getLogger("alerts")

ALERTS_ENABLED = os.environ.get("FREIGHT_ALERTS_ENABLED", "1") != "0"
ALERT_TO = os.environ.get("FREIGHT_ALERT_EMAIL", "kaleb@nedcollections.co.nz")
WINDOW_MIN = float(os.environ.get("ALERT_WINDOW_MIN", "15"))
COOLDOWN_MIN = float(os.environ.get("ALERT_COOLDOWN_MIN", "60"))
FALLBACK_THRESHOLD = int(os.environ.get("ALERT_FALLBACK_THRESHOLD", "6"))  # fallbacks/window = GSS degraded

_events = {}      # kind -> deque[float epoch]
_last_sent = {}   # kind -> float epoch


def _now() -> float:
    return datetime.now(timezone.utc).timestamp()


def _fire(kind: str, detail: str, threshold: int, blurb: str) -> None:
    """Record an event; email if the threshold is crossed in the window and cooldown allows."""
    if not ALERTS_ENABLED or not ALERT_TO:
        return
    now = _now()
    dq = _events.setdefault(kind, deque())
    dq.append(now)
    cut = now - WINDOW_MIN * 60
    while dq and dq[0] < cut:
        dq.popleft()
    if len(dq) < threshold:
        return
    last = _last_sent.get(kind, 0)
    if now - last < COOLDOWN_MIN * 60:
        return
    _last_sent[kind] = now
    count = len(dq)
    subject = f"⚠ NED Freight — {kind.replace('_', ' ')} ×{count} in {int(WINDOW_MIN)}min"
    text = (
        f"{count} '{kind}' event(s) in the last {int(WINDOW_MIN)} minutes on the freight app.\n\n"
        f"{blurb}\n\n"
        f"Most recent: {detail}\n\n"
        f"Rate log (see what's happening): https://ned-freight-app.onrender.com/  → Rate log tab\n"
        f"(Won't re-alert for {int(COOLDOWN_MIN)} min. Disable with FREIGHT_ALERTS_ENABLED=0.)\n"
    )
    try:
        asyncio.get_running_loop().create_task(send_email(ALERT_TO, subject, text))
    except RuntimeError:
        pass  # no running loop (e.g. unit test) — skip the email, never crash


def record_fallback(detail: str = "") -> None:
    _fire("courier_fallback", detail, FALLBACK_THRESHOLD,
          "Castle Parcels (GoSweetSpot) is dropping out — checkout is serving the % courier "
          "fallback instead of live courier rates. Likely a GSS slowdown/outage.")


def record_all_carriers_down(detail: str = "") -> None:
    _fire("all_carriers_down", detail, 1,
          "ALL carriers failed to quote — served the last-resort % of order. GoSweetSpot AND "
          "the Mainfreight Rating API are likely both unreachable. Investigate now.")


def record_no_rate(detail: str = "") -> None:
    _fire("no_rate", detail, 1,
          "A checkout got NO shipping rate (no carrier + no fallback possible). The customer "
          "may be blocked or shown a wrong default. Investigate now.")


def record_error(detail: str = "") -> None:
    _fire("quote_error", detail, 1,
          "The rate endpoint hit an exception (crash-guard fired). A code/infra error.")
