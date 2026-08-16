"""
Tiny Resend email helper for operational alerts (e.g. CBM sync failures -> Amy).

Reuses the NED Resend account (key in env). RESEND_FROM must be on a Resend-verified
domain (notify.nedcollections.co.nz). No-ops with a log line if the key is unset, so a
missing key never crashes the caller.
"""

import logging
import os

import httpx

log = logging.getLogger("emailer")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM = os.environ.get("RESEND_FROM", "NED Freight <noreply@notify.nedcollections.co.nz>")
RESEND_URL = "https://api.resend.com/emails"


async def send_email(to, subject: str, text: str, html: str = "") -> dict:
    """Send a plain-text (optionally HTML) email via Resend. Returns a status dict; never raises."""
    if not RESEND_API_KEY:
        log.warning("RESEND_API_KEY not set — skipping email %r", subject)
        return {"ok": False, "skipped": True, "reason": "no_api_key"}
    payload = {
        "from": RESEND_FROM,
        "to": [to] if isinstance(to, str) else list(to),
        "subject": subject,
        "text": text,
    }
    if html:
        payload["html"] = html
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(RESEND_URL, headers={"Authorization": f"Bearer {RESEND_API_KEY}"}, json=payload)
        if r.status_code >= 300:
            log.error("Resend send failed %s: %s", r.status_code, r.text[:300])
            return {"ok": False, "status": r.status_code, "body": r.text[:300]}
        return {"ok": True, "id": (r.json() or {}).get("id")}
    except Exception as e:
        log.error("Resend send exception: %s", e)
        return {"ok": False, "error": str(e)}
