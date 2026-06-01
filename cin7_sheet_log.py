"""
Cin7 sales-order → Freight Calculator spreadsheet logger.

When a Shopify-synced sales order is created in Cin7, append a row to the
NoEyeDeer Collections SharePoint workbook "Freight Calculator.xlsx" with:
    Company | Order Number | Shopify Freight Charge | Suggested Carrier | Delivery City

Uses Microsoft Graph app-only auth (Sites.Selected, scoped to the one site).
"""

import os
import time
from typing import Optional

import httpx

# ── Microsoft Graph identifiers ──────────────────────────────────────────────
TENANT_ID  = os.environ.get("MSSHEET_TENANT_ID",  "")
CLIENT_ID  = os.environ.get("MSSHEET_CLIENT_ID",  "")
CLIENT_SEC = os.environ.get("MSSHEET_CLIENT_SECRET", "")

# Hardcoded — these are stable identifiers for the NoEyeDeer site + file.
# Not secrets; safe to commit. If the file is moved/renamed, update these.
SITE_ID    = "nedcollections.sharepoint.com,ac44d5cc-0d46-4da5-acd6-e3f015867e6d,f5d5eb02-6711-40c3-bcdc-baaf582faf5d"
ITEM_ID    = "01XTEM6VVUU5ZMMHYNDNGK446UIS6CCGZI"   # Freight Calculator.xlsx
WORKSHEET  = "Sheet1"                                # human-readable name, stable

GRAPH = "https://graph.microsoft.com/v1.0"

# ── Token cache ──────────────────────────────────────────────────────────────
_token_cache = {"value": None, "expires_at": 0.0}


def _get_token() -> str:
    """Client-credentials token, cached until 60s before expiry."""
    if _token_cache["value"] and _token_cache["expires_at"] > time.time() + 60:
        return _token_cache["value"]
    if not (TENANT_ID and CLIENT_ID and CLIENT_SEC):
        raise RuntimeError("MSSHEET_TENANT_ID / CLIENT_ID / CLIENT_SECRET not set")
    r = httpx.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SEC,
            "grant_type":    "client_credentials",
            "scope":         "https://graph.microsoft.com/.default",
        },
        timeout=15,
    )
    r.raise_for_status()
    payload = r.json()
    _token_cache["value"]      = payload["access_token"]
    _token_cache["expires_at"] = time.time() + int(payload.get("expires_in", 3600))
    return _token_cache["value"]


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _ws_url() -> str:
    return f"{GRAPH}/sites/{SITE_ID}/drive/items/{ITEM_ID}/workbook/worksheets('{WORKSHEET}')"


def _used_range_rowcount() -> int:
    """Number of rows in the worksheet's used range (incl. header)."""
    r = httpx.get(f"{_ws_url()}/usedRange?$select=rowCount", headers=_headers(), timeout=20)
    r.raise_for_status()
    return int(r.json().get("rowCount", 0))


def order_already_logged(order_number: str) -> bool:
    """Idempotency: skip if Order Number column (B) already contains this ref."""
    if not order_number:
        return False
    n = _used_range_rowcount()
    if n < 2:
        return False
    # Read column B from row 2 to the last used row
    addr = f"B2:B{n}"
    r = httpx.get(f"{_ws_url()}/range(address='{addr}')?$select=values", headers=_headers(), timeout=20)
    r.raise_for_status()
    vals = r.json().get("values") or []
    target = order_number.strip().lower()
    for row in vals:
        cell = (row[0] if row else "")
        if isinstance(cell, str) and cell.strip().lower() == target:
            return True
    return False


def append_order_row(
    company:           str,
    order_number:      str,
    shopify_freight:   Optional[float],
    suggested_carrier: str,
    delivery_city:     str,
) -> dict:
    """
    Append one row to Freight Calculator (after the last used row).
    Columns A–E only — leaves F–I (Real Carrier / Real Charge / Note / Variance) blank
    for staff to complete once the carrier invoices.
    """
    next_row = _used_range_rowcount() + 1
    addr = f"A{next_row}:E{next_row}"
    body = {
        "values": [[
            company or "",
            order_number or "",
            float(shopify_freight) if shopify_freight is not None else "",
            suggested_carrier or "",
            delivery_city or "",
        ]]
    }
    r = httpx.patch(
        f"{_ws_url()}/range(address='{addr}')",
        headers=_headers(), json=body, timeout=30,
    )
    r.raise_for_status()
    return {"row": next_row, "address": addr}
