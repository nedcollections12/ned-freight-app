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


def _read_column_b() -> list:
    """
    Read column B (Order Number) from row 2 to the end of the used range.
    Returns a list of cell values (strings), one per row from row 2 onward.
    """
    n = _used_range_rowcount()
    if n < 2:
        return []
    r = httpx.get(
        f"{_ws_url()}/range(address='B2:B{n}')?$select=values",
        headers=_headers(), timeout=20,
    )
    r.raise_for_status()
    return [(row[0] if row else "") for row in (r.json().get("values") or [])]


def _is_empty(cell) -> bool:
    return cell is None or (isinstance(cell, str) and cell.strip() == "")


def order_already_logged(order_number: str) -> bool:
    """Idempotency: skip if Order Number column already contains this ref."""
    if not order_number:
        return False
    target = order_number.strip().lower()
    for cell in _read_column_b():
        if isinstance(cell, str) and cell.strip().lower() == target:
            return True
    return False


def _next_empty_row() -> int:
    """
    First row (starting at 2) whose Order Number column (B) is empty.
    Lands new entries in the pre-formatted blank template rows rather than
    after them — Excel's usedRange counts formatting-only cells as 'used',
    so a plain rowCount+1 jumps past the blank template section.
    """
    col_b = _read_column_b()
    for i, cell in enumerate(col_b):
        if _is_empty(cell):
            return 2 + i
    # Used range is fully populated — append below it
    return _used_range_rowcount() + 1


def _row_values(company, order_number, shopify_freight, suggested_carrier, delivery_city):
    return [[
        company or "",
        order_number or "",
        float(shopify_freight) if shopify_freight is not None else "",
        suggested_carrier or "",
        delivery_city or "",
    ]]


def append_order_row(company, order_number, shopify_freight, suggested_carrier, delivery_city):
    """Append after the last used row. Kept for backfill / explicit-append use."""
    next_row = _next_empty_row()
    addr = f"A{next_row}:E{next_row}"
    body = {"values": _row_values(company, order_number, shopify_freight, suggested_carrier, delivery_city)}
    r = httpx.patch(f"{_ws_url()}/range(address='{addr}')", headers=_headers(), json=body, timeout=30)
    r.raise_for_status()
    return {"row": next_row, "address": addr}


def prepend_order_row(company, order_number, shopify_freight, suggested_carrier, delivery_city):
    """
    Insert a new row at row 2 and shift everything below down by one — so the
    newest order is always at the top, just under the header. Used by live
    triggers (Shopify orders/create + Cin7 SalesOrder.Created webhooks).
    Columns A–E only; leaves F–I (Real Carrier / Real Charge / Note / Variance)
    blank for staff to complete once the carrier invoices.
    """
    # Insert an empty row at A2:I2, shifting existing rows down
    ins = httpx.post(f"{_ws_url()}/range(address='A2:I2')/insert",
                     headers=_headers(), json={"shift": "Down"}, timeout=30)
    ins.raise_for_status()
    # Write our values into the freshly-empty A2:E2
    body = {"values": _row_values(company, order_number, shopify_freight, suggested_carrier, delivery_city)}
    r = httpx.patch(f"{_ws_url()}/range(address='A2:E2')", headers=_headers(), json=body, timeout=30)
    r.raise_for_status()
    return {"row": 2, "address": "A2:E2"}
