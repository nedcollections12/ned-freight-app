"""
Forward CBM sync: Cin7 option `optionWeight` -> Shopify variant weight (= CBM the
freight app reads at checkout).

Fixes "new variant defaults to 0 CBM -> no freight rate at checkout": whenever a
plausible CBM (0 < cbm <= MAX) exists in Cin7 and differs from Shopify, push it to
Shopify. Matched by SKU (Cin7 option `code` == Shopify variant `sku`).

Guardrails (the Cin7 data has junk 31.0 defaults and decimal errors):
  * Only writes when 0 < cin7_cbm <= CBM_SYNC_MAX (default 4.0 m3).
  * NEVER overwrites a good Shopify value with a Cin7 zero/blank.
  * Out-of-range Cin7 values are skipped and emailed to CBM_SYNC_ALERT_TO to fix at source.
  * Any Shopify write error is collected and emailed too.

Run standalone dry-run:  python cin7_cbm_sync.py
Run standalone live:     python cin7_cbm_sync.py --live
Or call run_sync(dry_run=...) from the FastAPI app (POST /cin7/sync-cbm).
"""

import asyncio
import logging
import os
import sys
from base64 import b64encode
from pathlib import Path

import httpx

import emailer

log = logging.getLogger("cin7_cbm_sync")

CIN7_USERNAME = os.environ.get("CIN7_USERNAME", "")
CIN7_API_KEY = os.environ.get("CIN7_API_KEY", "")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "nedcollections.myshopify.com")
SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")

CBM_SYNC_MAX = float(os.environ.get("CBM_SYNC_MAX", "4.0"))     # above this = bad Cin7 data
EPS = 0.001                                                     # equality / min-positive tolerance
ALERT_TO = os.environ.get("CBM_SYNC_ALERT_TO", "amy@nedcollections.co.nz")

GQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"


def _cin7_auth():
    return {"Authorization": "Basic " + b64encode(f"{CIN7_USERNAME}:{CIN7_API_KEY}".encode()).decode()}


def _shopify_headers():
    return {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}


async def fetch_cin7_cbms(client) -> dict:
    """{sku_lower: {'sku','product','cbm'}} from Cin7 option optionWeight."""
    out, page = {}, 1
    while True:
        r = await client.get("https://api.cin7.com/api/v1/Products",
                             params={"page": page, "rows": 250, "fields": "id,name,productOptions"},
                             headers=_cin7_auth(), timeout=40)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        for p in batch:
            for o in (p.get("productOptions") or []):
                sku = (o.get("code") or "").strip()
                if sku:
                    out[sku.lower()] = {"sku": sku, "product": p.get("name", ""),
                                        "cbm": float(o.get("optionWeight") or 0)}
        if len(batch) < 250:
            break
        page += 1
    return out


async def fetch_shopify_cbms(client) -> dict:
    """{sku_lower: {'sku','product','inv_id','cbm'}} for active variants."""
    query = """
    query($cursor: String) {
      productVariants(first: 250, after: $cursor, query: "status:ACTIVE") {
        edges { node { sku product { title }
          inventoryItem { id measurement { weight { value } } } } }
        pageInfo { hasNextPage endCursor }
      }
    }"""
    out, cursor = {}, None
    while True:
        r = await client.post(GQL_URL, headers=_shopify_headers(),
                              json={"query": query, "variables": {"cursor": cursor}}, timeout=40)
        r.raise_for_status()
        d = r.json()["data"]["productVariants"]
        for e in d["edges"]:
            v = e["node"]
            sku = (v.get("sku") or "").strip()
            if not sku:
                continue
            w = (v["inventoryItem"]["measurement"] or {}).get("weight")
            out[sku.lower()] = {"sku": sku, "product": v["product"]["title"],
                                "inv_id": v["inventoryItem"]["id"],
                                "cbm": float((w["value"] if w else 0) or 0)}
        if not d["pageInfo"]["hasNextPage"]:
            break
        cursor = d["pageInfo"]["endCursor"]
    return out


async def _update_shopify_cbm(client, inv_id: str, cbm: float):
    mutation = """
    mutation($id: ID!, $input: InventoryItemInput!) {
      inventoryItemUpdate(id: $id, input: $input) {
        inventoryItem { id measurement { weight { value } } }
        userErrors { field message }
      }
    }"""
    variables = {"id": inv_id, "input": {"measurement": {"weight": {"value": cbm, "unit": "KILOGRAMS"}}}}
    r = await client.post(GQL_URL, headers=_shopify_headers(),
                          json={"query": mutation, "variables": variables}, timeout=20)
    r.raise_for_status()
    errs = (r.json().get("data") or {}).get("inventoryItemUpdate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(str(errs))


def _alert_body(summary: dict) -> str:
    lines = [
        "NED Freight — Cin7 → Shopify CBM sync report",
        f"Mode: {'DRY-RUN' if summary['dry_run'] else 'LIVE'}",
        f"Updated in Shopify: {len(summary['updated'])}",
        f"Skipped (Cin7 value out of range >{CBM_SYNC_MAX} m3 — FIX IN CIN7): {len(summary['bad_cin7'])}",
        f"Errors writing to Shopify: {len(summary['errors'])}",
        "",
    ]
    if summary["bad_cin7"]:
        lines.append("Bad Cin7 CBM values (not synced — please correct in Cin7):")
        for r in summary["bad_cin7"]:
            lines.append(f"  - {r['sku']}  {r['product']}  Cin7={r['cin7_cbm']} m3 (Shopify still {r['shopify_cbm']})")
        lines.append("")
    if summary["errors"]:
        lines.append("Shopify write errors:")
        for r in summary["errors"]:
            lines.append(f"  - {r['sku']}  {r['product']}  -> {r['error']}")
        lines.append("")
    if summary["updated"]:
        lines.append("Updated (Cin7 -> Shopify):")
        for r in summary["updated"][:50]:
            lines.append(f"  - {r['sku']}  {r['product']}  {r['shopify_cbm']} -> {r['cin7_cbm']}")
        if len(summary["updated"]) > 50:
            lines.append(f"  ... +{len(summary['updated']) - 50} more")
    return "\n".join(lines)


async def run_sync(dry_run: bool = True, send_alert: bool = True) -> dict:
    """
    Sync plausible Cin7 CBMs into Shopify. Returns a summary dict. Emails ALERT_TO when
    there are out-of-range Cin7 values or Shopify write errors (unless send_alert=False).
    """
    summary = {"dry_run": dry_run, "updated": [], "bad_cin7": [], "errors": [],
               "unchanged": 0, "no_shopify_match": 0}
    async with httpx.AsyncClient() as client:
        cin7 = await fetch_cin7_cbms(client)
        shop = await fetch_shopify_cbms(client)

        for sku_l, c in cin7.items():
            cbm = c["cbm"]
            if cbm <= EPS:
                continue                                   # no Cin7 CBM to push
            s = shop.get(sku_l)
            if not s:
                summary["no_shopify_match"] += 1           # Cin7-only SKU (wholesale/discontinued)
                continue
            if cbm > CBM_SYNC_MAX:                          # junk (31.0 etc.) — never push
                summary["bad_cin7"].append({"sku": c["sku"], "product": c["product"],
                                            "cin7_cbm": cbm, "shopify_cbm": s["cbm"]})
                continue
            if abs(cbm - s["cbm"]) <= EPS:
                summary["unchanged"] += 1
                continue
            row = {"sku": c["sku"], "product": s["product"], "cin7_cbm": cbm, "shopify_cbm": s["cbm"]}
            if dry_run:
                summary["updated"].append(row)
                continue
            try:
                await _update_shopify_cbm(client, s["inv_id"], cbm)
                summary["updated"].append(row)
            except Exception as e:
                summary["errors"].append({**row, "error": str(e)})
                log.error("CBM write failed for %s: %s", c["sku"], e)

    # Alert on anything that needs a human: bad Cin7 data or write errors.
    if send_alert and (summary["bad_cin7"] or summary["errors"]):
        subject = f"[NED Freight] CBM sync: {len(summary['errors'])} error(s), " \
                  f"{len(summary['bad_cin7'])} bad Cin7 value(s)"
        await emailer.send_email(ALERT_TO, subject, _alert_body(summary))

    return summary


def _load_dotenv():
    env = Path(__file__).parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _load_dotenv()
    # re-read module globals after loading .env
    CIN7_USERNAME = os.environ.get("CIN7_USERNAME", "")
    CIN7_API_KEY = os.environ.get("CIN7_API_KEY", "")
    SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")
    globals().update(CIN7_USERNAME=CIN7_USERNAME, CIN7_API_KEY=CIN7_API_KEY,
                     SHOPIFY_ADMIN_TOKEN=SHOPIFY_ADMIN_TOKEN)
    live = "--live" in sys.argv
    # never send email from a manual dry-run unless explicitly asked
    res = asyncio.run(run_sync(dry_run=not live, send_alert=("--alert" in sys.argv)))
    print(f"\nMode: {'LIVE' if live else 'DRY-RUN'}")
    print(f"  updated:         {len(res['updated'])}")
    print(f"  bad_cin7 (>4m3): {len(res['bad_cin7'])}")
    print(f"  errors:          {len(res['errors'])}")
    print(f"  unchanged:       {res['unchanged']}")
    print(f"  no_shopify_match:{res['no_shopify_match']}")
    for r in res["updated"][:20]:
        print(f"    {r['sku'][:16]:17}{r['product'][:24]:25} {r['shopify_cbm']} -> {r['cin7_cbm']}")
    if res["bad_cin7"]:
        print("  bad Cin7 (skipped):")
        for r in res["bad_cin7"][:20]:
            print(f"    {r['sku'][:16]:17}{r['product'][:24]:25} Cin7={r['cin7_cbm']}")
