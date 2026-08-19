"""
Forward CBM sync: Cin7 product `volume` -> Shopify variant weight (= CBM the freight
app reads at checkout).

Fixes "new variant defaults to 0 CBM -> no freight rate at checkout": when a plausible
CBM exists in Cin7 and the Shopify variant has NO CBM yet, fill it. Matched by SKU
(Cin7 option `code` == Shopify variant `sku`).

CBM source is the product-level `volume` field, NOT option `optionWeight` — the latter
holds kg for some products and caused freight overcharges (lamps synced at ~0.8 m3).

Guardrails (both Cin7 fields carry junk defaults — 0.4 volume / 31 weight):
  * FILL-ONLY: only writes when Shopify CBM is 0/missing; NEVER overwrites an existing
    value (so a junk 0.4/31 can't clobber a good Shopify CBM).
  * Only writes when 0 < cin7_cbm <= CBM_SYNC_MAX (default 4.0 m3).
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


async def _request_with_retry(client, method, url, *, max_attempts=5, **kwargs):
    """HTTP with backoff on 429 / 5xx — Cin7 and Shopify both rate-limit (429)."""
    last = None
    for attempt in range(max_attempts):
        r = await client.request(method, url, **kwargs)
        if r.status_code == 429 or r.status_code >= 500:
            last = r
            ra = (r.headers.get("Retry-After") or "").strip()
            wait = float(ra) if ra.replace(".", "", 1).isdigit() else min(2 ** attempt, 30)
            log.warning("%s %s -> %s; retry %d/%d in %.1fs", method, url.rsplit("/", 1)[-1],
                        r.status_code, attempt + 1, max_attempts, wait)
            await asyncio.sleep(wait)
            continue
        r.raise_for_status()
        return r
    if last is not None:
        last.raise_for_status()
    raise RuntimeError("request failed without a response")


async def fetch_cin7_cbms(client) -> dict:
    """
    {sku_lower: {'sku','product','cbm'}} — CBM read from the product-level `volume`
    field (Cin7 convention: volume = CBM m3, weight = kg). NOT `optionWeight`, which is
    an unreliable legacy field (holds kg for some products) and caused freight overcharges.
    """
    out, page = {}, 1
    while True:
        r = await _request_with_retry(client, "GET", "https://api.cin7.com/api/v1/Products",
                                      params={"page": page, "rows": 250, "fields": "id,name,volume,productOptions"},
                                      headers=_cin7_auth(), timeout=40)
        batch = r.json()
        if not batch:
            break
        for p in batch:
            cbm = float(p.get("volume") or 0)          # product-level CBM applies to all its options
            for o in (p.get("productOptions") or []):
                sku = (o.get("code") or "").strip()
                if sku:
                    out[sku.lower()] = {"sku": sku, "product": p.get("name", ""), "cbm": cbm}
        if len(batch) < 250:
            break
        page += 1
        await asyncio.sleep(0.6)                        # stay under Cin7's rate limit
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
        r = await _request_with_retry(client, "POST", GQL_URL, headers=_shopify_headers(),
                                      json={"query": query, "variables": {"cursor": cursor}}, timeout=40)
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
    r = await _request_with_retry(client, "POST", GQL_URL, headers=_shopify_headers(),
                                  json={"query": mutation, "variables": variables}, timeout=20)
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
    if summary.get("fatal_error"):
        lines.append(f"RUN FAILED before completing: {summary['fatal_error']}")
        lines.append("(No Shopify changes were made this run. It will retry next cycle.)")
        lines.append("")
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
               "unchanged": 0, "no_shopify_match": 0, "kept_existing": 0, "fatal_error": None}
    try:
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
            # FILL-ONLY: only populate a missing/zero Shopify CBM (the new-product case).
            # Never overwrite an existing value — both Cin7 fields carry junk defaults
            # (0.4 volume / 31 weight) that would clobber good data.
            if s["cbm"] > EPS:
                summary["kept_existing"] += 1
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

    except Exception as e:
        # Fetch/connection failure (e.g. Cin7 429 after retries) — don't crash the
        # scheduler or 500 the endpoint; record it and make sure it's not silent.
        summary["fatal_error"] = str(e)
        log.error("CBM sync run failed: %s", e)

    # Alert on anything that needs a human: a fatal failure, bad Cin7 data, or write errors.
    if send_alert and (summary["fatal_error"] or summary["bad_cin7"] or summary["errors"]):
        subject = f"[NED Freight] CBM sync: " + (
            "RUN FAILED" if summary["fatal_error"]
            else f"{len(summary['errors'])} error(s), {len(summary['bad_cin7'])} bad Cin7 value(s)")
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
    print(f"  kept_existing:   {res['kept_existing']}   (fill-only: never overwrites a value)")
    print(f"  no_shopify_match:{res['no_shopify_match']}")
    for r in res["updated"][:20]:
        print(f"    {r['sku'][:16]:17}{r['product'][:24]:25} {r['shopify_cbm']} -> {r['cin7_cbm']}")
    if res["bad_cin7"]:
        print("  bad Cin7 (skipped):")
        for r in res["bad_cin7"][:20]:
            print(f"    {r['sku'][:16]:17}{r['product'][:24]:25} Cin7={r['cin7_cbm']}")
