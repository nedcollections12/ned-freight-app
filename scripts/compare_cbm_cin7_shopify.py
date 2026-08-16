"""
Compare per-variant CBM between Cin7 (source of truth) and Shopify (freight app reads this).

Cin7 stores CBM in the option-level `optionWeight` field (NED convention: the weight
field carries CBM in m3). Shopify stores it in inventoryItem.measurement.weight (kg = CBM).
Match key: Cin7 option `code` == Shopify variant `sku`.

Read-only. Produces a CSV of every SKU with both values, the difference, and a status flag
so staff can review before the live sync overwrites Shopify from Cin7.

Usage:
    python scripts/compare_cbm_cin7_shopify.py [--out cbm_comparison.csv]
Requires env: CIN7_USERNAME, CIN7_API_KEY, SHOPIFY_STORE, SHOPIFY_ADMIN_TOKEN (loaded from .env).
"""

import csv
import os
import sys
from base64 import b64encode
from pathlib import Path

import httpx

ROOT = Path(__file__).parent.parent


def _load_env():
    """Minimal .env loader (avoids a dependency); real deploy uses Render env vars."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip())


_load_env()

CIN7_USERNAME = os.environ.get("CIN7_USERNAME", "")
CIN7_API_KEY = os.environ.get("CIN7_API_KEY", "")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "nedcollections.myshopify.com")
SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")

# CBM values within this tolerance are treated as equal (Shopify rounds tiny values).
EPS = 0.001


def _cin7_auth():
    token = b64encode(f"{CIN7_USERNAME}:{CIN7_API_KEY}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_cin7_cbms(client):
    """Return {sku_lower: {'sku', 'product', 'option', 'cbm'}} from Cin7 option optionWeight."""
    out = {}
    page = 1
    while True:
        r = client.get(
            "https://api.cin7.com/api/v1/Products",
            params={"page": page, "rows": 250, "fields": "id,name,productOptions"},
            headers=_cin7_auth(),
            timeout=40,
        )
        r.raise_for_status()
        products = r.json()
        if not products:
            break
        for p in products:
            pname = (p.get("name") or "").strip()
            for o in (p.get("productOptions") or []):
                sku = (o.get("code") or o.get("productOptionCode") or "").strip()
                if not sku:
                    continue
                opt_label = " / ".join(
                    x for x in (o.get("option1"), o.get("option2"), o.get("option3")) if x
                )
                out[sku.lower()] = {
                    "sku": sku,
                    "product": pname,
                    "option": opt_label,
                    "cbm": float(o.get("optionWeight") or 0),
                }
        if len(products) < 250:
            break
        page += 1
    return out


def fetch_shopify_cbms(client):
    """Return {sku_lower: {'sku', 'product', 'variant', 'inv_id', 'cbm'}} for active variants."""
    out = {}
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    query = """
    query($cursor: String) {
      productVariants(first: 250, after: $cursor, query: "status:ACTIVE") {
        edges { node {
          sku title
          product { title }
          inventoryItem { id measurement { weight { value } } }
        }}
        pageInfo { hasNextPage endCursor }
      }
    }
    """
    cursor = None
    while True:
        r = client.post(url, headers=headers, json={"query": query, "variables": {"cursor": cursor}}, timeout=40)
        r.raise_for_status()
        data = r.json()["data"]["productVariants"]
        for e in data["edges"]:
            v = e["node"]
            sku = (v.get("sku") or "").strip()
            if not sku:
                continue
            w = (v["inventoryItem"]["measurement"] or {}).get("weight")
            out[sku.lower()] = {
                "sku": sku,
                "product": v["product"]["title"],
                "variant": v["title"],
                "inv_id": v["inventoryItem"]["id"],
                "cbm": float((w["value"] if w else 0) or 0),
            }
        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["pageInfo"]["endCursor"]
    return out


def classify(cin7, shop):
    """Status for one SKU given its Cin7 and Shopify records (either may be None)."""
    c = cin7["cbm"] if cin7 else None
    s = shop["cbm"] if shop else None
    if cin7 and not shop:
        return "MISSING_IN_SHOPIFY"          # in Cin7 but no matching active Shopify variant
    if shop and not cin7:
        return "MISSING_IN_CIN7"             # Shopify variant with no Cin7 match (can't source CBM)
    if (c or 0) <= 0 and (s or 0) <= 0:
        return "BOTH_ZERO"                   # no CBM anywhere — needs entry in Cin7
    if (c or 0) <= 0:
        return "CIN7_ZERO"                   # Shopify has a value, Cin7 doesn't — sync would NOT overwrite to 0
    if abs((c or 0) - (s or 0)) <= EPS:
        return "MATCH"
    return "DIFFERS"                         # sync will update Shopify -> Cin7 value


def main():
    out_path = ROOT / "cbm_comparison.csv"
    if "--out" in sys.argv:
        out_path = Path(sys.argv[sys.argv.index("--out") + 1])

    for name, val in [("CIN7_API_KEY", CIN7_API_KEY), ("SHOPIFY_ADMIN_TOKEN", SHOPIFY_ADMIN_TOKEN)]:
        if not val:
            print(f"ERROR: {name} not set (check .env)")
            sys.exit(1)

    with httpx.Client() as client:
        print("Fetching Cin7 options...")
        cin7 = fetch_cin7_cbms(client)
        print(f"  {len(cin7)} Cin7 options (by SKU)")
        print("Fetching Shopify variants...")
        shop = fetch_shopify_cbms(client)
        print(f"  {len(shop)} active Shopify variants (by SKU)")

    all_skus = sorted(set(cin7) | set(shop))
    rows = []
    counts = {}
    for k in all_skus:
        c, s = cin7.get(k), shop.get(k)
        status = classify(c, s)
        counts[status] = counts.get(status, 0) + 1
        c_cbm = c["cbm"] if c else ""
        s_cbm = s["cbm"] if s else ""
        diff = (round(c["cbm"] - s["cbm"], 4) if (c and s) else "")
        rows.append({
            "sku": (c or s)["sku"],
            "product": (c or s).get("product", ""),
            "option": (c or {}).get("option", "") or (s or {}).get("variant", ""),
            "cin7_cbm": c_cbm,
            "shopify_cbm": s_cbm,
            "diff_cin7_minus_shopify": diff,
            "status": status,
        })

    # DIFFERS first (the actionable rows), then the rest
    order = {"DIFFERS": 0, "MISSING_IN_SHOPIFY": 1, "CIN7_ZERO": 2, "BOTH_ZERO": 3,
             "MISSING_IN_CIN7": 4, "MATCH": 5}
    rows.sort(key=lambda r: (order.get(r["status"], 9), str(r["product"])))

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"\nWrote {len(rows)} rows -> {out_path}")
    print("Summary by status:")
    for st in sorted(counts, key=lambda x: order.get(x, 9)):
        print(f"  {st:20} {counts[st]}")


if __name__ == "__main__":
    main()
