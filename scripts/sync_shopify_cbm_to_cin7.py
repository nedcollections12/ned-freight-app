"""
Seed Cin7 from Shopify: where a Shopify active variant HAS a CBM but the matching
Cin7 option's `optionWeight` is blank/zero, write Shopify's CBM up to Cin7.

Direction: Shopify -> Cin7 (the reverse of the checkout sync). Used once to make
Cin7 an accurate master before the ongoing Cin7 -> Shopify sync is turned on.

SAFETY:
  * DRY-RUN by default — prints/writes the plan and changes NOTHING. Pass --live to write.
  * Live writes re-fetch each product and PUT back its FULL productOptions array with
    only the target option's optionWeight changed, so no other option/field is touched.
  * --limit N   process only the first N products (use 1 for a single-record test).
  * Verifies each write by re-reading the option afterwards.

Usage:
    python scripts/sync_shopify_cbm_to_cin7.py                # dry-run, all
    python scripts/sync_shopify_cbm_to_cin7.py --live --limit 1   # write ONE, verify
    python scripts/sync_shopify_cbm_to_cin7.py --live            # write all (after confirming)
"""

import csv
import os
import sys
import time
from base64 import b64encode
from pathlib import Path

import httpx

ROOT = Path(__file__).parent.parent
EPS = 0.001
# Skip suspiciously tiny Shopify placeholders (e.g. 0.01) so we don't seed junk into Cin7.
MIN_MEANINGFUL_CBM = 0.02


def _load_env():
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


_load_env()
CIN7_USERNAME = os.environ.get("CIN7_USERNAME", "")
CIN7_API_KEY = os.environ.get("CIN7_API_KEY", "")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "nedcollections.myshopify.com")
SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")

LIVE = "--live" in sys.argv
LIMIT = None
if "--limit" in sys.argv:
    LIMIT = int(sys.argv[sys.argv.index("--limit") + 1])


def _cin7_auth():
    return {"Authorization": "Basic " + b64encode(f"{CIN7_USERNAME}:{CIN7_API_KEY}".encode()).decode()}


def fetch_cin7_products(client):
    """Return list of full product dicts (id, name, productOptions with id/code/optionWeight)."""
    products, page = [], 1
    while True:
        r = client.get("https://api.cin7.com/api/v1/Products",
                       params={"page": page, "rows": 250, "fields": "id,name,productOptions"},
                       headers=_cin7_auth(), timeout=40)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        products.extend(batch)
        if len(batch) < 250:
            break
        page += 1
    return products


def fetch_shopify_cbm_by_sku(client):
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    query = """
    query($cursor: String) {
      productVariants(first: 250, after: $cursor, query: "status:ACTIVE") {
        edges { node { sku inventoryItem { measurement { weight { value } } } } }
        pageInfo { hasNextPage endCursor }
      }
    }"""
    out, cursor = {}, None
    while True:
        r = client.post(url, headers=headers, json={"query": query, "variables": {"cursor": cursor}}, timeout=40)
        r.raise_for_status()
        d = r.json()["data"]["productVariants"]
        for e in d["edges"]:
            v = e["node"]
            sku = (v.get("sku") or "").strip()
            if not sku:
                continue
            w = (v["inventoryItem"]["measurement"] or {}).get("weight")
            out[sku.lower()] = float((w["value"] if w else 0) or 0)
        if not d["pageInfo"]["hasNextPage"]:
            break
        cursor = d["pageInfo"]["endCursor"]
    return out


def put_product_option_weights(client, product_id, options):
    """PUT the product's full options array back with updated optionWeight values."""
    body = [{
        "id": product_id,
        "productOptions": [{"id": o["id"], "optionWeight": o["optionWeight"]} for o in options],
    }]
    r = client.put("https://api.cin7.com/api/v1/Products", headers=_cin7_auth(), json=body, timeout=40)
    r.raise_for_status()
    return r.json()


def main():
    with httpx.Client() as client:
        print("Fetching Shopify CBMs...")
        shop = fetch_shopify_cbm_by_sku(client)
        print(f"  {len(shop)} active Shopify variants")
        print("Fetching Cin7 products...")
        products = fetch_cin7_products(client)
        print(f"  {len(products)} Cin7 products")

        # Build the plan: per product, which options need seeding from Shopify.
        plan = []          # rows for the report
        prod_updates = []  # (product, [full option list with new weights])
        for p in products:
            opts = p.get("productOptions") or []
            changed = False
            new_opts = []
            for o in opts:
                sku = (o.get("code") or "").strip()
                cur = float(o.get("optionWeight") or 0)
                shop_cbm = shop.get(sku.lower())
                target = cur
                if sku and cur <= EPS and shop_cbm is not None and shop_cbm >= MIN_MEANINGFUL_CBM:
                    target = round(shop_cbm, 4)
                    changed = True
                    plan.append({"sku": sku, "product": p.get("name", ""),
                                 "cin7_current": cur, "shopify_cbm": shop_cbm, "new_cin7": target})
                new_opts.append({"id": o["id"], "optionWeight": target})
            if changed:
                prod_updates.append((p, new_opts))

        # Report
        rep = ROOT / "cin7_backfill_plan.csv"
        with rep.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["sku", "product", "cin7_current", "shopify_cbm", "new_cin7"])
            w.writeheader()
            w.writerows(plan)
        print(f"\nPlan: {len(plan)} option(s) across {len(prod_updates)} product(s) would be seeded Shopify -> Cin7")
        print(f"Written to {rep.name}")
        for r in plan[:15]:
            print(f"  {r['sku'][:16]:17}{r['product'][:24]:25} Cin7 {r['cin7_current']} -> {r['new_cin7']}")
        if len(plan) > 15:
            print(f"  ... +{len(plan) - 15} more (see {rep.name})")

        if not LIVE:
            print("\nDRY-RUN — nothing written to Cin7. Re-run with --live (and optionally --limit 1) to apply.")
            return

        # LIVE
        targets = prod_updates[:LIMIT] if LIMIT else prod_updates
        print(f"\nLIVE: writing {len(targets)} product(s) to Cin7...")
        ok = fail = 0
        for p, new_opts in targets:
            try:
                put_product_option_weights(client, p["id"], new_opts)
                ok += 1
                print(f"  ✓ {p.get('name')[:30]} ({len(new_opts)} options)")
            except Exception as e:
                fail += 1
                print(f"  ✗ {p.get('name')[:30]}: {e}")
            time.sleep(0.4)  # stay under Cin7 rate limits
        print(f"\nDone. ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
