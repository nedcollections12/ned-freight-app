"""
Assign Shopify products to their oversized delivery profiles based on CBM categories.
Matches by SKU (style code) first, then by product title.

Usage:
    python scripts/assign_product_profiles.py [--dry-run]
"""

import asyncio, json, os, sys
from pathlib import Path
import httpx

SHOP  = os.environ.get("SHOPIFY_SHOP", "nedcollections.myshopify.com")
TOKEN = os.environ.get("SHOPIFY_TOKEN", "")
if not TOKEN:
    # Fall back to token file used by server.py
    token_file = Path(__file__).parent.parent / "data" / "shopify_token.json"
    if token_file.exists():
        import json as _json
        TOKEN = _json.loads(token_file.read_text()).get("token", "")
if not TOKEN:
    print("ERROR: No token found. Set SHOPIFY_TOKEN env var or ensure data/shopify_token.json exists.")
    sys.exit(1)
GQL   = f"https://{SHOP}/admin/api/2024-04/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

DRY_RUN = "--dry-run" in sys.argv

# ── Spreadsheet data: profile name → (product names, style codes) ─────────────
# Sources: NED Collections Oversized product catagories.xlsx
# For matching: style codes searched against SKU; product names against title.
PROFILE_PRODUCTS = {
    "Oversized 0.16-0.25m3": {
        "names":  ["Beu Chair","Boon Pouf Set","Broome Bedside","Carros Coffee Table","Circular Mirror",
                   "Dawn Long Ottoman","Dawn Ottoman","Elm Bedside","Kahn Table","Lennox Bedside",
                   "Lume Ottoman","Malli Bar Stool","Matte Table","Niche Side Table","Nodi Chair",
                   "Otto Bedside","Peninsula Ottoman","RT-1260","RT-1262","Read Dining Chair",
                   "Story 2 Seater","Surge Floor Lamp","Tres Chair","Tumo Chair"],
        "skus":   ["24101401","555","8101","BS-1344A - Black","HE6262","MC-7495CH","MC-9523CH",
                   "T301","TC092","WD-1797A-Blk","WF-1E019","WF-1E029","WHF-1E003",
                   "ottoman-A","ottoman-B"],
    },
    "Oversized 0.25-0.50m3": {
        "names":  ["Alan Chair","Alister Bench","Arch Floor Mirror","Arlo Chair","Bay Chair",
                   "Beu Bar Stool","Canta Coffee Table","Drift Bedhead","Dundee Bench",
                   "Elm Coffee Table","Elm Sideboard","Elm TV Unit","Fara Dining Table",
                   "Fara Extendable Dining Table","Gaudi Bench","George Coffee Table","Halo Chair",
                   "Hickory Large Ottoman","Jay Table","Le Bons Bench","Lou Chair","Lucca Dining Table",
                   "Luma Coffee Table","Mia Occasional Chair","Milan Coffee Table","Nova Coffee Table",
                   "Oki Low Table","Osca Table","Otte Dining Table","Porto Dining Table",
                   "Read Bar Stool","Tanner Chair","Tuscany Dining Chair"],
        "skus":   ["3902","553","A460A","Alice-DT-180","Alice-EX","BS-1797B-Blk","C303","C304",
                   "DC-S197V1","Dundee","GINA-CTR","HE1288","HE3719","LDC-295A","MC-7565BC",
                   "MC-7632CH-A","MC-7790CH","RT-S124A","RT-S260","TC091","TD077","TD097-220",
                   "TITONI-CT"],
    },
    "Oversized 0.50-0.75m3": {
        "names":  ["Bayside Chair","Dawn Arm Chair","Dossier Sofa 1 Seater",
                   "Dossier Sofa 1 Seater Left Arm","Dossier Sofa 1 Seater Right Arm",
                   "Elm TV Unit","French Swivel Chair","French Swivel Chair Wooden Trim",
                   "Grace Sofa 1 Seater","Halo Chair","Indo Buffet","Kuva Lounge Chair",
                   "LDB-170","Lune Bed","Lune Bedhead","Niche Dining Table","Nord Chair",
                   "Otley Dining Table","Rue Bistro Chair","Sable TV Unit","Tanner Chair",
                   "Theo Swivel Chair","Vero Table"],
        "skus":   ["34354-FK-1.5AL-F","34422-FK-1.5AL-F","34422-FK-1.5PL-F","34422-FK-1.5PR-F",
                   "A391A","A974","French Chair","HBC247001","HE1288","HSF255001","MC-7764LC",
                   "MC-7779DT","MC-7805BU","WF-1F019A"],
    },
    "Oversized 0.75-1.00m3": {
        "names":  ["Cloudy Buffet","Dossier Sofa 1 Seater Chaise","Drift Swivel Chair",
                   "Elm Sideboard","Leo Sideboard","Niche Dining Table","Noel Buffet"],
        "skus":   ["34422-FK-CS-F","Drift Chair","HE3719","MC-7800BU","MC-7805BU","NC06-J-1"],
    },
    "Oversized 1.00-1.25m3": {
        "names":  ["Dossier Sofa Corner","Drift Chair","Drift Left Arm Module","Drift Middle Module",
                   "Drift Right Arm Module","Faker Dining Table","Grace Sofa 1 Seater Chaise",
                   "Grace Sofa 1 Seater Left Arm","Grace Sofa 1 Seater Right Arm",
                   "Grace Sofa Corner","Lume Sofa"],
        "skus":   ["34354-FK-1.5PL-F","34354-FK-1.5PR-F","34354-FK-CNR-F","34354-FK-CS-F",
                   "34354-FK-CS-F","34422-FK-C-F","OSF-1582-Left-SP","OSF-1582-Middle-B",
                   "OSF-1582-Right-E"],
    },
    "Oversized 1.25-1.50m3": {
        "names":  ["Alice Dining Table","Fleur Sofa"],
        "skus":   ["ROSDT"],
    },
    "Oversized 1.50-1.75m3": {
        "names":  ["Verra Armless Sofa"],
        "skus":   [],
    },
    "Oversized 1.75-2.00m3": {
        "names":  ["Harlow Sofa","Hendrix Sofa","Milana Sofa"],
        "skus":   ["MC-7600SF","Mingle Sofa"],
    },
    "Oversized 2.00-2.50m3": {
        "names":  ["Dyne Sofa"],
        "skus":   ["S2588"],
    },
    "Oversized 2.50m3+": {
        "names":  ["Dyne Sofa","Montana Sofa"],
        "skus":   ["34394-FK-F","S2588"],
    },
}


async def gql(client, query, variables=None):
    r = await client.post(GQL, headers=HEADERS,
                          json={"query": query, "variables": variables or {}})
    return r.json()


async def fetch_all_products(client):
    """Fetch all products with variant IDs, SKUs, and product titles (paginated)."""
    query = """
    query($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            variants(first: 100) {
              edges {
                node { id sku }
              }
            }
          }
        }
      }
    }
    """
    products = []
    cursor = None
    page = 0
    while True:
        page += 1
        data = await gql(client, query, {"cursor": cursor})
        if data.get("errors"):
            print(f"  GQL errors: {data['errors']}")
            break
        pdata = data.get("data", {}).get("products", {})
        for edge in pdata.get("edges", []):
            node = edge["node"]
            products.append(node)
        page_info = pdata.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]
        print(f"  Fetched page {page}, {len(products)} products so far...")
    return products


def build_lookup(products):
    """Build sku_map and title_map from fetched products."""
    # sku  -> list of variant GIDs
    # title (normalised) -> list of variant GIDs
    sku_map   = {}
    title_map = {}

    for product in products:
        title_key = product["title"].strip().lower()
        for ve in product["variants"]["edges"]:
            variant_id = ve["node"]["id"]
            sku = (ve["node"]["sku"] or "").strip()

            if sku:
                sku_lower = sku.lower()
                sku_map.setdefault(sku_lower, []).append(variant_id)

            # All variants for a product share the product title match
            title_map.setdefault(title_key, []).append(variant_id)

    return sku_map, title_map


async def fetch_delivery_profiles(client):
    """Return dict: profile name -> profile GID."""
    q = """{ deliveryProfiles(first: 30) { edges { node { id name } } } }"""
    data = await gql(client, q)
    return {
        e["node"]["name"]: e["node"]["id"]
        for e in data.get("data", {}).get("deliveryProfiles", {}).get("edges", [])
    }


async def assign_variants_to_profile(client, profile_id, profile_name, variant_ids):
    if not variant_ids:
        print(f"  [{profile_name}] No variants to assign — skipping.")
        return

    if DRY_RUN:
        print(f"  [DRY RUN] Would assign {len(variant_ids)} variants to '{profile_name}'")
        return

    mutation = """
    mutation deliveryProfileUpdate($id: ID!, $profile: DeliveryProfileInput!) {
      deliveryProfileUpdate(id: $id, profile: $profile) {
        profile { id name }
        userErrors { field message }
      }
    }
    """
    # Shopify expects variantsToAssociate as list of variant GIDs
    variables = {
        "id": profile_id,
        "profile": {"variantsToAssociate": variant_ids}
    }
    data = await gql(client, mutation, variables)
    errs = (data.get("data") or {}).get("deliveryProfileUpdate", {}).get("userErrors", [])
    if errs:
        print(f"  [{profile_name}] ERRORS: {errs}")
    else:
        print(f"  [{profile_name}] ✓ Assigned {len(variant_ids)} variants")


async def main():
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Shop: {SHOP}\n")

    async with httpx.AsyncClient(timeout=60) as client:
        # 1. Fetch all products
        print("Fetching all Shopify products...")
        products = await fetch_all_products(client)
        print(f"Total products fetched: {len(products)}")

        sku_map, title_map = build_lookup(products)
        print(f"Unique SKUs indexed: {len(sku_map)}")
        print(f"Unique product titles indexed: {len(title_map)}\n")

        # 2. Fetch delivery profile IDs
        print("Fetching delivery profiles...")
        profile_map = await fetch_delivery_profiles(client)
        oversized_profiles = {k: v for k, v in profile_map.items() if "oversized" in k.lower()}
        print(f"Found {len(oversized_profiles)} oversized profiles\n")

        # 3. Match and assign
        for profile_name, sources in PROFILE_PRODUCTS.items():
            profile_id = oversized_profiles.get(profile_name)
            if not profile_id:
                print(f"  [{profile_name}] PROFILE NOT FOUND in Shopify — skipping")
                continue

            variant_ids = set()
            matched_skus    = []
            matched_titles  = []
            unmatched_skus  = []
            unmatched_names = []

            # Match by SKU
            for sku in sources["skus"]:
                found = sku_map.get(sku.lower())
                if found:
                    variant_ids.update(found)
                    matched_skus.append(sku)
                else:
                    unmatched_skus.append(sku)

            # Match by product name (title) for all names in the list
            for name in sources["names"]:
                found = title_map.get(name.lower())
                if found:
                    variant_ids.update(found)
                    matched_titles.append(name)
                else:
                    unmatched_names.append(name)

            print(f"[{profile_name}]")
            print(f"  SKUs matched: {len(matched_skus)}/{len(sources['skus'])}  "
                  f"Titles matched: {len(matched_titles)}/{len(sources['names'])}")
            if unmatched_skus:
                print(f"  Unmatched SKUs:   {unmatched_skus}")
            if unmatched_names:
                print(f"  Unmatched titles: {unmatched_names}")

            await assign_variants_to_profile(client, profile_id, profile_name, list(variant_ids))
            print()


asyncio.run(main())
