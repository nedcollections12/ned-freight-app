"""NED Freight App — FastAPI Server"""

import json, math, os, hmac, hashlib
from pathlib import Path
from typing import Optional
import uvicorn
import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from zones import detect_zone, get_oversized_zone

SHOPIFY_API_KEY    = os.environ.get("SHOPIFY_API_KEY", "")
SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET", "")
APP_URL            = os.environ.get("APP_URL", "https://ned-freight-app.onrender.com")
TOKEN_FILE         = Path(__file__).parent / "data" / "shopify_token.json"

BASE_DIR = Path(__file__).parent
RATES_FILE = BASE_DIR / "data" / "rates.json"
PRODUCTS_FILE = BASE_DIR / "data" / "oversized_products.json"

app = FastAPI(title="NED Freight App", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def load_rates():
    with open(RATES_FILE) as f: return json.load(f)

def save_rates(data):
    with open(RATES_FILE, "w") as f: json.dump(data, f, indent=2)

def load_products():
    return json.loads(PRODUCTS_FILE.read_text()) if PRODUCTS_FILE.exists() else {}

def save_products(data):
    PRODUCTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PRODUCTS_FILE.write_text(json.dumps(data, indent=2))

def lookup_standard_rate(rates, zone_id, order_value):
    zone_data = next((z for z in rates["standard"]["zones"] if z["id"] == zone_id), None)
    if zone_data and zone_data.get("by_request"): return None
    zone_rates = rates["standard"]["rates"].get(zone_id)
    if not zone_rates: return None
    for i, tier in enumerate(rates["standard"]["tiers"]):
        if tier["min"] <= order_value <= tier["max"]:
            r = zone_rates[i]; return float(r) if r is not None else None
    return None

def lookup_oversized_rate(rates, oz_zone_id, cbm):
    zone_data = next((z for z in rates["oversized"]["zones"] if z["id"] == oz_zone_id), None)
    if zone_data and zone_data.get("by_request"): return None
    zone_rates = rates["oversized"]["rates"].get(oz_zone_id)
    if not zone_rates: return None
    cats = rates["oversized"]["categories"]
    for i, cat in enumerate(cats):
        if cat["min_cbm"] <= cbm < cat["max_cbm"]:
            r = zone_rates[i]; return float(r) if r is not None else None
    if cbm >= cats[-1]["min_cbm"]:
        r = zone_rates[-1]; return float(r) if r is not None else None
    return None

@app.post("/shopify/rates")
async def shopify_rates(request: Request):
    try: body = await request.json()
    except: raise HTTPException(400, "Invalid JSON")
    rate_request = body.get("rate", {})
    destination = rate_request.get("destination", {})
    items = rate_request.get("items", [])
    province = destination.get("province", "")
    city = destination.get("city", "")
    postcode = destination.get("zip", "")
    is_rural = str(destination.get("address2", "")).lower().strip() == "rural"
    std_zone = detect_zone(province=province, city=city, postcode=postcode)
    oz_zone = get_oversized_zone(std_zone)
    rates_data = load_rates()
    settings = rates_data["settings"]
    free_threshold = settings.get("free_freight_threshold", 7500)
    rural_surcharge = settings.get("rural_surcharge", 14)
    products = load_products()
    order_value = 0.0; total_cbm = 0.0; has_oversized = False
    for item in items:
        qty = int(item.get("quantity", 1))
        price = int(item.get("price", 0)) / 100.0
        order_value += price * qty
        cbm_each = None
        for key in [str(item.get("variant_id","")), str(item.get("product_id",""))]:
            if key in products: cbm_each = products[key].get("cbm"); break
        if cbm_each and float(cbm_each) > 0.160:
            has_oversized = True; total_cbm += float(cbm_each) * qty
    currency = settings.get("currency", "NZD")
    if order_value >= free_threshold:
        return {"rates": [{"service_name":"Free Freight","service_code":"FREE","total_price":"0","currency":currency}]}
    if has_oversized and total_cbm > 0:
        oz_rate = lookup_oversized_rate(rates_data, oz_zone, total_cbm)
        if oz_rate is not None:
            total = math.ceil(oz_rate + (rural_surcharge if is_rural else 0))
            return {"rates": [{"service_name":"Freight" + (" (Rural)" if is_rural else ""),"service_code":"OVERSIZED","total_price":str(int(total*100)),"currency":currency}]}
        return {"rates": [{"service_name":"Freight — Contact Us","service_code":"BY_REQUEST","total_price":"0","currency":currency}]}
    std_rate = lookup_standard_rate(rates_data, std_zone, order_value)
    if std_rate is not None:
        total = math.ceil(std_rate + (rural_surcharge if is_rural else 0))
        return {"rates": [{"service_name":"Freight" + (" (Rural)" if is_rural else ""),"service_code":"STANDARD","total_price":str(int(total*100)),"currency":currency}]}
    return {"rates": [{"service_name":"Freight — Contact Us","service_code":"BY_REQUEST","total_price":"0","currency":currency}]}

@app.get("/api/rates")
async def get_rates(): return load_rates()

@app.put("/api/rates")
async def update_rates(request: Request):
    save_rates(await request.json()); return {"status":"saved"}

@app.put("/api/settings")
async def update_settings(request: Request):
    body = await request.json(); rates = load_rates()
    rates["settings"].update(body); save_rates(rates); return {"status":"saved"}

@app.get("/api/products")
async def get_products(): return load_products()

@app.put("/api/products/{product_id}")
async def upsert_product(product_id: str, request: Request):
    products = load_products(); products[product_id] = await request.json()
    save_products(products); return {"status":"saved"}

@app.delete("/api/products/{product_id}")
async def delete_product(product_id: str):
    products = load_products()
    if product_id not in products: raise HTTPException(404, "Not found")
    del products[product_id]; save_products(products); return {"status":"deleted"}

@app.post("/api/upload/{rate_type}")
async def upload_rates(rate_type: str, file: UploadFile = File(...)):
    content = await file.read(); fname = file.filename or ""
    if fname.endswith(".csv"):
        import csv, io
        rows = list(csv.reader(io.StringIO(content.decode("utf-8-sig"))))
    elif fname.endswith((".xlsx",".xls")):
        import openpyxl, io
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        rows = [[str(c.value or "") for c in row] for row in wb.active.iter_rows()]
    else: raise HTTPException(400, "Use .csv or .xlsx")
    rates = load_rates(); rate_key = "standard" if rate_type == "standard" else "oversized"
    updated = []; errors = []
    for row in rows:
        if not row or not row[0] or row[0].lower() in ("zone_id","zone",""): continue
        zid = row[0].strip().lower()
        try: new = [float(v) if v.strip() not in ("","null","None") else None for v in row[1:11]]
        except ValueError as e: errors.append(f"{zid}: {e}"); continue
        if zid in rates[rate_key]["rates"]:
            rates[rate_key]["rates"][zid] = new; updated.append(zid)
        else: errors.append(f"Unknown zone: {zid}")
    save_rates(rates)
    return {"status":"ok","updated_zones":updated,"errors":errors}

@app.get("/api/test-rate")
async def test_rate(province:str="", city:str="", postcode:str="", order_value:float=0, cbm:float=0, is_rural:bool=False):
    rates_data = load_rates(); settings = rates_data["settings"]
    free_threshold = settings.get("free_freight_threshold",7500)
    rural_surcharge = settings.get("rural_surcharge",14) if is_rural else 0
    std_zone = detect_zone(province=province, city=city, postcode=postcode)
    oz_zone = get_oversized_zone(std_zone)
    result = {"zone":std_zone,"oz_zone":oz_zone,"order_value":order_value,"cbm":cbm,"is_rural":is_rural}
    if order_value >= free_threshold:
        result.update({"freight_charge":0,"service":"FREE"}); return result
    if cbm > 0.160:
        rate = lookup_oversized_rate(rates_data, oz_zone, cbm)
        result.update({"service":"OVERSIZED","base_rate":rate,"freight_charge":math.ceil(rate+rural_surcharge) if rate is not None else None})
    else:
        rate = lookup_standard_rate(rates_data, std_zone, order_value)
        result.update({"service":"STANDARD","base_rate":rate,"freight_charge":math.ceil(rate+rural_surcharge) if rate is not None else None})
    return result

@app.get("/shopify/install")
async def shopify_install(shop: str):
    scopes = "read_shipping,write_shipping"
    redirect_uri = f"{APP_URL}/shopify/callback"
    url = (f"https://{shop}/admin/oauth/authorize"
           f"?client_id={SHOPIFY_API_KEY}&scope={scopes}"
           f"&redirect_uri={redirect_uri}")
    return RedirectResponse(url)

@app.get("/shopify/callback")
async def shopify_callback(code: str, shop: str, request: Request):
    # Exchange code for permanent access token
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"https://{shop}/admin/oauth/access_token",
            json={"client_id": SHOPIFY_API_KEY,
                  "client_secret": SHOPIFY_API_SECRET,
                  "code": code}
        )
    data = r.json()
    token = data.get("access_token")
    if not token:
        raise HTTPException(400, f"Token exchange failed: {data}")

    # Persist token
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps({"shop": shop, "token": token}))

    # Register carrier service
    async with httpx.AsyncClient() as client:
        cs = await client.post(
            f"https://{shop}/admin/api/2024-04/carrier_services.json",
            headers={"X-Shopify-Access-Token": token,
                     "Content-Type": "application/json"},
            json={"carrier_service": {
                "name": "NED Freight",
                "callback_url": f"{APP_URL}/shopify/rates",
                "service_discovery": True
            }}
        )
    cs_data = cs.json()
    cs_id = cs_data.get("carrier_service", {}).get("id", "already exists")
    return HTMLResponse(
        f"<h2>✅ NED Freight connected!</h2>"
        f"<p>Shop: <b>{shop}</b></p>"
        f"<p>Carrier service ID: <b>{cs_id}</b></p>"
        f"<p>Callback URL: <code>{APP_URL}/shopify/rates</code></p>"
    )

@app.post("/api/sync-shopify-zones")
async def sync_shopify_zones():
    """Push 9 oversized freight zones + correct flat rates to Shopify (spreadsheet pricing)."""
    if not TOKEN_FILE.exists():
        raise HTTPException(400, "No Shopify token found. Please reinstall the app first.")

    token_data = json.loads(TOKEN_FILE.read_text())
    token = token_data["token"]
    shop  = token_data["shop"]

    headers  = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    gql_url  = f"https://{shop}/admin/api/2024-04/graphql.json"

    # ── 8-zone structure ─────────────────────────────────────────────────────
    # NOTE: Shopify's delivery profile system automatically merges ALL non-Canterbury
    # South Island provinces (MBH, NSN, TAS, WTC, OTA, STL) into a single geographic
    # unit called "South Island", regardless of how many separate zones are created
    # via the API. This is a confirmed Shopify platform limitation — even individual
    # single-province zones get merged.
    # Resolution: use ONE explicit "South Island" zone with the Lower SI (higher) rate
    # so Otago/Southland customers are never undercharged. Upper SI customers
    # (Marlborough/Nelson/Tasman/West Coast) pay the Lower SI rate — $10/tier more
    # than the spreadsheet rate. Canterbury remains a separate zone as normal.
    ZONE_DEFS = [
        {"name": "Christchurch",              "provinces": ["CAN"]},
        {"name": "South Island",              "provinces": ["MBH","NSN","TAS","WTC","OTA","STL"]},
        {"name": "NI Lower",                  "provinces": ["WGN"]},
        {"name": "Waikato",                   "provinces": ["WKO"]},
        {"name": "Bay of Plenty / Gisborne",  "provinces": ["BOP","GIS"]},
        {"name": "Taranaki / Wan / HB",       "provinces": ["TKI","MWT","HKB"]},
        {"name": "Auckland",                  "provinces": ["AUK"]},
        {"name": "Northland",                 "provinces": ["NTL"]},
    ]

    # Flat rates per zone per CBM tier (source: "Oversized Shopify Rates" tab)
    # Tier index: 0=0.16-0.25, 1=0.25-0.50, 2=0.50-0.75, 3=0.75-1.00, 4=1.00-1.25,
    #             5=1.25-1.50, 6=1.50-1.75, 7=1.75-2.00, 8=2.00-2.50, 9=2.50+
    # "South Island" uses Lower SI (Otago/Southland) rates — the higher of the two —
    # to ensure no undercharging for any South Island customer.
    ZONE_RATES = {
        "Christchurch":              [ 40,  40,  40,  40,  40,  45,  55,  60,  70,  85],
        "South Island":              [ 65,  65,  65,  85, 105, 130, 155, 175, 200, 245],
        "NI Lower":                  [ 65,  65,  75,  95, 120, 150, 175, 200, 230, 285],
        "Waikato":                   [ 65,  65,  85, 120, 150, 185, 220, 255, 285, 355],
        "Bay of Plenty / Gisborne":  [ 65,  65,  85, 120, 155, 185, 220, 255, 290, 355],
        "Taranaki / Wan / HB":       [ 65,  65, 110, 150, 195, 240, 285, 325, 370, 455],
        "Auckland":                  [ 65,  65,  65,  90, 115, 140, 165, 190, 215, 265],
        "Northland":                 [ 70,  75, 125, 170, 220, 265, 315, 365, 410, 510],
    }

    # Map starting CBM string in profile name → tier index
    TIER_MAP = {
        "0.16": 0, "0.25": 1, "0.50": 2, "0.75": 3, "1.00": 4,
        "1.25": 5, "1.50": 6, "1.75": 7, "2.00": 8, "2.50": 9,
    }

    # ── Step 1: fetch all delivery profiles ──────────────────────────────────
    query = """
    {
      deliveryProfiles(first: 20) {
        edges { node {
          id name
          profileLocationGroups {
            locationGroup { id }
            locationGroupZones(first: 30) {
              edges { node { zone { id name } } }
            }
          }
        }}
      }
    }
    """
    async with httpx.AsyncClient(timeout=30) as client:
        r    = await client.post(gql_url, headers=headers, json={"query": query})
        data = r.json()

    all_profiles = [
        e["node"]
        for e in data.get("data", {}).get("deliveryProfiles", {}).get("edges", [])
    ]
    oversized = [p for p in all_profiles if "oversized" in p["name"].lower()]

    if not oversized:
        return {
            "error": "No profiles with 'Oversized' in the name found",
            "profiles_found": [p["name"] for p in all_profiles]
        }

    mutation = """
    mutation deliveryProfileUpdate($id: ID!, $profile: DeliveryProfileInput!) {
      deliveryProfileUpdate(id: $id, profile: $profile) {
        profile { id name }
        userErrors { field message }
      }
    }
    """

    results = []

    for profile in oversized:
        profile_id   = profile["id"]
        profile_name = profile["name"]
        lgroups      = profile.get("profileLocationGroups", [])

        if not lgroups:
            results.append({"profile": profile_name, "error": "No location groups found"})
            continue

        lg    = lgroups[0]
        lg_id = lg["locationGroup"]["id"]
        existing_zone_ids = [
            e["node"]["zone"]["id"]
            for e in lg.get("locationGroupZones", {}).get("edges", [])
        ]

        # Determine CBM tier index from profile name
        tier_idx = None
        name_lower = profile_name.lower()
        for cbm_key, idx in TIER_MAP.items():
            if cbm_key in name_lower:
                tier_idx = idx
                break
        if tier_idx is None:
            results.append({"profile": profile_name, "skipped": True,
                            "reason": "No CBM tier found in name — manual setup required"})
            continue

        def make_zone_payload(zdef, rate):
            return {
                "zone": {
                    "name": zdef["name"],
                    "countries": [{
                        "code": "NZ",
                        "includeAllProvinces": False,
                        "provinces": [{"code": p} for p in zdef["provinces"]]
                    }]
                },
                "methodDefinitionsToCreate": [{
                    "name": "Oversized Freight",
                    "active": True,
                    "rateDefinition": {
                        "price": {"amount": str(float(rate)), "currencyCode": "NZD"}
                    }
                }]
            }

        # ── Single call: delete all existing zones + create all 12 new zones ───
        # SI provinces are now individual province zones (Marlborough, Nelson/Tasman,
        # West Coast, Otago, Southland) so Shopify treats each like it treats Canterbury —
        # keeping them separate rather than merging into a single "South Island" zone.
        all_zone_payloads = [make_zone_payload(z, ZONE_RATES[z["name"]][tier_idx]) for z in ZONE_DEFS]
        variables = {
            "id": profile_id,
            "profile": {
                "locationGroupsToUpdate": [{
                    "id": lg_id,
                    "zonesToCreate": all_zone_payloads,
                    "zonesToDelete": existing_zone_ids
                }]
            }
        }
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(gql_url, headers=headers,
                                  json={"query": mutation, "variables": variables})
            r1 = r.json()

        errs = (r1.get("data") or {}).get("deliveryProfileUpdate", {}).get("userErrors", [])
        results.append({
            "profile":       profile_name,
            "success":       len(errs) == 0,
            "tier_index":    tier_idx,
            "zones_created": len(ZONE_DEFS),
            "zones_deleted": len(existing_zone_ids),
            "errors":        errs,
        })

    return {"results": results, "profiles_processed": len(oversized)}


@app.get("/api/raw-profile")
async def raw_profile():
    """Diagnostic: raw GraphQL deliveryProfiles response + REST shipping zones."""
    if not TOKEN_FILE.exists():
        raise HTTPException(400, "No token.")
    token_data = json.loads(TOKEN_FILE.read_text())
    token, shop = token_data["token"], token_data["shop"]
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    gql_url = f"https://{shop}/admin/api/2024-04/graphql.json"

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. GraphQL deliveryProfiles
        gql_q = """{ deliveryProfiles(first: 20) { edges { node { id name } } } }"""
        r1 = await client.post(gql_url, headers=headers, json={"query": gql_q})
        gql_result = r1.json()

        # 2. REST delivery_profiles
        r2 = await client.get(
            f"https://{shop}/admin/api/2024-04/delivery_profiles.json",
            headers={"X-Shopify-Access-Token": token}
        )
        rest_result = r2.json()

        # 3. REST shipping_zones (older API)
        r3 = await client.get(
            f"https://{shop}/admin/api/2024-04/shipping_zones.json",
            headers={"X-Shopify-Access-Token": token}
        )
        shipping_zones = r3.json()

    return {
        "gql_delivery_profiles": gql_result,
        "rest_delivery_profiles": rest_result,
        "rest_shipping_zones_names": [z.get("name") for z in shipping_zones.get("shipping_zones", [])]
    }


@app.get("/api/check-zones")
async def check_zones():
    """Return zone names, province codes, and rates for every Oversized profile (GraphQL)."""
    if not TOKEN_FILE.exists():
        raise HTTPException(400, "No token.")
    token_data = json.loads(TOKEN_FILE.read_text())
    token, shop = token_data["token"], token_data["shop"]
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    gql_url = f"https://{shop}/admin/api/2024-04/graphql.json"

    query = """
    {
      deliveryProfiles(first: 20) {
        edges { node {
          id name
          profileLocationGroups {
            locationGroupZones(first: 30) {
              edges { node {
                zone {
                  id name
                  countries {
                    code { ... on CountryCode { countryCode } }
                    provinces { code }
                  }
                }
                methodDefinitions(first: 10) {
                  edges { node {
                    name
                    rateProvider {
                      ... on DeliveryRateDefinition {
                        price { amount }
                      }
                    }
                  }}
                }
              }}
            }
          }
        }}
      }
    }
    """
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(gql_url, headers=headers, json={"query": query})
        data = r.json()

    # Expose GQL errors if any
    if data.get("errors"):
        raise HTTPException(500, {"gql_errors": data["errors"]})

    out = []
    for edge in data.get("data", {}).get("deliveryProfiles", {}).get("edges", []):
        profile = edge["node"]
        if "oversized" not in profile["name"].lower():
            continue
        zones_out = []
        for lg in profile.get("profileLocationGroups", []):
            for ze in lg.get("locationGroupZones", {}).get("edges", []):
                znode = ze["node"]
                z = znode.get("zone", {})
                provinces = [
                    prov["code"]
                    for c in z.get("countries", [])
                    for prov in c.get("provinces", [])
                ]
                rates = []
                for me in znode.get("methodDefinitions", {}).get("edges", []):
                    m = me["node"]
                    price = (m.get("rateProvider") or {}).get("price", {}).get("amount")
                    rates.append({"name": m["name"], "price": price})
                zones_out.append({
                    "zone_id":   z.get("id"),
                    "zone_name": z.get("name"),
                    "provinces": sorted(provinces),
                    "rates":     rates
                })
        out.append({"profile": profile["name"], "zone_count": len(zones_out), "zones": zones_out})
    return out


@app.get("/health")
async def health(): return {"status":"ok","version":"1.0.0"}

static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
