"""NED Freight App — FastAPI Server"""

import json, math, os, hmac, hashlib
from pathlib import Path
from typing import Optional
# Load .env before importing anything that reads env vars
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # dotenv only needed for local dev; Render injects env vars natively
import uvicorn
import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from zones import detect_zone, get_oversized_zone
import live_rates  # Live carrier-rate calculation (CP/MF/DF)
import rate_log     # Persistent log of every rate quote (SQLite on Render disk)

# Free-shipping threshold — orders ≥ this get free freight
FREE_SHIPPING_THRESHOLD = float(os.environ.get("FREE_SHIPPING_THRESHOLD", "500"))

# When True, /shopify/rates returns BOTH inclusive (NED_LIVE) and exclusive (NED_LIVE_B2B)
# rates. Only set this after the Delivery Customization Function is active — otherwise retail
# customers see two options. Toggle via Render env var DUAL_RATES=1.
DUAL_RATES = os.environ.get("DUAL_RATES", "0") == "1"
GST = 1.15

# ── Auckland 3PL routing (Phase A) ───────────────────────────────────────────
# Lets NI/retail+B2B customers collect Auckland-3PL stock or have it shipped from
# Auckland (live NEDCOLAKL rate). Mixed carts (some items only in AKL, some only in
# CHCH) fall to a manual "Request Freight Quote" until Phase B adds order splitting.
# Feature-flagged; any error/uncertainty → normal ex-CHCH behaviour (never blocks checkout).
AKL_ROUTING      = os.environ.get("AKL_ROUTING", "0") == "1"
AKL_LOCATION_ID  = "gid://shopify/Location/81228890299"  # Auckland Warehouse (3PL)
CHCH_LOCATION_ID = "gid://shopify/Location/60827664571"  # Click & Collect | Showroom (Main Branch)
# Bias toward fulfilling dual-stock items from Auckland (high 3PL holding cost → deplete
# it first). 0.0 = strict cheapest with ties going to Auckland; 0.10 = choose Auckland
# even if up to 10% dearer than shipping the dual items ex-CHCH.
AKL_BIAS         = float(os.environ.get("AKL_BIAS", "0"))

# Appended to the freight charge at checkout so customers can flag an odd quote.
CONTACT_NOTE = "If this shipping charge doesn't seem right, please contact us."

SHOPIFY_API_KEY    = os.environ.get("SHOPIFY_API_KEY", "")
SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET", "")
APP_URL            = os.environ.get("APP_URL", "https://ned-freight-app.onrender.com")
TOKEN_FILE         = Path(__file__).parent / "data" / "shopify_token.json"

BASE_DIR = Path(__file__).parent
RATES_FILE = BASE_DIR / "data" / "rates.json"
PRODUCTS_FILE = BASE_DIR / "data" / "oversized_products.json"

app = FastAPI(title="NED Freight App", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def shopify_embed_headers(request, call_next):
    """Allow Shopify admin to embed this app's UI in an iframe."""
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors https://*.myshopify.com https://admin.shopify.com"
    )
    # Strip any X-Frame-Options header that would block embedding
    if "x-frame-options" in {k.lower() for k in response.headers}:
        del response.headers["X-Frame-Options"]
    return response

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
    """
    Shopify Carrier Service callback.
    Quotes Castle Parcels (live), Mainfreight + Dailyfreight (formula), picks cheapest.
    Applies NED markup. Returns single rate to Shopify.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    rate_request = body.get("rate", {})
    destination  = rate_request.get("destination", {})
    items        = rate_request.get("items", [])
    currency     = rate_request.get("currency", "NZD")

    # Cart subtotal (Shopify sends item.price in cents)
    order_value = sum(
        (int(i.get("price", 0)) / 100.0) * int(i.get("quantity", 1))
        for i in items
    )

    # Free shipping over threshold
    if order_value >= FREE_SHIPPING_THRESHOLD:
        rate_log.log_rate(destination=destination, items=items, result=None,
                          status="free_shipping", rate=0.0)
        return {"rates": [{
            "service_name": "Free Shipping",
            "service_code": "FREE",
            "total_price":  "0",
            "currency":     currency,
            "description":  f"Free freight on orders over ${int(FREE_SHIPPING_THRESHOLD)}"
        }]}

    # Auckland 3PL routing (Phase A) — may replace the standard rate entirely (cart
    # fulfilled from Auckland) or augment it with an Auckland collect option.
    akl = await _auckland_routing(destination, items, currency, gst_divisor=1.0)
    if akl and "rates" in akl:
        return {"rates": akl["rates"]}

    # Live carrier quote (debug=True so the rate log captures the full carrier breakdown)
    result = await live_rates.calculate_freight(items, destination, debug=True)

    rates_out = []

    # Add a "PICK UP - Wigram Warehouse" option for Canterbury customers only
    if _is_canterbury(destination) and await _all_at_chch(items):
        rates_out.append({
            "service_name": "PICK UP - Wigram Warehouse",
            "service_code": "PICKUP",
            "total_price":  "0",
            "currency":     currency,
            "description":  "Collect from 7 Paradyne Place, Wigram. Please arrange a time before collection."
        })

    if not result.get("success"):
        # No carrier matched — only show pickup (if available) or "Contact us"
        rate_log.log_rate(destination=destination, items=items, result=result,
                          status="no_carrier_match", rate=None,
                          error=result.get("error"))
        if not rates_out:
            rates_out.append({
                "service_name": "Freight — Contact Us",
                "service_code": "BY_REQUEST",
                "total_price":  "0",
                "currency":     currency,
                "description":  "Custom quote required for this destination"
            })
        return {"rates": rates_out}

    # Round customer price up to nearest dollar for a clean display
    display_price = float(math.ceil(result["customer_price"]))
    price_cents = int(display_price * 100)
    rate_log.log_rate(destination=destination, items=items, result=result,
                      status="quoted", rate=display_price)
    rates_out.append({
        "service_name": "Standard Delivery",
        "service_code": "NED_LIVE",
        "total_price":  str(price_cents),
        "currency":     currency,
        "description":  "3 to 5 business days. " + CONTACT_NOTE,
    })
    # B2B exclusive rate — only emitted once DUAL_RATES=1 (i.e. after Delivery Customization
    # Function is active and hiding this from retail customers)
    if DUAL_RATES:
        excl_cents = int(round(display_price / GST, 2) * 100)
        rates_out.append({
            "service_name": "Standard Delivery",
            "service_code": "NED_LIVE_B2B",
            "total_price":  str(excl_cents),
            "currency":     currency,
            "description":  "3 to 5 business days. " + CONTACT_NOTE,
        })
    return {"rates": rates_out}


def _is_canterbury(destination: dict) -> bool:
    """Detect if a destination is in Canterbury (eligible for warehouse pickup)."""
    province = (destination.get("province") or "").upper().strip()
    if province in ("CAN", "CANTERBURY"):
        return True
    # Fall back to city/postcode if province missing
    city = (destination.get("city") or "").lower().strip()
    pc = (destination.get("postal_code") or destination.get("zip") or "").strip()
    canterbury_cities = {
        "christchurch", "chch", "rolleston", "lincoln", "rangiora", "kaiapoi",
        "prebbleton", "halswell", "wigram", "burnside", "ashburton", "darfield",
        "oxford", "amberley", "methven", "geraldine", "timaru", "kaikoura",
        "akaroa", "lyttelton", "sumner", "redcliffs", "ferrymead",
    }
    if city in canterbury_cities:
        return True
    # Canterbury postcode ranges: 7000-8999
    try:
        pc_int = int(pc[:4])
        if 7000 <= pc_int <= 8999:
            return True
    except (ValueError, TypeError):
        pass
    return False


_NI_PROVINCES = {
    "AUK", "NTL", "WKO", "BOP", "GIS", "HKB", "TKI", "MWT", "WGN",
    "AUCKLAND", "NORTHLAND", "WAIKATO", "BAY OF PLENTY", "GISBORNE",
    "HAWKE'S BAY", "HAWKES BAY", "TARANAKI", "MANAWATU-WANGANUI",
    "MANAWATU-WHANGANUI", "WELLINGTON",
}


def _is_north_island(destination: dict) -> bool:
    """North Island destination — eligible to collect from the Auckland 3PL."""
    province = (destination.get("province") or "").upper().strip()
    if province in _NI_PROVINCES:
        return True
    pc = (destination.get("postal_code") or destination.get("zip") or "").strip()
    try:
        # NI postcodes are 0110–6999; South Island is 7000–9999.
        if 100 <= int(pc[:4]) <= 6999:
            return True
    except (ValueError, TypeError):
        pass
    return False


async def get_location_stock(variant_ids: list) -> dict:
    """
    {variant_id(str): {"akl","akl_oh","chch","chch_oh"}} for the two NZ locations,
    where the plain keys are AVAILABLE and *_oh are ON_HAND (physical). on_hand is used
    to tell a genuinely Auckland-exclusive item from a dual-stocked one — "available"
    goes negative on backorder (CHCH sells-when-out-of-stock) and must never gate routing.
    Fail-safe: returns {} on any error so callers fall back to normal behaviour.
    """
    gids = [f"gid://shopify/ProductVariant/{v}" for v in variant_ids if v]
    if not gids:
        return {}
    query = """
    query($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on ProductVariant {
          legacyResourceId
          inventoryItem { inventoryLevels(first: 20) { edges { node {
            location { id }
            quantities(names: ["available", "on_hand"]) { name quantity }
          }}}}
        }
      }
    }
    """
    try:
        headers = _shopify_headers()
        gql_url = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
        async with httpx.AsyncClient(timeout=9.0) as client:
            r = await client.post(gql_url, headers=headers,
                                  json={"query": query, "variables": {"ids": gids}})
        nodes = (r.json().get("data") or {}).get("nodes") or []
    except Exception as e:
        # Don't fail silently — a slow/failed stock read means Auckland routing
        # won't engage (safe: normal ex-CHCH freight), but we want it visible.
        print(f"[AKL] get_location_stock failed ({type(e).__name__}: {e}) — {len(gids)} variants; falling back to normal freight")
        return {}
    out = {}
    for node in nodes:
        if not node:
            continue
        vid = str(node.get("legacyResourceId") or "")
        rec = {"akl": 0, "akl_oh": 0, "chch": 0, "chch_oh": 0}
        levels = (node.get("inventoryItem") or {}).get("inventoryLevels", {}).get("edges", [])
        for e in levels:
            loc = e["node"]["location"]["id"]
            qmap = {x["name"]: x["quantity"] for x in (e["node"].get("quantities") or [])}
            av, oh = qmap.get("available", 0), qmap.get("on_hand", 0)
            if loc == AKL_LOCATION_ID:
                rec["akl"], rec["akl_oh"] = av, oh
            elif loc == CHCH_LOCATION_ID:
                rec["chch"], rec["chch_oh"] = av, oh
        if vid:
            out[vid] = rec
    return out


async def _all_at_chch(items: list) -> bool:
    """
    True if every cart item has physical Christchurch stock (on_hand >= qty) — i.e. it can
    genuinely be collected from / shipped ex the Wigram (CHCH) warehouse. Used to gate the
    "PICK UP - Wigram Warehouse" option so it's never offered for an item that isn't there
    (e.g. an Auckland-only line). Fail-safe: returns True on any error/uncertainty so the
    existing Canterbury pickup behaviour is preserved when we can't read stock.
    """
    try:
        variant_ids = [str(i.get("variant_id")) for i in items if i.get("variant_id")]
        if not variant_ids:
            return True
        stock = await get_location_stock(variant_ids)
        if not stock:
            return True  # couldn't read stock -> don't strip the option
        for i in items:
            s = stock.get(str(i.get("variant_id")))
            if s is None:
                continue  # unknown variant -> don't block on it
            if s.get("chch_oh", 0) < int(i.get("quantity", 1)):
                return False
        return True
    except Exception:
        return True


def _akl_rate(name: str, code: str, price_cents: int, currency: str, desc: str) -> dict:
    return {"service_name": name, "service_code": code, "total_price": str(price_cents),
            "currency": currency, "description": desc}


def _std_rate(price_incl: float, gst_divisor: float, currency: str, desc: str) -> dict:
    """A real, priced 'Standard Delivery' line (gst_divisor divides for the B2B endpoint)."""
    cents = int(float(math.ceil(price_incl / gst_divisor)) * 100)
    return {"service_name": "Standard Delivery", "service_code": "NED_LIVE",
            "total_price": str(cents), "currency": currency, "description": desc}


async def _route_decision(destination: dict, items: list):
    """
    Core Auckland routing DECISION (no rate formatting) — shared by the rate callbacks
    and the /route endpoint so checkout pricing and the order split stay consistent.

    Returns None (no Auckland stock / not feasible) or a dict:
      must_akl, dual, chch_only     — item classification
      akl_items, chch_items         — ship grouping for the chosen (cheapest, AKL-biased) scenario
      akl_price, chch_price         — GST-incl customer_price per ship group
      collectable                   — items Auckland physically holds (must_akl + dual)
      chch_only_price               — freight for the non-Auckland items alone (collect-rest price)
      scenario                      — "A" (dual→CHCH) or "B" (dual→AKL)

    Rules: engage only when >=1 item is in stock at Auckland; CHCH availability never
    gates (default/backorder origin); items with no physical CHCH stock (on_hand < qty)
    must go via Auckland; dual-stock items go to the cheaper origin (AKL-biased).
    Fail-safe: returns None on any error.
    """
    try:
        import asyncio

        variant_ids = [str(i.get("variant_id")) for i in items if i.get("variant_id")]
        if not variant_ids:
            return None
        stock = await get_location_stock(variant_ids)
        if not stock:
            return None
        # Tolerate duplicate/unresolvable variants — any item not in `stock` defaults to
        # CHCH (via _s), so we still engage on resolvable Auckland items (unknowns ship CHCH).

        def _qty(i):
            return int(i.get("quantity", 1))

        def _s(i):
            return stock.get(str(i.get("variant_id")),
                             {"akl": 0, "akl_oh": 0, "chch": 0, "chch_oh": 0})

        must_akl, dual, chch_only = [], [], []
        for i in items:
            q, s = _qty(i), _s(i)
            if s["akl"] >= q:
                (dual if s["chch_oh"] >= q else must_akl).append(i)
            else:
                chch_only.append(i)

        if not must_akl and not dual:
            return None  # nothing in Auckland

        async def akl_q(grp):
            return await live_rates.calculate_auckland_freight(grp, destination) if grp \
                else {"success": True, "customer_price": 0.0}

        async def chch_q(grp):
            return await live_rates.calculate_freight(grp, destination) if grp \
                else {"success": True, "customer_price": 0.0}

        # must_akl always ex-AKL, chch_only always ex-CHCH; DUAL goes where cheaper:
        #   A) dual -> CHCH   B) dual -> AKL
        aA, cA, aB, cB = await asyncio.gather(
            akl_q(must_akl),        chch_q(chch_only + dual),
            akl_q(must_akl + dual), chch_q(chch_only),
        )
        opt = {}
        if aA.get("success") and cA.get("success"):
            opt["A"] = (must_akl, chch_only + dual, aA["customer_price"], cA["customer_price"])
        if aB.get("success") and cB.get("success"):
            opt["B"] = (must_akl + dual, chch_only, aB["customer_price"], cB["customer_price"])
        if not opt:
            return None

        # Prefer Auckland (scenario B) to deplete high-holding-cost AKL stock, when it's
        # not more than AKL_BIAS dearer than routing the dual items ex-CHCH (ties -> AKL).
        if "B" in opt and ("A" not in opt or
                           (opt["B"][2] + opt["B"][3]) <= (opt["A"][2] + opt["A"][3]) * (1 + AKL_BIAS)):
            name = "B"
        else:
            name = "A"
        akl_grp, chch_grp, akl_price, chch_price = opt[name]
        return {
            "must_akl": must_akl, "dual": dual, "chch_only": chch_only,
            "akl_items": akl_grp, "chch_items": chch_grp,
            "akl_price": akl_price, "chch_price": chch_price,
            "collectable": must_akl + dual,
            "chch_only_price": cB["customer_price"] if cB.get("success") else None,
            "scenario": name,
        }
    except Exception:
        return None


async def _auckland_routing(destination: dict, items: list, currency: str,
                            gst_divisor: float = 1.0):
    """
    Additive Auckland-3PL routing for the rate callbacks. Returns None (caller runs the
    normal ex-CHCH flow) or {"rates": [...]} — the COMPLETE rate set, which ALWAYS
    includes a real priced Standard Delivery and can NEVER emit a bare "Request Quote"
    nor suppress the normal freight (the NED3139 bug). Fail-safe: None on any error.
    """
    if not AKL_ROUTING:
        return None
    d = await _route_decision(destination, items)
    if d is None:
        return None
    try:
        akl_grp, chch_grp = d["akl_items"], d["chch_items"]
        akl_price, chch_price = d["akl_price"], d["chch_price"]
        total = akl_price + chch_price
        is_ni = _is_north_island(destination)

        if akl_grp and chch_grp:
            note = "Your order ships from multiple warehouses (Auckland + Christchurch) — 3 to 5 business days."
        elif akl_grp:
            note = "Ships from our Auckland warehouse — 3 to 5 business days."
        else:
            note = "3 to 5 business days."
        rates = [_std_rate(total, gst_divisor, currency, f"{note} {CONTACT_NOTE}")]

        if is_ni and d["collectable"]:
            if not d["chch_only"]:
                rates.append(_akl_rate(
                    "PICK UP - Auckland Warehouse", "PICKUP_AKL", 0, currency,
                    "Collect from our Auckland warehouse, 86 Ascot Road, Māngere. "
                    "We'll email you when it's ready."))
            elif d["chch_only_price"] is not None:
                cents = int(float(math.ceil(d["chch_only_price"] / gst_divisor)) * 100)
                rates.append(_akl_rate(
                    "Collect Auckland items + ship the rest", "NED_MIXED_COLLECT", cents, currency,
                    "Collect Auckland-stocked items from Māngere; the rest ships from Christchurch."))

        try:
            skus = lambda grp: [i.get("sku") or str(i.get("variant_id")) for i in grp]
            rate_log.log_rate(
                destination=destination, items=items, status="akl_routed", rate=round(total, 2),
                result={"akl_route": {"scenario": d["scenario"],
                                      "akl_skus": skus(akl_grp), "akl_charge": round(akl_price, 2),
                                      "chch_skus": skus(chch_grp), "chch_charge": round(chch_price, 2),
                                      "total": round(total, 2)}})
        except Exception:
            pass

        return {"rates": rates}
    except Exception:
        return None


@app.post("/route")
async def route_endpoint(request: Request):
    """
    Origin-routing decision for the order splitter (ned-order-split). Given a cart
    ({items, destination} or a Shopify {rate:{...}} body), returns which variant_ids
    fulfil from Auckland vs Christchurch (+ the freight split), so an order can be split
    to the correct Cin7 branch. Read-only; mirrors what checkout priced.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    rate = body.get("rate", body)
    items = rate.get("items", [])
    destination = rate.get("destination", {})
    d = await _route_decision(destination, items)
    if not d:
        return {"routed": False}
    vids = lambda grp: [str(i.get("variant_id")) for i in grp if i.get("variant_id")]
    return {
        "routed": True,
        "scenario": d["scenario"],
        "akl_variant_ids": vids(d["akl_items"]),
        "chch_variant_ids": vids(d["chch_items"]),
        "collectable_variant_ids": vids(d["collectable"]),
        "akl_charge": round(d["akl_price"], 2),
        "chch_charge": round(d["chch_price"], 2),
    }


@app.post("/shopify/rates-b2b")
async def shopify_rates_b2b(request: Request):
    """
    B2B Carrier Service callback — returns rates ex-GST (÷ 1.15).
    Used for B2B markets with ADD_TAXES_AT_CHECKOUT: Shopify adds 15% GST back
    at checkout so the total stays the same but appears as ex-GST + tax line.
    """
    GST = 1.15
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    rate_request = body.get("rate", {})
    destination  = rate_request.get("destination", {})
    items        = rate_request.get("items", [])
    currency     = rate_request.get("currency", "NZD")

    akl = await _auckland_routing(destination, items, currency, gst_divisor=GST)
    if akl and "rates" in akl:
        return {"rates": akl["rates"]}

    result = await live_rates.calculate_freight(items, destination, debug=True)

    rates_out = []

    if _is_canterbury(destination) and await _all_at_chch(items):
        rates_out.append({
            "service_name": "PICK UP - Wigram Warehouse",
            "service_code": "PICKUP",
            "total_price":  "0",
            "currency":     currency,
            "description":  "Collect from 7 Paradyne Place, Wigram. Please arrange a time before collection."
        })

    if not result.get("success"):
        rate_log.log_rate(destination=destination, items=items, result=result,
                          status="no_carrier_match_b2b", rate=None,
                          error=result.get("error"))
        if not rates_out:
            rates_out.append({
                "service_name": "Freight — Contact Us",
                "service_code": "BY_REQUEST",
                "total_price":  "0",
                "currency":     currency,
                "description":  "Custom quote required for this destination"
            })
        return {"rates": rates_out}

    incl_price = float(math.ceil(result["customer_price"]))
    excl_price = round(incl_price / GST, 2)
    price_cents = int(excl_price * 100)
    rate_log.log_rate(destination=destination, items=items, result=result,
                      status="quoted_b2b", rate=excl_price)
    rates_out.append({
        "service_name": "Standard Delivery",
        "service_code": "NED_LIVE_B2B",
        "total_price":  str(price_cents),
        "currency":     currency,
        "description":  "3 to 5 business days. " + CONTACT_NOTE,
    })
    return {"rates": rates_out}


@app.post("/shopify/rates/debug")
async def shopify_rates_debug(request: Request):
    """
    Same as /shopify/rates but returns all carrier quotes for debugging.
    NOT registered as a Shopify carrier service — use for testing only.
    """
    body = await request.json()
    rate_request = body.get("rate", {})
    items        = rate_request.get("items", [])
    destination  = rate_request.get("destination", {})
    return await live_rates.calculate_freight(items, destination, debug=True)


@app.get("/api/quote")
async def api_quote(
    city: str = "Auckland",
    cbm:  float = 0.5,
    qty:  int = 1,
):
    """
    Test endpoint: simulate a Shopify rate request from query params.
    Example: /api/quote?city=Auckland&cbm=0.5&qty=2
    """
    items = [{"grams": int(cbm * 1000), "quantity": qty, "price": 5000}]  # weight = CBM in kg → grams
    destination = {"city": city, "country": "NZ", "postal_code": ""}
    return await live_rates.calculate_freight(items, destination, debug=True)


@app.get("/api/rate-log")
async def api_rate_log(limit: int = 200):
    """Recent rate quotes (newest first) for the Rate Log admin tab."""
    return {"entries": rate_log.recent(limit)}


@app.post("/api/reload-rates")
async def reload_rates():
    """Force reload of carrier_rates.json without restarting the app."""
    live_rates.reload_carrier_rates()
    return {"status": "ok", "message": "Carrier rates reloaded"}


@app.get("/api/carrier-info")
async def carrier_info():
    """Return all carrier rate cards and metadata for the Carriers tab UI."""
    with open(BASE_DIR / "data" / "carrier_rates.json") as f:
        return json.load(f)


# ─── CBM Admin API ─────────────────────────────────────────────────────────────
# Pulls product weights from Shopify so the UI can browse + edit them.
# Uses the admin access token stored in env (SHOPIFY_ADMIN_TOKEN) for write
# operations. If no token configured, returns read-only data via shopify_token.json.

SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "nedcollections.myshopify.com")
SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")


def _shopify_headers():
    """Use SHOPIFY_ADMIN_TOKEN env var, else fall back to OAuth-persisted token."""
    if SHOPIFY_ADMIN_TOKEN:
        return {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    if TOKEN_FILE.exists():
        data = json.loads(TOKEN_FILE.read_text())
        return {"X-Shopify-Access-Token": data["token"], "Content-Type": "application/json"}
    raise HTTPException(401, "No Shopify token configured")


@app.get("/api/cbm-list")
async def cbm_list():
    """List every active variant with current weight (= CBM in kg-equivalent)."""
    headers = _shopify_headers()
    gql_url = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
    query = """
    query($cursor: String) {
      productVariants(first: 250, after: $cursor, query: "status:ACTIVE") {
        edges { node {
          id sku title
          product { id title productType }
          inventoryItem { id measurement { weight { value } } }
        }}
        pageInfo { hasNextPage endCursor }
      }
    }
    """
    all_v = []
    cursor = None
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            r = await client.post(gql_url, headers=headers, json={"query": query, "variables": {"cursor": cursor}})
            data = r.json()
            for e in data["data"]["productVariants"]["edges"]:
                v = e["node"]
                w = v["inventoryItem"]["measurement"]["weight"]
                all_v.append({
                    "variant_id":   v["id"],
                    "inv_id":       v["inventoryItem"]["id"],
                    "product":      v["product"]["title"],
                    "variant":      v["title"],
                    "sku":          v["sku"] or "",
                    "category":     v["product"].get("productType") or "",
                    "cbm":          (w["value"] if w else 0) or 0,
                })
            if not data["data"]["productVariants"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["data"]["productVariants"]["pageInfo"]["endCursor"]
    return {"variants": all_v, "count": len(all_v)}


@app.put("/api/cbm")
async def cbm_update(request: Request):
    """Update one variant's weight (CBM in kg). Body: {inv_id, cbm}."""
    body = await request.json()
    inv_id = body.get("inv_id")
    cbm = body.get("cbm")
    if not inv_id or cbm is None:
        raise HTTPException(400, "Required: inv_id, cbm")
    cbm = max(float(cbm), 0.0001)  # Shopify rounds <0.001 to 0
    headers = _shopify_headers()
    gql_url = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
    mutation = """
    mutation($id: ID!, $input: InventoryItemInput!) {
      inventoryItemUpdate(id: $id, input: $input) {
        inventoryItem { id measurement { weight { value } } }
        userErrors { field message }
      }
    }
    """
    variables = {
        "id": inv_id,
        "input": {"measurement": {"weight": {"value": cbm, "unit": "KILOGRAMS"}}}
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(gql_url, headers=headers, json={"query": mutation, "variables": variables})
    data = r.json()
    errors = (data.get("data") or {}).get("inventoryItemUpdate", {}).get("userErrors", [])
    if errors:
        raise HTTPException(400, str(errors))
    actual = data["data"]["inventoryItemUpdate"]["inventoryItem"]["measurement"]["weight"]["value"]
    return {"status": "ok", "cbm": actual}

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
    scopes = "read_shipping,write_shipping,read_products"
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

    if data.get("errors") and not data.get("data"):
        return {"error": "GraphQL query failed", "gql_errors": data["errors"]}

    all_profiles = [
        e["node"]
        for e in (data.get("data") or {}).get("deliveryProfiles", {}).get("edges", [])
    ]
    oversized = [p for p in all_profiles if "oversized" in p["name"].lower()]

    # Fulfillment location IDs for this store (from Shopify admin → Settings → Locations).
    # Used when rebuilding empty location groups on profiles that have never had zones.
    # The active NZ shipping location is "Click & Collect | Showroom" (ID 60827664571).
    # Including all 4 store locations so the LG mirrors the working profiles' setup.
    location_ids = [
        "gid://shopify/Location/60827664571",  # Click & Collect | Showroom (Active, fulfills online)
        "gid://shopify/Location/75356766395",  # Click & Collect | Warehouse
    ]

    if not oversized:
        return {
            "error": "No profiles with 'Oversized' in the name found",
            "profiles_found": [p["name"] for p in all_profiles]
        }

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

        # ── Rebuild location group from scratch ───────────────────────────────
        # Using locationGroupsToCreate (not Update) for ALL profiles because:
        # - locationGroupsToUpdate on SI zone always gets Shopify's "geographic
        #   entity cached" rate ($55) overriding whatever rate we specify
        # - locationGroupsToCreate in a fresh LG bypasses this cache entirely
        # - DeliveryLocationGroupZoneInput (used here) is FLAT — no 'zone' wrapper
        all_zone_payloads = [
            {
                "name": z["name"],
                "countries": [{
                    "code": "NZ",
                    "includeAllProvinces": False,
                    "provinces": [{"code": p} for p in z["provinces"]]
                }],
                "methodDefinitionsToCreate": [{
                    "name": "Oversized Freight",
                    "active": True,
                    "rateDefinition": {
                        "price": {"amount": str(float(ZONE_RATES[z["name"]][tier_idx])),
                                  "currencyCode": "NZD"}
                    }
                }]
            }
            for z in ZONE_DEFS
        ]

        rebuild_vars = {
            "id": profile_id,
            "profile": {
                "locationGroupsToDelete": [lg_id],
                "locationGroupsToCreate": [{
                    "locations":     location_ids,
                    "zonesToCreate": all_zone_payloads
                }]
            }
        }
        rebuild_mutation = """
        mutation deliveryProfileUpdate($id: ID!, $profile: DeliveryProfileInput!) {
          deliveryProfileUpdate(id: $id, profile: $profile) {
            profile { profileLocationGroups { locationGroup { id } } }
            userErrors { field message }
          }
        }
        """
        async with httpx.AsyncClient(timeout=60) as client:
            rb = await client.post(gql_url, headers=headers,
                                   json={"query": rebuild_mutation, "variables": rebuild_vars})
            rd = rb.json()

        errs = (rd.get("data") or {}).get("deliveryProfileUpdate", {}).get("userErrors", [])
        if errs:
            results.append({"profile": profile_name, "success": False,
                            "tier_index": tier_idx, "errors": errs})
            continue

        new_lgs = ((rd.get("data", {}).get("deliveryProfileUpdate") or {})
                   .get("profile", {}).get("profileLocationGroups", []))
        new_lg_id = new_lgs[0]["locationGroup"]["id"] if new_lgs else None

        results.append({
            "profile":       profile_name,
            "success":       True,
            "tier_index":    tier_idx,
            "zones_created": len(ZONE_DEFS),
            "zones_deleted": len(existing_zone_ids),
            "new_lg_id":     new_lg_id,
            "errors":        [],
        })

    return {"results": results, "profiles_processed": len(oversized)}


@app.get("/api/debug-profile")
async def debug_profile(name: str = "Oversized 1.00-1.25m3"):
    """Diagnostic: full zone+rate data for a single named profile."""
    if not TOKEN_FILE.exists():
        raise HTTPException(400, "No token.")
    token_data = json.loads(TOKEN_FILE.read_text())
    token, shop = token_data["token"], token_data["shop"]
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    gql_url = f"https://{shop}/admin/api/2024-04/graphql.json"

    # First fetch profile list to find the ID
    list_q = """{ deliveryProfiles(first: 20) { edges { node { id name } } } }"""
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(gql_url, headers=headers, json={"query": list_q})
        ld = r.json()
    profile_id = None
    for e in ld.get("data", {}).get("deliveryProfiles", {}).get("edges", []):
        if e["node"]["name"].lower() == name.lower():
            profile_id = e["node"]["id"]
            break
    if not profile_id:
        return {"error": f"Profile '{name}' not found",
                "available": [e["node"]["name"] for e in ld.get("data",{}).get("deliveryProfiles",{}).get("edges",[])]}

    # Full query on this specific profile
    detail_q = """
    query($id: ID!) {
      deliveryProfile(id: $id) {
        id name
        profileLocationGroups {
          locationGroup { id }
          locationGroupZones(first: 30) {
            edges { node {
              zone { id name countries { provinces { code } } }
              methodDefinitions(first: 10) {
                edges { node {
                  id name active
                  rateProvider {
                    ... on DeliveryRateDefinition { price { amount currencyCode } }
                  }
                }}
              }
            }}
          }
        }
      }
    }
    """
    async with httpx.AsyncClient(timeout=30) as client:
        r2 = await client.post(gql_url, headers=headers,
                               json={"query": detail_q, "variables": {"id": profile_id}})
        return {"profile_id": profile_id, "raw": r2.json()}


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


@app.post("/api/sync-product-profiles")
async def sync_product_profiles():
    """Assign Shopify products to their oversized delivery profiles based on CBM spreadsheet data."""
    if not TOKEN_FILE.exists():
        raise HTTPException(400, "No Shopify token. Re-auth at /shopify/install first.")

    token_data = json.loads(TOKEN_FILE.read_text())
    token, shop = token_data["token"], token_data["shop"]
    headers  = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    gql_url  = f"https://{shop}/admin/api/2024-04/graphql.json"

    # Spreadsheet data: profile name → lists of product titles and SKUs
    # Source: NED Collections Oversized product catagories.xlsx
    PROFILE_PRODUCTS = {
        "Oversized 0.16-0.25m3": {
            "names": ["Beu Chair","Boon Pouf Set","Broome Bedside","Carros Coffee Table","Circular Mirror",
                      "Dawn Long Ottoman","Dawn Ottoman","Elm Bedside","Kahn Table","Lennox Bedside",
                      "Lume Ottoman","Malli Bar Stool","Matte Table","Niche Side Table","Nodi Chair",
                      "Otto Bedside","Peninsula Ottoman","RT-1260","RT-1262","Read Dining Chair",
                      "Story 2 Seater","Surge Floor Lamp","Tres Chair","Tumo Chair"],
            "skus":  ["24101401","555","8101","BS-1344A - Black","HE6262","MC-7495CH","MC-9523CH",
                      "T301","TC092","WD-1797A-Blk","WF-1E019","WF-1E029","WHF-1E003",
                      "ottoman-A","ottoman-B"],
        },
        "Oversized 0.25-0.50m3": {
            "names": ["Alan Chair","Alister Bench","Arch Floor Mirror","Arlo Chair","Bay Chair",
                      "Beu Bar Stool","Canta Coffee Table","Drift Bedhead","Dundee Bench",
                      "Elm Coffee Table","Elm Sideboard","Elm TV Unit","Fara Dining Table",
                      "Fara Extendable Dining Table","Gaudi Bench","George Coffee Table","Halo Chair",
                      "Hickory Large Ottoman","Jay Table","Le Bons Bench","Lou Chair","Lucca Dining Table",
                      "Luma Coffee Table","Mia Occasional Chair","Milan Coffee Table","Nova Coffee Table",
                      "Oki Low Table","Osca Table","Otte Dining Table","Porto Dining Table",
                      "Read Bar Stool","Tanner Chair","Tuscany Dining Chair"],
            "skus":  ["3902","553","A460A","Alice-DT-180","Alice-EX","BS-1797B-Blk","C303","C304",
                      "DC-S197V1","Dundee","GINA-CTR","HE1288","HE3719","LDC-295A","MC-7565BC",
                      "MC-7632CH-A","MC-7790CH","RT-S124A","RT-S260","TC091","TD077","TD097-220",
                      "TITONI-CT"],
        },
        "Oversized 0.50-0.75m3": {
            "names": ["Bayside Chair","Dawn Arm Chair","Dossier Sofa 1 Seater",
                      "Dossier Sofa 1 Seater Left Arm","Dossier Sofa 1 Seater Right Arm",
                      "Elm TV Unit","French Swivel Chair","French Swivel Chair Wooden Trim",
                      "Grace Sofa 1 Seater","Halo Chair","Indo Buffet","Kuva Lounge Chair",
                      "LDB-170","Lune Bed","Lune Bedhead","Niche Dining Table","Nord Chair",
                      "Otley Dining Table","Rue Bistro Chair","Sable TV Unit","Tanner Chair",
                      "Theo Swivel Chair","Vero Table"],
            "skus":  ["34354-FK-1.5AL-F","34422-FK-1.5AL-F","34422-FK-1.5PL-F","34422-FK-1.5PR-F",
                      "A391A","A974","French Chair","HBC247001","HE1288","HSF255001","MC-7764LC",
                      "MC-7779DT","MC-7805BU","WF-1F019A"],
        },
        "Oversized 0.75-1.00m3": {
            "names": ["Cloudy Buffet","Dossier Sofa 1 Seater Chaise","Drift Swivel Chair",
                      "Elm Sideboard","Leo Sideboard","Niche Dining Table","Noel Buffet"],
            "skus":  ["34422-FK-CS-F","Drift Chair","HE3719","MC-7800BU","MC-7805BU","NC06-J-1"],
        },
        "Oversized 1.00-1.25m3": {
            "names": ["Dossier Sofa Corner","Drift Chair","Drift Left Arm Module","Drift Middle Module",
                      "Drift Right Arm Module","Faker Dining Table","Grace Sofa 1 Seater Chaise",
                      "Grace Sofa 1 Seater Left Arm","Grace Sofa 1 Seater Right Arm",
                      "Grace Sofa Corner","Lume Sofa"],
            "skus":  ["34354-FK-1.5PL-F","34354-FK-1.5PR-F","34354-FK-CNR-F","34354-FK-CS-F",
                      "34422-FK-C-F","OSF-1582-Left-SP","OSF-1582-Middle-B","OSF-1582-Right-E"],
        },
        "Oversized 1.25-1.50m3": {
            "names": ["Alice Dining Table","Fleur Sofa"],
            "skus":  ["ROSDT"],
        },
        "Oversized 1.50-1.75m3": {
            "names": ["Verra Armless Sofa"],
            "skus":  [],
        },
        "Oversized 1.75-2.00m3": {
            "names": ["Harlow Sofa","Hendrix Sofa","Milana Sofa"],
            "skus":  ["MC-7600SF","Mingle Sofa"],
        },
        "Oversized 2.00-2.50m3": {
            "names": ["Dyne Sofa"],
            "skus":  ["S2588"],
        },
        "Oversized 2.50m3+": {
            "names": ["Dyne Sofa","Montana Sofa"],
            "skus":  ["34394-FK-F","S2588"],
        },
    }

    # ── Step 1: Fetch all products (paginated) ────────────────────────────────
    product_query = """
    query($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id title
            variants(first: 100) {
              edges { node { id sku } }
            }
          }
        }
      }
    }
    """
    sku_map   = {}  # sku.lower() -> [variant_gid, ...]
    title_map = {}  # title.lower() -> [variant_gid, ...]
    cursor = None
    while True:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(gql_url, headers=headers,
                                  json={"query": product_query, "variables": {"cursor": cursor}})
            pdata = r.json()
        if pdata.get("errors") and not pdata.get("data"):
            return {"error": "Product query failed — token may lack read_products scope",
                    "gql_errors": pdata["errors"]}
        products_page = (pdata.get("data") or {}).get("products", {})
        for edge in products_page.get("edges", []):
            node = edge["node"]
            title_key = node["title"].strip().lower()
            for ve in node["variants"]["edges"]:
                vid = ve["node"]["id"]
                sku = (ve["node"]["sku"] or "").strip()
                if sku:
                    sku_map.setdefault(sku.lower(), []).append(vid)
                title_map.setdefault(title_key, []).append(vid)
        page_info = products_page.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info["endCursor"]

    # ── Step 2: Fetch delivery profile IDs ────────────────────────────────────
    dp_query = """{ deliveryProfiles(first: 30) { edges { node { id name } } } }"""
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(gql_url, headers=headers, json={"query": dp_query})
        dp_data = r.json()
    profile_id_map = {
        e["node"]["name"]: e["node"]["id"]
        for e in (dp_data.get("data") or {}).get("deliveryProfiles", {}).get("edges", [])
    }

    # ── Step 3: Match variants + assign to profiles ────────────────────────────
    mutation = """
    mutation deliveryProfileUpdate($id: ID!, $profile: DeliveryProfileInput!) {
      deliveryProfileUpdate(id: $id, profile: $profile) {
        profile { id name }
        userErrors { field message }
      }
    }
    """
    results = []
    for profile_name, sources in PROFILE_PRODUCTS.items():
        profile_id = profile_id_map.get(profile_name)
        if not profile_id:
            results.append({"profile": profile_name, "error": "Profile not found in Shopify"})
            continue

        variant_ids = set()
        unmatched_skus, unmatched_names = [], []

        for sku in sources["skus"]:
            found = sku_map.get(sku.lower())
            if found:
                variant_ids.update(found)
            else:
                unmatched_skus.append(sku)

        for name in sources["names"]:
            found = title_map.get(name.lower())
            if found:
                variant_ids.update(found)
            else:
                unmatched_names.append(name)

        if not variant_ids:
            results.append({"profile": profile_name, "warning": "No variants matched",
                            "unmatched_skus": unmatched_skus, "unmatched_names": unmatched_names})
            continue

        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(gql_url, headers=headers,
                                  json={"query": mutation,
                                        "variables": {
                                            "id": profile_id,
                                            "profile": {"variantsToAssociate": list(variant_ids)}
                                        }})
            mdata = r.json()

        errs = (mdata.get("data") or {}).get("deliveryProfileUpdate", {}).get("userErrors", [])
        results.append({
            "profile":          profile_name,
            "success":          not bool(errs),
            "variants_assigned": len(variant_ids),
            "unmatched_skus":   unmatched_skus,
            "unmatched_names":  unmatched_names,
            "errors":           errs,
        })

    total_products_indexed = len({v for vlist in title_map.values() for v in vlist})
    return {
        "products_indexed": total_products_indexed,
        "skus_indexed":     len(sku_map),
        "results":          results,
    }


@app.get("/health")
async def health(): return {"status":"ok","version":"1.0.0"}


# ── Cin7 → SharePoint sheet log ─────────────────────────────────────────────
# When a Shopify-synced sales order is created in Cin7, append a row to the
# NoEyeDeer Collections "Freight Calculator.xlsx" workbook with the company,
# order number, Shopify freight charge, suggested carrier, and delivery city.
# See cin7_sheet_log.py for the Graph write logic.

from base64 import b64encode as _b64
import cin7_sheet_log

CIN7_USERNAME       = os.environ.get("CIN7_USERNAME", "")
CIN7_API_KEY        = os.environ.get("CIN7_API_KEY", "")
CIN7_WEBHOOK_TOKEN  = os.environ.get("CIN7_WEBHOOK_TOKEN", "")
SHOPIFY_STORE       = os.environ.get("SHOPIFY_STORE", "nedcollections.myshopify.com")
SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")


def _cin7_auth_header() -> dict:
    if not (CIN7_USERNAME and CIN7_API_KEY):
        return {}
    return {"Authorization": "Basic " + _b64(f"{CIN7_USERNAME}:{CIN7_API_KEY}".encode()).decode()}


async def _fetch_cin7_sales_order(order_id: int) -> Optional[dict]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"https://api.cin7.com/api/v1/SalesOrders",
            params={"where": f"id={order_id}"},
            headers=_cin7_auth_header(),
        )
    if r.status_code != 200:
        return None
    data = r.json()
    return data[0] if isinstance(data, list) and data else None


async def _fetch_shopify_order_by_name(name: str) -> Optional[dict]:
    """Get items + shipping address for the Shopify order, to recompute the carrier."""
    if not SHOPIFY_ADMIN_TOKEN:
        return None
    query = """
    query($q: String!) {
      orders(first: 1, query: $q) {
        nodes {
          name
          shippingAddress { address1 city province country zip }
          lineItems(first: 50) { nodes { quantity sku variant { inventoryItem { measurement { weight { value unit } } } } } }
        }
      }
    }
    """
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json",
            headers={"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"},
            json={"query": query, "variables": {"q": f"name:{name}"}},
        )
    if r.status_code != 200:
        return None
    nodes = ((r.json().get("data") or {}).get("orders") or {}).get("nodes") or []
    return nodes[0] if nodes else None


async def _derive_suggested_carrier(shopify_name: str) -> str:
    """Recompute the carrier the freight app would suggest for this order's cart."""
    so = await _fetch_shopify_order_by_name(shopify_name)
    if not so:
        return ""
    addr = so.get("shippingAddress") or {}
    items = []
    for li in (so.get("lineItems", {}) or {}).get("nodes", []):
        v = li.get("variant") or {}
        meas = ((v.get("inventoryItem") or {}).get("measurement") or {}).get("weight") or {}
        kg = float(meas.get("value") or 0)        # value is in measurement.unit (KILOGRAMS by default)
        grams = int(round(kg * 1000))             # weight (= CBM kg) → grams for freight calc
        items.append({"grams": grams, "quantity": int(li.get("quantity", 1)), "name": li.get("sku") or ""})
    dest = {
        "city":        addr.get("city", ""),
        "province":    addr.get("province", ""),
        "country":     addr.get("country", "NZ"),
        "postal_code": addr.get("zip", ""),
        "address1":    addr.get("address1", ""),
    }
    result = await live_rates.calculate_freight(items, dest, debug=False)
    return result.get("chosen_carrier", "") if result.get("success") else ""


@app.post("/cin7/webhook/sales-order")
async def cin7_sales_order_webhook(request: Request, token: str = ""):
    """
    Cin7 SalesOrder.Created webhook.
    URL: POST /cin7/webhook/sales-order?token=<CIN7_WEBHOOK_TOKEN>
    Body: Cin7 webhook payload (at minimum contains the SO id).

    Idempotent: skips rows whose Order Number is already in the sheet.
    Filter: only Shopify-synced orders (source / projectName).
    Returns 200 even on internal failures (so Cin7 doesn't retry-storm); failures
    are logged in the response body for the operator to inspect.
    """
    if not CIN7_WEBHOOK_TOKEN or token != CIN7_WEBHOOK_TOKEN:
        raise HTTPException(401, "invalid webhook token")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # Cin7 payloads vary; accept several common id keys
    order_id = (
        payload.get("salesOrderId") or payload.get("SalesOrderId")
        or payload.get("id") or payload.get("Id")
    )
    if not order_id:
        return {"status": "ignored", "reason": "no_order_id", "payload_keys": list(payload.keys())}

    order = await _fetch_cin7_sales_order(order_id)
    if not order:
        return {"status": "error", "reason": "cin7_fetch_failed", "id": order_id}

    # Filter: Shopify-synced orders only (source = "Shopify NedCollectionsNZ").
    source = (order.get("source") or order.get("projectName") or "").lower()
    if "shopify" not in source:
        return {"status": "ignored", "reason": "not_shopify_synced", "source": source}

    order_ref     = (order.get("reference") or "").strip()
    # Prefer company → billing company → retail fallback (firstName + lastName)
    company       = (order.get("company") or order.get("billingCompany") or "").strip()
    if not company:
        company = (f"{order.get('firstName') or ''} {order.get('lastName') or ''}").strip()
    delivery_city = order.get("deliveryCity") or ""
    freight       = order.get("freightTotal")

    # Idempotency: skip if already in the sheet
    try:
        if order_ref and cin7_sheet_log.order_already_logged(order_ref):
            return {"status": "skipped", "reason": "already_logged", "order": order_ref}
    except Exception as e:
        return {"status": "error", "reason": "sheet_read_failed", "detail": str(e)}

    # Recompute the suggested carrier by replaying the freight calc on the Shopify cart
    suggested_carrier = ""
    if order_ref:
        try:
            suggested_carrier = await _derive_suggested_carrier(order_ref)
        except Exception:
            suggested_carrier = ""

    try:
        result = cin7_sheet_log.prepend_order_row(
            company=company,
            order_number=order_ref,
            shopify_freight=freight,
            suggested_carrier=suggested_carrier,
            delivery_city=delivery_city,
        )
    except Exception as e:
        return {"status": "error", "reason": "sheet_write_failed", "detail": str(e)}

    return {"status": "logged", "order": order_ref, "row": result.get("row"), "carrier": suggested_carrier}


# ── Shopify orders/create webhook ───────────────────────────────────────────
# Primary live trigger (Cin7 webhooks aren't available to us). Shopify calls
# this on every new order; we extract directly from the Shopify order and
# append a row. Same shape/columns as the Cin7 path — just an earlier trigger
# at order-placement rather than waiting for the Cin7 sync.

def _verify_shopify_hmac(body: bytes, header_hmac: str) -> bool:
    secret = os.environ.get("SHOPIFY_API_SECRET", "")
    if not secret or not header_hmac:
        return False
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    import base64 as _b
    expected = _b.b64encode(digest).decode()
    return hmac.compare_digest(expected, header_hmac)


async def _fetch_shopify_order_full(name: str) -> Optional[dict]:
    """Single GraphQL pull of everything we need for the sheet row + carrier recompute."""
    if not SHOPIFY_ADMIN_TOKEN:
        return None
    query = """
    query($q: String!) {
      orders(first: 1, query: $q) {
        nodes {
          name
          customer { firstName lastName displayName
                     companyContactProfiles { company { name } } }
          shippingAddress { address1 city province country zip company }
          shippingLine { discountedPriceSet { shopMoney { amount } } }
          lineItems(first: 50) { nodes { quantity sku variant {
            inventoryItem { measurement { weight { value unit } } }
          } } }
        }
      }
    }
    """
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json",
            headers={"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"},
            json={"query": query, "variables": {"q": f"name:{name}"}},
        )
    if r.status_code != 200:
        return None
    nodes = ((r.json().get("data") or {}).get("orders") or {}).get("nodes") or []
    return nodes[0] if nodes else None


def _company_from_shopify(o: dict) -> str:
    """
    Prefer B2B company link → fall back to ship-to company → for retail
    (no company at all) use the customer's name so the row isn't blank.
    """
    profs = ((o.get("customer") or {}).get("companyContactProfiles") or [])
    if profs and (profs[0].get("company") or {}).get("name"):
        return profs[0]["company"]["name"]
    ship_co = ((o.get("shippingAddress") or {}).get("company") or "").strip()
    if ship_co:
        return ship_co
    # Retail: use customer name
    cust = o.get("customer") or {}
    name = f"{cust.get('firstName') or ''} {cust.get('lastName') or ''}".strip()
    return name or (cust.get("displayName") or "")


def _cart_items_from_shopify(o: dict) -> list:
    items = []
    for li in (o.get("lineItems", {}) or {}).get("nodes", []):
        v = li.get("variant") or {}
        meas = ((v.get("inventoryItem") or {}).get("measurement") or {}).get("weight") or {}
        kg = float(meas.get("value") or 0)
        items.append({"grams": int(round(kg * 1000)), "quantity": int(li.get("quantity", 1)), "name": li.get("sku") or ""})
    return items


@app.post("/shopify/webhook/orders-create")
async def shopify_order_created(request: Request):
    """
    Shopify orders/create webhook → append a row to the Freight Calculator sheet.
    Validates HMAC against SHOPIFY_API_SECRET. Returns 200 even on internal errors
    so Shopify doesn't retry-storm; failure detail comes back in the response body.
    """
    body = await request.body()
    if not _verify_shopify_hmac(body, request.headers.get("X-Shopify-Hmac-Sha256", "")):
        raise HTTPException(401, "invalid hmac")

    try:
        payload = json.loads(body or b"{}")
    except Exception:
        return {"status": "ignored", "reason": "bad_json"}

    name = (payload.get("name") or "").strip()
    if not name:
        return {"status": "ignored", "reason": "no_name"}

    # Dedup against sheet (column B) before doing any work
    try:
        if cin7_sheet_log.order_already_logged(name):
            return {"status": "skipped", "reason": "already_logged", "order": name}
    except Exception as e:
        return {"status": "error", "reason": "sheet_read_failed", "detail": str(e)}

    # Pull the order via GraphQL — cleaner than parsing the REST webhook body
    o = await _fetch_shopify_order_full(name)
    if not o:
        return {"status": "error", "reason": "shopify_order_not_found", "order": name}

    company       = _company_from_shopify(o)
    delivery_city = (o.get("shippingAddress") or {}).get("city", "")
    sline         = (o.get("shippingLine") or {}).get("discountedPriceSet", {}).get("shopMoney", {})
    try:
        freight = float(sline.get("amount")) if sline.get("amount") is not None else None
    except (TypeError, ValueError):
        freight = None

    # Recompute the suggested carrier from the cart + destination
    suggested_carrier = ""
    try:
        items = _cart_items_from_shopify(o)
        dest = {
            "city":        delivery_city,
            "province":    (o.get("shippingAddress") or {}).get("province", ""),
            "country":     (o.get("shippingAddress") or {}).get("country", "NZ"),
            "postal_code": (o.get("shippingAddress") or {}).get("zip", ""),
            "address1":    (o.get("shippingAddress") or {}).get("address1", ""),
        }
        result = await live_rates.calculate_freight(items, dest, debug=False)
        if result.get("success"):
            suggested_carrier = result.get("chosen_carrier", "") or ""
    except Exception:
        suggested_carrier = ""

    try:
        res = cin7_sheet_log.prepend_order_row(
            company=company, order_number=name,
            shopify_freight=freight, suggested_carrier=suggested_carrier,
            delivery_city=delivery_city,
        )
    except Exception as e:
        return {"status": "error", "reason": "sheet_write_failed", "detail": str(e)}

    return {"status": "logged", "order": name, "row": res.get("row"), "carrier": suggested_carrier}


@app.post("/shopify/webhook/register")
async def shopify_webhook_register(token: str = ""):
    """
    One-time helper: register the orders/create webhook with Shopify.
    Hit once with ?token=<CIN7_WEBHOOK_TOKEN> (reused as a generic admin token).
    Idempotent — if a webhook for the same topic+address already exists, it's left alone.
    """
    if not CIN7_WEBHOOK_TOKEN or token != CIN7_WEBHOOK_TOKEN:
        raise HTTPException(401, "invalid token")
    if not SHOPIFY_ADMIN_TOKEN:
        raise HTTPException(500, "SHOPIFY_ADMIN_TOKEN not set")
    address = f"{APP_URL.rstrip('/')}/shopify/webhook/orders-create"
    H = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    base = f"https://{SHOPIFY_STORE}/admin/api/2024-10/webhooks.json"
    async with httpx.AsyncClient(timeout=20) as client:
        existing = (await client.get(base, headers=H, params={"topic": "orders/create"})).json().get("webhooks", [])
        for w in existing:
            if w.get("address") == address and w.get("topic") == "orders/create":
                return {"status": "already_registered", "webhook_id": w.get("id"), "address": address}
        r = await client.post(base, headers=H, json={"webhook": {"topic": "orders/create", "address": address, "format": "json"}})
    return {"status": r.status_code, "response": r.json(), "address": address}


@app.get("/cin7/webhook/sales-order/test")
async def cin7_webhook_test(token: str = "", order_id: int = 0):
    """
    Manual dry-run: ?token=<X>&order_id=<Cin7 SO id>.
    Same logic as the webhook but triggered manually for testing. Use this
    to verify a real order writes correctly before registering the Cin7 webhook.
    """
    if not CIN7_WEBHOOK_TOKEN or token != CIN7_WEBHOOK_TOKEN:
        raise HTTPException(401, "invalid webhook token")
    if not order_id:
        raise HTTPException(400, "order_id required")
    # Simulate the webhook by calling the same path with a synthetic payload
    class _Req:
        async def json(self): return {"id": order_id}
    return await cin7_sales_order_webhook(_Req(), token=token)

static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
