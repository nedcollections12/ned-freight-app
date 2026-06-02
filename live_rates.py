"""
Live carrier rate calculation for NED Collections.

For each cart:
1. Estimate per-item cube dimensions from weight (weight = CBM convention).
2. Query Castle Parcels via GoSweetSpot API (live, per-parcel pricing).
3. Compute Mainfreight and Dailyfreight cost from cached rate cards.
4. Pick cheapest of the three.
5. Apply NED markup (FAF already baked into GSS Cost; MF/DF need FAF+GST applied).
6. Return single rate to Shopify.

All amounts NZD.
"""

import json
import math
import os
import unicodedata
from pathlib import Path
from typing import Optional

import httpx


def _strip_diacritics(s: str) -> str:
    """Remove macrons and other accents — e.g. 'Wānaka' → 'wanaka'."""
    if not s:
        return ""
    decomposed = unicodedata.normalize("NFD", s)
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn")

BASE_DIR = Path(__file__).parent
RATES_FILE = BASE_DIR / "data" / "carrier_rates.json"

GSS_ACCESS_KEY = os.environ.get("GSS_ACCESS_KEY", "")
GSS_SITE_ID    = os.environ.get("GSS_SITE_ID", "")
GSS_URL        = "https://api.gosweetspot.com/api/rates"

ORIGIN = {
    "Name": "NED Collections Warehouse",
    "Address": {
        # Warehouse, NOT the showroom. Used as the pickup point for all carrier quotes.
        "StreetAddress": os.environ.get("ORIGIN_STREET", "7 Paradyne Place"),
        "Suburb":        os.environ.get("ORIGIN_SUBURB", "Wigram"),
        "City":          os.environ.get("ORIGIN_CITY", "Christchurch"),
        "PostCode":      os.environ.get("ORIGIN_POSTCODE", "8042"),
        "CountryCode":   os.environ.get("ORIGIN_COUNTRY", "NZ"),
    }
}

# Multipliers — tunable via env vars
FAF_MULTIPLIER = float(os.environ.get("FAF_MULTIPLIER", "1.30"))   # Fuel adjustment
GST_MULTIPLIER = float(os.environ.get("GST_MULTIPLIER", "1.15"))   # NZ GST 15%
NED_MARKUP     = float(os.environ.get("NED_MARKUP", "1.10"))       # NED's margin

# GoSweetSpot's "Cost" field already includes their 8% markup (NED pays this).
# Strip it to get the carrier's true cost, then re-apply NED markup.
GSS_BUILTIN_MARKUP = 1.08

_carrier_rates_cache = None


def _load_carrier_rates():
    global _carrier_rates_cache
    if _carrier_rates_cache is None:
        with open(RATES_FILE) as f:
            _carrier_rates_cache = json.load(f)
    return _carrier_rates_cache


def reload_carrier_rates():
    """Force reload of carrier rates from disk (for /api/reload-rates endpoint)."""
    global _carrier_rates_cache
    _carrier_rates_cache = None


def _normalise_city(city: str) -> str:
    """
    Map a customer city to a rate-card city via alias table.
    Strips macrons first so 'Wānaka' matches the same key as 'Wanaka'.
    """
    rates = _load_carrier_rates()
    city_key = _strip_diacritics((city or "").strip().lower())
    if not city_key:
        return ""
    aliases = rates.get("city_aliases", {})
    return aliases.get(city_key, city_key)


# ── Postcode / province fallback ────────────────────────────────────────────
# When the city field doesn't resolve to a rate key (e.g. Google Address
# Validation puts a suburb like "Karori" or "Riccarton" in the city field),
# we resolve the destination by NZ postcode, then by region. This stops a
# valid NZ address from falling through to no_carrier_match (which would let
# Shopify undercharge via its static delivery profile).
#
# Each entry maps to (mainfreight_key, dailyfreight_key) because MF uses flat
# city keys while DF uses zoned keys. Ranges are inclusive 4-digit postcodes.
_POSTCODE_KEYS = [
    (100,  409,  "whangarei",        "whangarei"),       # Northland (Whangarei, Dargaville, Kerikeri, Paihia)
    (410,  499,  "kaitaia",          "kaitaia"),          # Far North (Kaitaia)
    (500,  599,  "whangarei",        "whangarei"),        # Kaipara
    (600,  999,  "auckland",         "auckland_z1"),      # North Shore / Rodney / West Auckland
    (1010, 1099, "auckland",         "auckland_z1"),      # Auckland central / isthmus / east
    (2000, 2999, "auckland",         "auckland_z1"),      # South Auckland / Manukau / Papakura / Pukekohe
    (3010, 3099, "rotorua",          "rotorua"),          # Rotorua
    (3110, 3119, "tauranga",         "tauranga"),         # Tauranga / Mt Maunganui / Papamoa / Te Puke
    (3120, 3199, "rotorua",          "whakatane"),        # Eastern BoP (Whakatane, Opotiki)
    (3200, 3299, "hamilton",         "hamilton_z1"),      # Hamilton
    (3330, 3399, "taupo",            "taupo"),            # Taupo / Turangi
    (3400, 3499, "hamilton",         "hamilton_z1"),      # Cambridge / Te Awamutu / Otorohanga
    (3500, 3599, "thames",           "thames"),           # Thames / Coromandel
    (4010, 4099, "gisborne",         "gisborne"),         # Gisborne
    (4100, 4299, "napier",           "napier"),           # Hawke's Bay (Napier, Hastings, Waipukurau)
    (4300, 4399, "new plymouth",     "new plymouth"),     # Taranaki (New Plymouth, Stratford)
    (4400, 4499, "palmerston north", "palmerston north"), # Manawatu (PN, Feilding, Dannevirke)
    (4500, 4699, "wanganui",         "wanganui"),         # Whanganui + South Taranaki (Hawera)
    (5010, 5099, "wellington",       "wellington"),       # Hutt / Porirua / Kapiti
    (5500, 5599, "levin",            "levin"),            # Horowhenua (Levin)
    (5800, 5899, "wellington",       "masterton"),        # Wairarapa (Masterton)
    (6010, 6099, "wellington",       "wellington"),       # Wellington city
    (7010, 7199, "nelson",           "nelson"),           # Nelson / Tasman (Richmond, Motueka)
    (7200, 7399, "blenheim",         "blenheim"),         # Marlborough (Blenheim, Picton) + Kaikoura / Hanmer
    (7400, 7699, "christchurch",     "christchurch"),     # North Canterbury (Rangiora, Amberley, Oxford)
    (7700, 7799, "christchurch",     "ashburton"),        # Mid Canterbury (Ashburton)
    (7800, 7899, "greymouth",        "greymouth"),        # West Coast (Greymouth, Hokitika, Westport)
    (7900, 7999, "timaru",           "timaru"),           # South Canterbury (Timaru)
    (8010, 8099, "christchurch",     "christchurch"),     # Christchurch city
    (9000, 9099, "dunedin",          "dunedin"),          # Dunedin
    (9300, 9399, "cromwell",         "cromwell_z1"),      # Central Otago (Cromwell, Alexandra, Queenstown, Wanaka)
    (9400, 9499, "oamaru",           "oamaru"),           # Waitaki (Oamaru)
    (9700, 9799, "invercargill",     "gore"),             # Gore / Eastern Southland
    (9800, 9899, "invercargill",     "invercargill"),     # Invercargill / Southland
]

# Region (Shopify province) → (mainfreight_key, dailyfreight_key).
# Coarser than postcode but a robust catch-all: the region IS the location.
_PROVINCE_KEYS = {
    "northland":            ("whangarei",        "whangarei"),
    "auckland":             ("auckland",         "auckland_z1"),
    "waikato":              ("hamilton",         "hamilton_z1"),
    "bay of plenty":        ("tauranga",         "tauranga"),
    "gisborne":             ("gisborne",         "gisborne"),
    "hawke's bay":          ("napier",           "napier"),
    "hawkes bay":           ("napier",           "napier"),
    "taranaki":             ("new plymouth",     "new plymouth"),
    "manawatu-whanganui":   ("palmerston north", "palmerston north"),
    "manawatu-wanganui":    ("palmerston north", "palmerston north"),
    "manawatu":             ("palmerston north", "palmerston north"),
    "wellington":           ("wellington",       "wellington"),
    "tasman":               ("nelson",           "nelson"),
    "nelson":               ("nelson",           "nelson"),
    "marlborough":          ("blenheim",         "blenheim"),
    "west coast":           ("greymouth",        "greymouth"),
    "canterbury":           ("christchurch",     "christchurch"),
    "otago":                ("dunedin",          "dunedin"),
    "southland":            ("invercargill",     "invercargill"),
}


def _fallback_keys(destination: dict) -> Optional[tuple]:
    """
    Resolve (mainfreight_key, dailyfreight_key) from postcode, then province.
    Used only when the city field fails to match any rate key, so it never
    alters pricing for addresses that already resolve. Returns None if neither
    postcode nor province can be matched.
    """
    pc_raw = (destination.get("postal_code") or destination.get("zip") or "").strip()
    if pc_raw:
        try:
            pc = int(pc_raw[:4])
            for low, high, mf_key, df_key in _POSTCODE_KEYS:
                if low <= pc <= high:
                    return (mf_key, df_key)
        except (ValueError, TypeError):
            pass

    prov = _strip_diacritics((destination.get("province") or "").strip().lower())
    if prov:
        if prov in _PROVINCE_KEYS:
            return _PROVINCE_KEYS[prov]
        # Tolerate partial/variant region strings ("manawatu whanganui" etc.)
        for key, val in _PROVINCE_KEYS.items():
            if key in prov or prov in key:
                return val
    return None


def _cube_dimensions_cm(weight_kg: float) -> tuple:
    """
    Convert a weight (= CBM in kg) into a cube's side length in cm.
    1.0kg = 1.0m³ → 100cm cube. 0.5kg = 0.5m³ → 79.4cm cube.
    Floor at 5cm to avoid zero-dimension API errors.
    """
    if weight_kg <= 0:
        return (5, 5, 5)
    side_m = weight_kg ** (1.0 / 3.0)
    side_cm = max(side_m * 100, 5)
    return (round(side_cm, 1), round(side_cm, 1), round(side_cm, 1))


# Cap any single package sent to GSS — beyond ~1m³ couriers won't quote
# sensibly. Larger carts are split across multiple cartons of equal volume.
_MAX_CARTON_CBM = 1.0


def _build_packages(items: list) -> list:
    """
    Consolidate the entire cart into a small number of equal-sized cartons
    (capped at _MAX_CARTON_CBM each), rather than sending one parcel per
    cart line × quantity.

    Real-world fulfilment packs multiple units per carton, but the previous
    "one-parcel-per-unit" approach hit GSS's per-parcel minimums (~$8 each):
    a 24-unit ACSR cart quoted $206 via Post Haste even though the warehouse
    actually ships it as 2 cartons quoted at $30 on Castle Parcels' portal.
    Consolidating matches what the warehouse does and produces realistic
    courier quotes. MF/DF already price on total volume, so they're unaffected.
    """
    total_kg = sum(
        (float(item.get("grams", 0) or 0) / 1000.0) * int(item.get("quantity", 1))
        for item in items
    )
    if total_kg <= 0:
        # Empty/zero-weight cart — single tiny placeholder so GSS doesn't error
        return [{"Name": "Carton", "Length": 5, "Width": 5, "Height": 5, "Kg": 0.001, "Type": "Box"}]

    n_cartons = max(1, math.ceil(total_kg / _MAX_CARTON_CBM))
    kg_per_carton = total_kg / n_cartons
    L, W, H = _cube_dimensions_cm(kg_per_carton)
    carton = {"Name": "Carton", "Length": L, "Width": W, "Height": H,
              "Kg": round(kg_per_carton, 3), "Type": "Box"}
    return [dict(carton) for _ in range(n_cartons)]


def _total_cbm(items: list) -> float:
    """Sum cart CBM (weight in kg = CBM convention)."""
    total = 0.0
    for item in items:
        qty = int(item.get("quantity", 1))
        grams = float(item.get("grams", 0) or 0)
        total += (grams / 1000.0) * qty
    return total


# GSS geocodes off the City field and rejects some legacy / amalgamated
# council names (former cities and regional-district names) even with a valid
# postcode — returning zero rate options. We remap those to the parent metro
# for the GSS City field only; the customer's original value stays in Suburb,
# and the rate itself is postcode-driven so it stays accurate.
# (Confirmed unrecognised via live GSS probes; extend as new ones surface.)
_GSS_CITY_REMAP = {
    "north shore": "Auckland",
    "waitemata":   "Auckland",
    "rodney":      "Auckland",
    "kapiti":      "Wellington",
    "hutt city":   "Lower Hutt",
}


async def quote_castle_parcels(items: list, destination: dict) -> Optional[dict]:
    """
    Live quote from GoSweetSpot (Castle Parcels / Post Haste).
    Returns dict with raw NED cost (8% GSS markup stripped), or None on failure.
    """
    if not GSS_ACCESS_KEY or not GSS_SITE_ID:
        return None

    raw_city = destination.get("city", "")
    # Remap GSS-unrecognised city names to their parent metro so GSS can quote.
    gss_city = _GSS_CITY_REMAP.get(_strip_diacritics(raw_city.strip().lower()), raw_city)

    dest_payload = {
        "Name": destination.get("name", "Customer"),
        "Address": {
            "StreetAddress": destination.get("address1") or destination.get("address", ""),
            "Suburb":        raw_city,    # keep original locality; GSS treats Suburb loosely
            "City":          gss_city,
            "PostCode":      destination.get("postal_code") or destination.get("zip", ""),
            "CountryCode":   destination.get("country") or "NZ",
        }
    }

    pkgs = _build_packages(items)
    if not pkgs:
        return None

    payload = {
        "DeliveryReference": "QUOTE",
        "Origin": ORIGIN,
        "Destination": dest_payload,
        "Packages": pkgs,
    }
    headers = {
        "access_key": GSS_ACCESS_KEY,
        "site_id":    GSS_SITE_ID,
        "Content-Type": "application/json",
    }

    try:
        # 7s: catches GSS's slow tail while staying under Shopify's ~10s
        # carrier-service window (MF/DF + serialization are instant, so the
        # rest of the request needs only ~1s of that budget).
        async with httpx.AsyncClient(timeout=7.0) as client:
            r = await client.post(GSS_URL, json=payload, headers=headers)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        # GSS timeout/error → fall back to MF/DF formulas (instant)
        return None

    options = data.get("Available", [])
    if not options:
        return None

    # SAFETY FILTER: drop suspiciously low quotes for large items.
    # GSS sometimes returns Kiwi Express Car-Economy at $13-86 for cube >1m³,
    # which is clearly wrong for sofa-sized parcels (real KX Oversize minimum
    # for ChCh local is ~$46, Auckland ~$87).
    # If filter removes everything → return None so MF/DF formulas take over.
    cart_cbm = sum(float(p.get("Length", 0)) * float(p.get("Width", 0)) * float(p.get("Height", 0))
                   for p in payload["Packages"]) / 1_000_000  # cm³ → m³
    min_sensible_per_m3 = 40  # absolute floor: $40/m³ raw cost
    threshold = max(8, cart_cbm * min_sensible_per_m3)
    filtered = [
        o for o in options
        if (o.get("Cost", 0) / GSS_BUILTIN_MARKUP) >= threshold
    ]
    if not filtered:
        # All GSS quotes are unrealistic for this cart — skip CP entirely
        # so the cheapest of MF/DF (formula-based) gets picked.
        return None
    options = filtered

    # Choose cheapest available option (typically Post Haste 2-Day)
    cheapest = min(options, key=lambda o: o.get("Cost", float("inf")))
    gss_cost = float(cheapest.get("Cost", 0))
    true_cost = gss_cost / GSS_BUILTIN_MARKUP  # strip GSS's 8%

    return {
        "carrier":   cheapest.get("CarrierName", "Castle Parcels"),
        "service":   cheapest.get("DeliveryType", "Standard"),
        "raw_cost":  round(true_cost, 2),  # incl FAF+GST already
        "is_rural":  cheapest.get("IsRuralDelivery", False),
        "_source":   "GoSweetSpot live API",
    }


def quote_mainfreight(cart_cbm: float, destination: dict, override_key: Optional[str] = None) -> Optional[dict]:
    """
    Compute Mainfreight cost from cached rate card.
    Formula: MAX(min_charge, base + per_m3 × cart_cbm). Result is excl GST & excl FAF.
    override_key forces a specific rate key (used by the postcode/province fallback).
    """
    rates = _load_carrier_rates()
    city = override_key or _normalise_city(destination.get("city", ""))
    rate = rates["mainfreight"]["rates"].get(city)
    if not rate:
        return None
    base = rate["base"]
    per = rate["per_m3"]
    minc = rate["min"]
    raw_excl = max(minc, base + per * cart_cbm)
    # Apply FAF and GST to align with GSS Cost (which already includes them)
    raw_cost = raw_excl * FAF_MULTIPLIER * GST_MULTIPLIER
    return {
        "carrier":   "Mainfreight",
        "service":   "M2H Two-Man",
        "raw_cost":  round(raw_cost, 2),
        "_source":   f"Formula: MAX({minc}, {base} + {per} × {cart_cbm:.3f}) × FAF × GST",
    }


def _df_tier_index(cart_cbm: float) -> int:
    """Pick per-m³ tier based on cart CBM. 0 = small (<5), 1 = mid (5-10), 2 = large (10+)."""
    if cart_cbm >= 10:
        return 2
    if cart_cbm >= 5:
        return 1
    return 0


def quote_dailyfreight(cart_cbm: float, destination: dict, override_key: Optional[str] = None) -> Optional[dict]:
    """
    Dailyfreight quote with hub-and-spoke zones and volume-tier discounts.

    The city alias map routes the customer's suburb to a specific rate key like
    'cromwell_z1' (Cromwell metro) or 'cromwell_z5' (Wanaka, Queenstown, etc.).
    Per-m³ rate drops with cart size — three tiers in the rate card.

    Formula: MAX(base, per_m3_tier × cart_cbm) × FAF × GST
    override_key forces a specific rate key (used by the postcode/province fallback).
    """
    rates = _load_carrier_rates()
    rate_key = override_key or _normalise_city(destination.get("city", ""))
    rate = rates["dailyfreight"]["rates"].get(rate_key)
    if not rate:
        return None
    tier_idx = _df_tier_index(cart_cbm)
    per_m3 = rate["tiers"][tier_idx]
    base = rate["base"]
    raw_excl = max(base, per_m3 * cart_cbm)
    raw_cost = raw_excl * FAF_MULTIPLIER * GST_MULTIPLIER
    tier_label = ["<5m³", "5-10m³", "≥10m³"][tier_idx]
    return {
        "carrier":   "Dailyfreight",
        "service":   "LCL Palletised",
        "raw_cost":  round(raw_cost, 2),
        "_source":   f"{rate_key} tier {tier_label}: MAX({base}, {per_m3} × {cart_cbm:.3f}) × FAF × GST",
    }


async def calculate_freight(items: list, destination: dict, debug: bool = False) -> dict:
    """
    Main entry: get quotes from all three carriers, pick cheapest, apply markup.
    Returns a dict suitable for Shopify carrier service response.
    """
    cart_cbm = _total_cbm(items)
    quotes = []

    # Castle Parcels (live)
    cp = await quote_castle_parcels(items, destination)
    if cp:
        quotes.append(cp)

    # Mainfreight (formula)
    mf = quote_mainfreight(cart_cbm, destination)
    if mf:
        quotes.append(mf)

    # Dailyfreight (formula)
    df = quote_dailyfreight(cart_cbm, destination)
    if df:
        quotes.append(df)

    # Postcode/province fallback: if neither formula carrier matched the city
    # name (e.g. Google put a suburb in the city field), resolve by postcode
    # then region so we never drop to no_carrier_match for a real NZ address.
    # Only triggers when both MF and DF missed, so existing pricing is untouched.
    if not mf and not df:
        fb = _fallback_keys(destination)
        if fb:
            mf_key, df_key = fb
            mf = quote_mainfreight(cart_cbm, destination, override_key=mf_key)
            if mf:
                mf["_source"] += "  [postcode/province fallback]"
                quotes.append(mf)
            df = quote_dailyfreight(cart_cbm, destination, override_key=df_key)
            if df:
                df["_source"] += "  [postcode/province fallback]"
                quotes.append(df)

    if not quotes:
        return {
            "success": False,
            "error":   "no_carrier_match",
            "cart_cbm": cart_cbm,
            "destination_city": destination.get("city", ""),
            "quotes": [],
        }

    # Pick cheapest of available
    cheapest = min(quotes, key=lambda q: q["raw_cost"])
    customer_price = round(cheapest["raw_cost"] * NED_MARKUP, 2)

    result = {
        "success":         True,
        "cart_cbm":        round(cart_cbm, 4),
        "chosen_carrier":  cheapest["carrier"],
        "chosen_service":  cheapest["service"],
        "raw_cost":        cheapest["raw_cost"],
        "ned_markup":      NED_MARKUP,
        "customer_price":  customer_price,
    }
    if debug:
        result["all_quotes"] = quotes
        result["destination"] = destination
    return result
