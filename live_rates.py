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
from pathlib import Path
from typing import Optional

import httpx

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
    """Map a customer city to a rate-card city via alias table."""
    rates = _load_carrier_rates()
    city_key = (city or "").strip().lower()
    if not city_key:
        return ""
    aliases = rates.get("city_aliases", {})
    return aliases.get(city_key, city_key)


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


def _build_packages(items: list) -> list:
    """
    Convert Shopify cart items into GoSweetSpot package list.
    Each item with quantity N → N separate packages.
    Item weight comes from Shopify (we set it = CBM in kg).
    """
    pkgs = []
    for item in items:
        qty = int(item.get("quantity", 1))
        # Shopify carrier service sends "grams" (in cart payload it's the line item's grams field per unit)
        grams = float(item.get("grams", 0) or 0)
        weight_kg = grams / 1000.0
        if weight_kg <= 0:
            weight_kg = 0.001  # 1g fallback
        L, W, H = _cube_dimensions_cm(weight_kg)
        for _ in range(qty):
            pkgs.append({
                "Name": "Carton",
                "Length": L, "Width": W, "Height": H,
                "Kg": round(weight_kg, 3),
                "Type": "Box",
            })
    return pkgs


def _total_cbm(items: list) -> float:
    """Sum cart CBM (weight in kg = CBM convention)."""
    total = 0.0
    for item in items:
        qty = int(item.get("quantity", 1))
        grams = float(item.get("grams", 0) or 0)
        total += (grams / 1000.0) * qty
    return total


async def quote_castle_parcels(items: list, destination: dict) -> Optional[dict]:
    """
    Live quote from GoSweetSpot (Castle Parcels / Post Haste).
    Returns dict with raw NED cost (8% GSS markup stripped), or None on failure.
    """
    if not GSS_ACCESS_KEY or not GSS_SITE_ID:
        return None

    dest_payload = {
        "Name": destination.get("name", "Customer"),
        "Address": {
            "StreetAddress": destination.get("address1") or destination.get("address", ""),
            "Suburb":        destination.get("city", ""),  # GSS treats Suburb loosely
            "City":          destination.get("city", ""),
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
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.post(GSS_URL, json=payload, headers=headers)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None

    options = data.get("Available", [])
    if not options:
        return None

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


def quote_mainfreight(cart_cbm: float, destination: dict) -> Optional[dict]:
    """
    Compute Mainfreight cost from cached rate card.
    Formula: MAX(min_charge, base + per_m3 × cart_cbm). Result is excl GST & excl FAF.
    """
    rates = _load_carrier_rates()
    city = _normalise_city(destination.get("city", ""))
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


def quote_dailyfreight(cart_cbm: float, destination: dict) -> Optional[dict]:
    """
    Compute Dailyfreight cost from cached rate card.
    Formula: MAX(min, per_m3 × cart_cbm). Result is excl GST & excl FAF.
    """
    rates = _load_carrier_rates()
    city = _normalise_city(destination.get("city", ""))
    rate = rates["dailyfreight"]["rates"].get(city)
    if not rate:
        return None
    per = rate["per_m3"]
    minc = rate["min"]
    raw_excl = max(minc, per * cart_cbm)
    raw_cost = raw_excl * FAF_MULTIPLIER * GST_MULTIPLIER
    return {
        "carrier":   "Dailyfreight",
        "service":   "LCL Palletised",
        "raw_cost":  round(raw_cost, 2),
        "_source":   f"Formula: MAX({minc}, {per} × {cart_cbm:.3f}) × FAF × GST",
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
