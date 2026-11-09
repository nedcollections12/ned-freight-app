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
import logging
import math
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

import alerts

logger = logging.getLogger("ned_freight")

# Shared async HTTP client — reused across every quote so we don't pay a fresh
# TLS + Cloudflare handshake to GoSweetSpot/Mainfreight on each checkout. A cold
# connection from our Singapore region costs ~1-2s; on slow provincial/inter-island
# lanes that pushed GSS past its 9s budget and silently dropped Castle Parcels
# (the ~31% CP drop-out we saw concentrated on South-Island routes). Keep-alive
# holds the connection warm between checkouts; per-request timeouts stay at the
# call sites below.
_HTTP_LIMITS = httpx.Limits(max_keepalive_connections=20, keepalive_expiry=90.0)
_client = httpx.AsyncClient(limits=_HTTP_LIMITS)


async def aclose_http() -> None:
    """Close the shared HTTP client — called on app shutdown."""
    await _client.aclose()


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

# Mainfreight Rating API — covers both Dailyfreight (LCL) and Mainfreight (M2H)
# via the same endpoint with different account/serviceLevel codes.
MAINFREIGHT_API_KEY = os.environ.get("MAINFREIGHT_API_KEY", "")
MAINFREIGHT_URL     = "https://api.mainfreight.com/transport/1.0/customer/rate?region=NZ"
MF_ACCOUNT_DF  = "NEDCOLDF"   # Ned Collections Dailyfreight (LCL)
MF_ACCOUNT_MF  = "NEDCOLCHC"  # Ned Collections Mainfreight 2 Home (M2H) — ex-Christchurch
MF_ACCOUNT_AKL = "NEDCOLAKL"  # Ned Collections Mainfreight 2 Home (M2H) — ex-Auckland 3PL

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

# Auckland 3PL warehouse — origin for NEDCOLAKL (Mainfreight ex-Auckland) quotes.
# Fields match the Mainfreight Rating API payload shape (suburb/city/postCode).
ORIGIN_AKL_ADDRESS = {
    "suburb":      os.environ.get("AKL_ORIGIN_SUBURB", "Mangere"),
    "city":        os.environ.get("AKL_ORIGIN_CITY", "Auckland"),
    "postCode":    os.environ.get("AKL_ORIGIN_POSTCODE", "2022"),
    "countryCode": os.environ.get("AKL_ORIGIN_COUNTRY", "NZ"),
}

# Multipliers — tunable via env vars
FAF_MULTIPLIER = float(os.environ.get("FAF_MULTIPLIER", "1.30"))   # Fuel adjustment
GST_MULTIPLIER = float(os.environ.get("GST_MULTIPLIER", "1.15"))   # NZ GST 15%
NED_MARKUP     = float(os.environ.get("NED_MARKUP", "1.10"))       # NED's margin

# GoSweetSpot's "Cost" field already includes their 8% markup (NED pays this).
# Strip it to get the carrier's true cost, then re-apply NED markup.
GSS_BUILTIN_MARKUP = 1.08

# ── Rural delivery surcharge buffer ───────────────────────────────────────────
# GoSweetSpot's rating QUOTE under-flags rural addresses: it returns
# IsRuralDelivery=False for genuine rural-delivery zones (confirmed on Mangawhai
# 0505 — no address framing flips it), so the quote OMITS the rural surcharge that
# Castle/Kiwi then add on the actual freight INVOICE. Result: we charged below our
# own freight cost on rural lanes (carrier_reconciliation_CP.csv: Eureka -$19.26,
# Pyes Pa -$5.57, Te Awamutu -$5.02...). When the destination postcode is a known
# rural-delivery zone but GSS didn't flag it, add Castle's rural surcharge to the CP
# cost basis (NED markup then applies on top, same as the rest of the cost).
#
# Castle charges rural delivery PER CARTON. Measured 9 Nov 2026 from GSS itself
# (Morrinsville 3300 urban vs 3371 RD1, same cart): Post Haste adds $9.17 per carton
# in GSS's Cost (1 carton +$9.17, 3 cartons +$27.51) = $8.49/carton after stripping
# GSS's 8% — consistent with the rate card's $7.09 ex-GST + FAF. Kiwi Express Oversize
# read ~$9.50-$19/carton (less consistent) at first. Re-measured 9 Nov 2026 on clean
# same-zone pairs (Tauranga 3112 vs 3187, Rangiora 7400 vs 7471; 1x0.20, 2x0.20, 1x0.35
# m³): Kiwi Express Oversize adds $9.11 in GSS's Cost PER CONSIGNMENT (2 cartons still
# +$9.11) = $8.44 after stripping GSS's 8%. (Morrinsville/Te Aroha diffs were bigger
# only because 3371/3392 also sit in a dearer zone.) So oversize is per delivery.
# When GSS DOES flag rural, the fee is already in its Cost and nothing is added.
# Override per carton (incl GST) via env. Kill switch: RURAL_SURCHARGE_ENABLED=0.
RURAL_SURCHARGE_ENABLED  = os.environ.get("RURAL_SURCHARGE_ENABLED", "1") != "0"
RURAL_SURCHARGE_COURIER  = float(os.environ.get("RURAL_SURCHARGE_COURIER", "8.49"))   # incl GST, per carton
RURAL_SURCHARGE_OVERSIZE = float(os.environ.get("RURAL_SURCHARGE_OVERSIZE", "8.44"))  # incl GST, per consignment

# Postcodes that ARE rural-delivery but GSS returns IsRuralDelivery=False for.
# Seeded with the one lane we've confirmed (Mangawhai); extend from Castle invoices
# or an NZ Post rural-delivery postcode dataset. Env RURAL_POSTCODES_EXTRA adds
# comma-separated 4-digit codes without a code change (e.g. "3112,3287,3800").
_RURAL_POSTCODES = {"0505"}  # Mangawhai (Kaipara, Northland)
_RURAL_POSTCODES |= {
    p.strip() for p in os.environ.get("RURAL_POSTCODES_EXTRA", "").split(",") if p.strip()
}


_RD_RE = re.compile(r"\bR\.?\s?D\.?\s*\d{1,2}\b", re.IGNORECASE)


def _is_rural_address(destination: dict) -> bool:
    """
    True for an NZ rural-delivery address: an "RD n" line (NZ Post's rural-delivery
    marker, e.g. "RD1" / "R.D. 2") anywhere in the address, or a known-rural postcode.
    """
    text = " ".join(str(destination.get(k) or "") for k in ("address1", "address2", "address3", "city"))
    if _RD_RE.search(text):
        return True
    pc4 = re.sub(r"\D", "", (destination.get("postal_code") or destination.get("zip") or ""))[:4]
    return pc4 in _RURAL_POSTCODES


def _rural_surcharge(destination: dict, is_rural_flagged: bool, service_or_carrier: str,
                     n_cartons: int = 1) -> float:
    """
    Per-carton rural fee to add to a Castle Parcels cost when the address is rural
    (an "RD n" line or a known-rural postcode) but GSS didn't flag it. Returns 0 when
    disabled, when GSS already flagged rural (fee is then already in the Cost), or when
    the address isn't rural. Courier is charged per carton; oversize once per delivery.
    """
    if not RURAL_SURCHARGE_ENABLED or is_rural_flagged or not _is_rural_address(destination):
        return 0.0
    label = (service_or_carrier or "").lower()
    if "oversize" in label:
        return round(RURAL_SURCHARGE_OVERSIZE, 2)          # per consignment
    return round(RURAL_SURCHARGE_COURIER * max(1, int(n_cartons)), 2)  # per carton

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


# ── Service-routing rules (live-only pricing + oversize / pallet) ─────────────
# All of this is feature-flagged so it ships dark: with the flags off, quoting
# behaves EXACTLY as before. Nothing here may ever produce a bare "no rate" for a
# cart that would otherwise have quoted — every filter falls back to the full quote
# set rather than block checkout (see _apply_service_rules).

# LIVE_ONLY: make the live carrier APIs authoritative and retire the static
# rate-card formula fallbacks from the pricing path. An unrecoverable live miss
# then falls through to the existing "Freight — Contact Us" (never a 500).
LIVE_ONLY = os.environ.get("LIVE_ONLY", "0") == "1"

# PALLET_RULE_ENABLED: apply NED's service rules (oversize -> Two-Man; small cart
# -> no LCL pallet). Now ON by default — leaving it off is the Dailyfreight undercharge
# (DF pallet rate winning non-pallet carts). Set the env var to "0" only to temporarily
# revert to pure cheapest-wins.
PALLET_RULE_ENABLED = os.environ.get("PALLET_RULE_ENABLED", "1") == "1"

# Below this cart CBM we don't put an order on an LCL pallet by itself — drop
# Dailyfreight so a courier / Mainfreight Two-Man wins instead.
PALLET_MIN_CBM = float(os.environ.get("PALLET_MIN_CBM", "0.8"))

# COURIER FALLBACK — when Castle Parcels (GSS) fails to return a quote for a small,
# courier-sized cart, the Mainfreight/Dailyfreight freight minimums (~$90-190) are
# nonsensical for courier goods and lose the sale (a known ~20% GSS drop-out on slow
# lanes). Instead charge a flat % of the order's GST-EXCLUSIVE value — close to NED's
# average courier cost — so the customer always sees a sane rate. Fires ONLY for a
# non-oversize cart below PALLET_MIN_CBM with NO courier quote; large/pallet carts
# keep their real MF/DF freight rate. Kill switch: COURIER_FALLBACK_ENABLED=0.
COURIER_FALLBACK_ENABLED = os.environ.get("COURIER_FALLBACK_ENABLED", "1") != "0"
COURIER_FALLBACK_PCT = float(os.environ.get("COURIER_FALLBACK_PCT", "0.08"))  # 8% of ex-GST order
COURIER_FALLBACK_MIN = float(os.environ.get("COURIER_FALLBACK_MIN", "0"))     # optional $ floor (0 = off)

# Volume-based courier fallback (default). The % of order value had two problems: it
# tracks price, not size (median 75% off the real Castle cost across the last 2,000
# quotes), and the rate request doesn't say whether the cart is trade, so B2B carts
# (prices already ex-GST) had GST stripped twice (NED4200). Instead estimate Castle's
# cost from cart CBM, fitted 9 Nov 2026 on 1,071 real non-rural GSS quotes ex-CHCH
# (raw_cost incl GST, GSS 8% stripped): North Island ≈ $8.55 + $340/m³ (median error
# 7%), South Island ≈ $9.50 + $66/m³ (28%). NED_MARKUP and the rural fee go on top,
# exactly like a live CP quote. Set COURIER_FALLBACK_MODE=pct to go back to the %.
COURIER_FALLBACK_MODE = os.environ.get("COURIER_FALLBACK_MODE", "cbm").lower()
COURIER_FB_NI_BASE   = float(os.environ.get("COURIER_FB_NI_BASE", "8.55"))
COURIER_FB_NI_PER_M3 = float(os.environ.get("COURIER_FB_NI_PER_M3", "340"))
COURIER_FB_SI_BASE   = float(os.environ.get("COURIER_FB_SI_BASE", "9.50"))
COURIER_FB_SI_PER_M3 = float(os.environ.get("COURIER_FB_SI_PER_M3", "66"))


def _is_south_island(destination: dict) -> Optional[bool]:
    """South Island from the postcode (7xxx-9xxx), else None when unknown."""
    pc = re.sub(r"\D", "", (destination.get("postal_code") or destination.get("zip") or ""))[:4]
    if len(pc) == 4:
        return pc[0] in "789"
    return None


def _cbm_courier_cost(cart_cbm: float, destination: dict) -> tuple:
    """Estimated Castle Parcels raw cost (incl GST) for a courier cart, and a label.
    Unknown island prices as North Island (the dearer lane from CHCH)."""
    si = _is_south_island(destination)
    if si:
        return COURIER_FB_SI_BASE + COURIER_FB_SI_PER_M3 * cart_cbm, "South Island"
    return COURIER_FB_NI_BASE + COURIER_FB_NI_PER_M3 * cart_cbm, ("North Island" if si is False else "island unknown")

# Mainfreight Rating API resilience: one fast retry on a transient miss (timeout /
# 5xx / connection reset) so a single blip doesn't drop MF/DF to "Contact Us" under
# LIVE_ONLY. Under LIVE_ONLY the timeout tightens to 4s so call + retry still fits
# Shopify's ~10s window (MF and DF run in parallel with the 9s GSS call, so the
# envelope is unchanged). With LIVE_ONLY OFF the defaults are the ORIGINAL behaviour
# (single 7s call, no retry) — so a flags-off deploy changes nothing at checkout;
# the formula fallback still backstops a miss. Explicit env vars override either way.
# 4xx and 200-without-charge are permanent — never retried.
MF_TIMEOUT  = float(os.environ.get("MF_TIMEOUT", "4.0" if LIVE_ONLY else "7.0"))
MF_RETRIES  = int(os.environ.get("MF_RETRIES", "1" if LIVE_ONLY else "0"))

OVERSIZE_FILE = BASE_DIR / "data" / "oversize_items.json"
_oversize_ids_cache = None


def _load_oversize_ids() -> set:
    """
    Shopify product_ids flagged as >1.5m long — can't go on a pallet AND can't go
    by courier, so a cart containing one is forced onto Mainfreight M2H two-man.
    Curated in-app list (data/oversize_items.json), editable via the admin UI.
    Fail-safe: any read/parse error -> empty set (no oversize forcing) so a bad file
    never blocks checkout.
    """
    global _oversize_ids_cache
    if _oversize_ids_cache is None:
        try:
            data = json.loads(OVERSIZE_FILE.read_text()) if OVERSIZE_FILE.exists() else []
            _oversize_ids_cache = {str(e.get("product_id")) for e in data
                                   if isinstance(e, dict) and e.get("product_id")}
        except Exception:
            logger.exception("oversize_items.json unreadable — treating as empty (no oversize forcing)")
            _oversize_ids_cache = set()
    return _oversize_ids_cache


def reload_oversize_ids():
    """Force reload of the oversize list from disk (for the admin /api/oversize writes)."""
    global _oversize_ids_cache
    _oversize_ids_cache = None


def _cart_has_oversize(items: list) -> bool:
    """True if any cart line is on the oversize (>1.5m) list."""
    ids = _load_oversize_ids()
    if not ids:
        return False
    return any(str(i.get("product_id") or "") in ids for i in items)


# Carrier-class predicates — quotes carry {carrier, service}. Kept string-based to
# match the live/formula quote shapes already produced below.
def _is_courier_quote(q: dict) -> bool:
    c = q.get("carrier", "") or ""
    return any(t in c for t in ("Haste", "Castle", "Kiwi"))


def _is_pallet_quote(q: dict) -> bool:
    """Dailyfreight = LCL palletised service."""
    return (q.get("carrier", "") or "") == "Dailyfreight"


def _is_twoman_quote(q: dict) -> bool:
    """Mainfreight = M2H two-man-into-home service."""
    return (q.get("carrier", "") or "") == "Mainfreight"


def _apply_service_rules(quotes: list, items: list, cart_cbm: float) -> list:
    """
    Filter carrier quotes by NED's service rules before cheapest-wins selection:
      1. Any item >1.5m (oversize list) -> force Mainfreight M2H two-man.
      2. Else cart CBM < PALLET_MIN_CBM -> this order is NOT a pallet load, so the
         Dailyfreight LCL (pallet) rate must not be used: drop it so a courier (CP) or
         Mainfreight two-man wins instead. This DELIBERATELY charges the real non-pallet
         rate even when the pallet quote was cheaper — the LCL pallet price only holds for
         a genuinely palletised consignment, and applying it to small orders is the
         undercharge this rule exists to stop (DF was winning ~52% of sub-0.8m³ carts).
      3. Else (cart CBM >= PALLET_MIN_CBM) -> pallet is legitimate; all carriers eligible.
    FAIL-SAFE: a filter never empties a non-empty set — if DF was the ONLY carrier that
    quoted this lane, keep it (a rate always beats no rate at checkout).
    """
    if not quotes:
        return quotes
    if _cart_has_oversize(items):
        twoman = [q for q in quotes if _is_twoman_quote(q)]
        return twoman or quotes
    if cart_cbm < PALLET_MIN_CBM:
        # Below the pallet floor: exclude Dailyfreight (LCL pallet) outright. Pricier
        # non-pallet is the CORRECT charge for a non-pallet order; only fall back to the
        # full set (DF included) if dropping it would leave no rate at all.
        non_pallet = [q for q in quotes if not _is_pallet_quote(q)]
        return non_pallet or quotes
    return quotes


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
    # Ranges checked 9 Nov 2026 against ~7,200 NZ Shopify order/customer addresses
    # plus the rate log (town typed vs postcode). Where a town has a city_aliases
    # entry, its range MUST map to the same hub, or _resolve_destination_city will
    # "correct" that town to the wrong place. Still hand-built: an NZ Post
    # postcode→locality list is the long-term source of truth.
    (100,  409,  "whangarei",        "whangarei"),        # Northland (Whangarei, Dargaville, Kerikeri, Paihia)
    (410,  499,  "kaitaia",          "kaitaia"),          # Far North (Kaitaia)
    (500,  599,  "whangarei",        "whangarei"),        # Kaipara
    (600,  999,  "auckland",         "auckland_z1"),      # North Shore / Rodney / West Auckland
    (1010, 1839, "auckland",         "auckland_z1"),      # Auckland central / isthmus / east + PO boxes
    (1840, 1999, "auckland",         "auckland_z9"),      # Waiheke Island PO boxes / RD (1841, 1971)
    (2000, 2999, "auckland",         "auckland_z1"),      # South Auckland / Manukau / Papakura / Pukekohe
    (3010, 3099, "rotorua",          "rotorua"),          # Rotorua
    (3110, 3119, "tauranga",         "tauranga"),         # Tauranga / Mt Maunganui / Papamoa / Te Puke
    (3120, 3128, "rotorua",          "whakatane"),        # Whakatane 3120 / Ohope 3121 / Opotiki 3122 / Kawerau 3127
    (3129, 3157, "tauranga",         "tauranga"),         # Katikati 3129 + Tauranga PO boxes (3150)
    (3158, 3169, "rotorua",          "whakatane"),        # Eastern BoP PO boxes
    (3170, 3189, "tauranga",         "tauranga"),         # Western BoP RDs: Katikati, Omokoroa, Te Puna, Pyes Pa, Te Puke (3173, 3187)
    (3190, 3199, "rotorua",          "whakatane"),        # Eastern BoP RDs: Whakatane, Matata, Waimana, Opotiki
    (3200, 3299, "hamilton",         "hamilton_z1"),      # Hamilton
    # 33xx is mixed: Taupō/Tūrangi sit between Morrinsville / Te Aroha rural-delivery
    # codes. A blanket 3330-3399 → Taupō misrated Morrinsville RD 3371 (NED4200).
    (3300, 3329, "hamilton",         "hamilton_z3"),      # Morrinsville 3300 / Te Aroha 3320
    (3330, 3369, "taupo",            "taupo"),            # Taupo 3330 / Turangi 3334 / PO boxes
    (3370, 3376, "hamilton",         "hamilton_z3"),      # Morrinsville RDs (3371-3375)
    (3377, 3389, "taupo",            "taupo"),            # Taupo / Turangi RDs
    (3390, 3399, "hamilton",         "hamilton_z3"),      # Te Aroha RDs (3391-3393)
    (3400, 3499, "hamilton",         "hamilton_z1"),      # Cambridge / Matamata / Putaruru / Tokoroa
    (3500, 3699, "thames",           "thames"),           # Thames / Coromandel + Hauraki: Paeroa 3600, Waihi 3610, Whangamata 3620
    (3700, 3899, "hamilton",         "hamilton_z3"),      # Huntly / Ngaruawahia / Te Kauwhata + Te Awamutu 3800 / Ohaupo
    (3900, 3909, "hamilton",         "hamilton_z3"),      # Otorohanga 3900
    (3910, 3971, "hamilton",         "hamilton_z10"),     # Te Kuiti 3910 / Piopio 3912 / Taumarunui 3920 + RDs
    (3972, 3979, "hamilton",         "hamilton_z3"),      # Otorohanga RDs (3973)
    (3980, 3999, "hamilton",         "hamilton_z10"),     # Taumarunui RDs (Retaruke 3990)
    (4010, 4099, "gisborne",         "gisborne"),         # Gisborne
    (4100, 4299, "napier",           "napier"),           # Hawke's Bay (Napier, Hastings, Waipukurau)
    (4300, 4399, "new plymouth",     "new plymouth"),     # Taranaki (New Plymouth, Stratford)
    (4400, 4499, "palmerston north", "palmerston north"), # Manawatu (PN)
    (4500, 4699, "wanganui",         "wanganui"),         # Whanganui + South Taranaki (Hawera)
    (4700, 4999, "palmerston north", "palmerston north"), # Feilding 4702 / Marton / Taihape / Ashhurst / Foxton / Pahiatua 4910 / Dannevirke 4930
    (5010, 5099, "wellington",       "wellington"),       # Hutt / Porirua / Kapiti
    (5100, 5499, "wellington",       "wellington"),       # Wellington-region PO boxes / Kapiti RDs (5391 Waikanae)
    (5500, 5599, "levin",            "levin"),            # Horowhenua (Levin, Otaki)
    (5700, 5899, "wellington",       "masterton"),        # Wairarapa: Featherston 5710 / Martinborough 5711 / Greytown 5712 / Carterton 5713 / Masterton
    (6010, 6999, "wellington",       "wellington"),       # Wellington city + PO boxes
    (7000, 7199, "nelson",           "nelson"),           # Nelson / Tasman (Mapua 7005, Richmond, Motueka)
    (7200, 7399, "blenheim",         "blenheim"),         # Marlborough (Blenheim, Picton) + Kaikoura / Hanmer
    (7400, 7699, "christchurch",     "christchurch"),     # North Canterbury (Rangiora, Amberley, Oxford)
    (7700, 7799, "christchurch",     "ashburton"),        # Mid Canterbury (Ashburton)
    (7800, 7899, "greymouth",        "greymouth"),        # West Coast (Greymouth, Hokitika, Westport)
    (7900, 7999, "timaru",           "timaru"),           # South Canterbury (Timaru)
    (8010, 8999, "christchurch",     "christchurch"),     # Christchurch city + PO boxes + Banks Peninsula RDs (8971)
    (9000, 9299, "dunedin",          "dunedin"),          # Dunedin + Clutha (Balclutha 9230, Kaitangata 9210, Milton)
    (9300, 9399, "cromwell",         "cromwell_z1"),      # Central Otago (Cromwell, Alexandra, Queenstown, Wanaka)
    (9400, 9499, "oamaru",           "oamaru"),           # Waitaki (Oamaru)
    (9532, 9532, "cromwell",         "cromwell_z5"),      # Lawrence (matches its city alias)
    (9583, 9589, "dunedin",          "dunedin"),          # Clutha RDs (Clinton, Waiwera South)
    (9591, 9591, "cromwell",         "cromwell_z5"),      # Lawrence RD (Craigellachie)
    (9600, 9699, "invercargill",     "invercargill"),     # Te Anau / western Southland RDs (Otautau, Isla Bank)
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


# Fine-grained postcode → town overrides for places the carriers mis-geocode when
# the customer types the wrong city. Mainfreight/Dailyfreight/GSS all rate off the
# city TEXT, so "129 Estuary Drive, Auckland, 0505" (actually Mangawhai, rural
# Northland) was rated as Auckland metro — ~half the true two-man rate. Sending the
# real town name gets the correct rate (Mangawhai M2H $281 vs Auckland $120).
# Only needed where the region hub (_fallback_keys) is itself too coarse to be
# accurate; extend from the rate log (or an NZ Post postcode dataset) as cases surface.
_POSTCODE_TOWN = {
    "0505": "Mangawhai",   # Kaipara / Northland — commonly typed as "Auckland"
}

# Recognised NZ city/hub names (from the postcode + province rate keys). We only
# correct a destination when the typed city is one of THESE — i.e. a real city that
# disagrees with its postcode — so a bare suburb typed in the city field (e.g.
# "Takapuna") is left untouched and keeps today's behaviour.
_KNOWN_HUBS = (
    {k for _, _, k, _ in _POSTCODE_KEYS}
    | {mf for mf, _ in _PROVINCE_KEYS.values()}
)


_FUZZY_CUTOFF = float(os.environ.get("TOWN_FUZZY_CUTOFF", "0.88"))


def _town_vocab() -> dict:
    """Known town spellings → rate hub (alias table keys, hub names, postcode towns)."""
    rates = _load_carrier_rates()
    vocab = {}
    for name, key in (rates.get("city_aliases") or {}).items():
        if not name.startswith("_") and isinstance(key, str):
            vocab[name] = re.sub(r"_z\d+$", "", key)
    for hub in _KNOWN_HUBS:
        vocab.setdefault(hub, hub)
    for town in _POSTCODE_TOWN.values():
        t = _strip_diacritics(town.lower())
        vocab.setdefault(t, re.sub(r"_z\d+$", "", _normalise_city(t)))
    return vocab


def _fuzzy_town(city: str, destination: dict) -> Optional[str]:
    """
    Return the correctly-spelt town for a typo'd city, or None. Conservative:
      * only when the typed text is NOT already a known town/alias,
      * the closest known spelling scores >= _FUZZY_CUTOFF (Morinsville→Morrinsville
        0.96; Te Puke vs Te Puna 0.86 is NOT merged), and
      * that town's rate hub matches the postcode's hub (or the region's, if the
        postcode isn't in the table) — so a typo is never "corrected" across the country.
    """
    import difflib
    typed = _strip_diacritics(city.strip().lower())
    vocab = _town_vocab()
    if not typed or typed in vocab:
        return None
    match = difflib.get_close_matches(typed, list(vocab), n=1, cutoff=_FUZZY_CUTOFF)
    if not match:
        return None
    area = _fallback_keys({"postal_code": destination.get("postal_code") or destination.get("zip") or "",
                           "province": destination.get("province") or ""})
    if not area or re.sub(r"_z\d+$", "", area[0]) != vocab[match[0]]:
        return None
    return match[0].title()


def _resolve_destination_city(destination: dict) -> tuple:
    """
    Correct a destination whose typed city contradicts its postcode, so carriers
    rate the REAL location. Postcode is Shopify-validated and far more reliable than
    the free-typed city field. Conservative by design — only overrides when:
      * the postcode resolves to a rate hub, AND
      * the typed city ALSO resolves to a *recognised* hub (not a random suburb), AND
      * those two hubs are in different places.
    Returns (destination_to_use, correction) where correction is (typed, used) or None.
    Never mutates the caller's dict (returns a corrected copy) so the rate log still
    records what the customer actually entered.
    """
    pc_raw = (destination.get("postal_code") or destination.get("zip") or "").strip()
    city = (destination.get("city") or "").strip()
    if not (pc_raw and city):
        return destination, None

    # Misspelt town ("Morinsville", "Christchurvh"): GSS and the Mainfreight API geocode
    # off the town text and return nothing for a typo. Snap it to the closest known town
    # when that town sits in the same area as the postcode (or region).
    fixed = _fuzzy_town(city, destination)
    if fixed:
        corrected = dict(destination)
        corrected["city"] = fixed
        corrected["suburb"] = fixed
        return corrected, (city, fixed)

    fb = _fallback_keys({"postal_code": pc_raw})   # postcode-only resolution
    if not fb:
        return destination, None
    mf_pc = re.sub(r"_z\d+$", "", fb[0])
    city_key = re.sub(r"_z\d+$", "", _normalise_city(city))
    if city_key == mf_pc:
        return destination, None                   # city agrees with postcode — trust it
    if city_key not in _KNOWN_HUBS:
        return destination, None                   # typed a suburb/unknown — leave as-is
    # Confident conflict: a real city that disagrees with its own postcode.
    pc4 = re.sub(r"\D", "", pc_raw)[:4]
    town = _POSTCODE_TOWN.get(pc4) or mf_pc.title()
    corrected = dict(destination)
    corrected["city"] = town
    corrected["suburb"] = town
    return corrected, (city, town)


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


# Castle Parcels' courier service tops out around a 0.13 m³ cube (~51 cm side);
# above that GoSweetSpot only returns oversize/freight options (verified via live
# GSS probes to Wanaka: courier holds to 0.13, flips to Kiwi Express Oversize at
# 0.14). We use this ONE number two ways in _build_packages:
#   1. Courier carton cap — pooled small items are split into cartons no larger
#      than this, so each parcel qualifies for cheap courier pricing.
#   2. Small-vs-large divide — any single unit bigger than this physically can't
#      be a courier parcel (dining chairs, big furniture), so it ships as its own
#      carton and naturally routes to the Mainfreight/Dailyfreight formulas.
_COURIER_MAX_CBM = float(os.environ.get("COURIER_MAX_CBM", "0.13"))

# Packing factor: real cartons are a little larger than the raw sum of item CBMs
# (carton walls, items don't tessellate perfectly, void fill, etc.). Genuine
# packing overhead is ~10-20%, so 1.15 is the default.
#
# NOTE: this was previously 1.6 (+60%), reverse-engineered from a SINGLE product
# (the 24× ACSR case: raw 0.048 m³ vs real cartons 0.0784 m³). That gap wasn't
# packing air — that SKU's per-unit CBM was understated — so applying 1.6 to the
# whole catalogue over-inflated products with accurate CBM (e.g. mugs) and pushed
# them over Castle Parcels' ~0.16 m³ courier ceiling into pricier oversize/LCL.
# The correct long-term fix is accurate per-product CBM, not a large blanket
# factor. Tunable via PACKING_FACTOR env var.
_PACKING_FACTOR = float(os.environ.get("PACKING_FACTOR", "1.15"))


# ── Per-product carton map ───────────────────────────────────────────────────
# Most furniture ships as one or more real cartons whose TRUE dimensions decide
# courier eligibility — but Shopify stores only a single lumped CBM per variant,
# which _build_packages would otherwise model as one fat cube and push over the
# courier cap (so Castle Parcels never quotes and the order falls to the pricier
# pallet / two-man rate — the Nova problem). This curated map (data/cartons.json,
# editable in the admin UI) gives GoSweetSpot the real cartons for such products.
#
# Keyed by Shopify SKU or product_id (SKU wins). Two input modes per product:
#   • cartons: [{length_cm,width_cm,height_cm, kg?}, …]  — explicit cartons, OR
#   • cartons: [{cbm}, …]  /  top-level  cbm: <m³>       — cube(s) from a CBM.
# Fail-safe: a bad/missing file → empty map, so every product simply keeps the
# cube-from-CBM behaviour and a checkout quote is never blocked.
# Stored on the Render persistent disk (RATE_LOG_DIR, e.g. /var/data) so cartons the
# warehouse team enter survive redeploys — same disk the rate log uses. Falls back to
# the committed repo copy (baseline) on a freshly-provisioned disk, and to ./data
# locally where RATE_LOG_DIR is unset.
_PERSIST_DIR = Path(os.environ.get("RATE_LOG_DIR", str(BASE_DIR / "data")))
CARTONS_FILE = _PERSIST_DIR / "cartons.json"
_CARTONS_REPO = BASE_DIR / "data" / "cartons.json"
_carton_map_cache = None


def _read_cartons_file() -> list:
    """Raw entries: prefer the persistent disk, else the repo baseline, else empty."""
    path = CARTONS_FILE if CARTONS_FILE.exists() else _CARTONS_REPO
    return json.loads(path.read_text()) if path.exists() else []


def read_carton_entries() -> list:
    """Raw carton-map entries for the admin API (never raises)."""
    try:
        return _read_cartons_file()
    except Exception:
        logger.exception("cartons.json unreadable")
        return []


def write_carton_entries(data: list) -> None:
    """Persist carton-map entries to the Render disk and refresh the in-memory map."""
    CARTONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CARTONS_FILE.write_text(json.dumps(data, indent=2))
    reload_carton_map()


def _carton_to_package(c: dict) -> Optional[dict]:
    """Normalise one carton spec → a GoSweetSpot package dict (None if unusable)."""
    L, W, H = c.get("length_cm"), c.get("width_cm"), c.get("height_cm")
    if L and W and H:
        L, W, H = float(L), float(W), float(H)
        cbm = (L * W * H) / 1_000_000.0                  # cm³ → m³
        kg = float(c.get("kg") or 0) or round(cbm, 3)    # weight=CBM convention unless a real kg is given
    else:
        cbm = float(c.get("cbm") or 0)
        if cbm <= 0:
            return None
        L, W, H = _cube_dimensions_cm(cbm)
        kg = round(cbm, 3)
    return {"Name": "Carton", "Length": round(L, 1), "Width": round(W, 1),
            "Height": round(H, 1), "Kg": round(kg, 3), "Type": "Box"}


def _entry_to_packages(entry: dict) -> list:
    """A carton-map entry → GSS package dicts for ONE unit of the product."""
    cartons = entry.get("cartons")
    if isinstance(cartons, list) and cartons:
        return [p for p in (_carton_to_package(c) for c in cartons if isinstance(c, dict)) if p]
    cbm = float(entry.get("cbm") or 0)                    # simple mode: one cube from the product CBM
    p = _carton_to_package({"cbm": cbm}) if cbm > 0 else None
    return [p] if p else []


def _load_carton_map() -> dict:
    """SKU/product_id → {'packages': [GSS cartons for one unit], 'title': str}. Cached."""
    global _carton_map_cache
    if _carton_map_cache is None:
        m = {}
        try:
            data = _read_cartons_file()
            for e in data:
                if not isinstance(e, dict):
                    continue
                key = str(e.get("key") or "").strip()
                if not key:
                    continue
                pkgs = _entry_to_packages(e)
                if pkgs:
                    m[key] = {"packages": pkgs, "title": e.get("title", "")}
        except Exception:
            logger.exception("cartons.json unreadable — treating as empty (cube-from-CBM for all)")
            m = {}
        _carton_map_cache = m
    return _carton_map_cache


def reload_carton_map():
    """Force reload of the carton map from disk (for the admin /api/cartons writes)."""
    global _carton_map_cache
    _carton_map_cache = None


def _carton_packages_for(item: dict) -> Optional[list]:
    """Mapped cartons for a cart line (SKU first, then product_id), else None."""
    cmap = _load_carton_map()
    if not cmap:
        return None
    sku = str(item.get("sku") or "").strip()
    pid = str(item.get("product_id") or "").strip()
    entry = (cmap.get(sku) if sku else None) or (cmap.get(pid) if pid else None)
    return entry["packages"] if entry else None


def _build_packages(items: list) -> list:
    """
    Turn the cart into realistic cartons for GoSweetSpot, separating courier-
    eligible goods from oversize goods by their PER-UNIT CBM:

    • Large items (unit CBM > _COURIER_MAX_CBM) — e.g. dining chairs, big
      furniture. These can't be a courier parcel, so each unit ships as its own
      carton at its real size. GSS returns only oversize/freight for them and the
      Mainfreight/Dailyfreight formulas win — exactly how they're really shipped.

    • Small items (unit CBM ≤ _COURIER_MAX_CBM) — mugs, tea lights, plates. These
      are pooled and split into equal cartons no larger than _COURIER_MAX_CBM each
      so every parcel qualifies for Castle Parcels' cheap courier tier, mirroring
      the warehouse shipping several master cartons rather than one giant crate.

    Consolidating the small pool (rather than one parcel per unit × qty) avoids
    GSS's per-parcel minimums (~$8 each). The _PACKING_FACTOR overhead is applied
    to the small pool only, so GSS sees realistic packed volume without undercutting
    the portal price. MF/DF formulas use raw cart CBM and are unaffected.
    """
    small_kg = 0.0          # pooled CBM of courier-eligible items
    large_cartons = []      # one carton per oversize unit, at its real CBM
    mapped_cartons = []     # real cartons from the carton map, sent to GSS as-is
    for item in items:
        qty = int(item.get("quantity", 1))
        mapped = _carton_packages_for(item)
        if mapped:
            # Real cartons known for this product — emit them per unit so GSS can
            # courier each one, bypassing the cube-from-CBM estimate below.
            for _ in range(qty):
                mapped_cartons.extend(mapped)
            continue
        unit_cbm = float(item.get("grams", 0) or 0) / 1000.0
        if unit_cbm > _COURIER_MAX_CBM:
            L, W, H = _cube_dimensions_cm(unit_cbm)
            for _ in range(qty):
                large_cartons.append({"Name": "Carton", "Length": L, "Width": W,
                                      "Height": H, "Kg": round(unit_cbm, 3), "Type": "Box"})
        else:
            small_kg += unit_cbm * qty

    small_cartons = []
    if small_kg > 0:
        packed_kg = small_kg * _PACKING_FACTOR
        n_cartons = max(1, math.ceil(packed_kg / _COURIER_MAX_CBM))
        kg_per_carton = packed_kg / n_cartons
        L, W, H = _cube_dimensions_cm(kg_per_carton)
        small_cartons = [{"Name": "Carton", "Length": L, "Width": W, "Height": H,
                          "Kg": round(kg_per_carton, 3), "Type": "Box"} for _ in range(n_cartons)]

    packages = mapped_cartons + large_cartons + small_cartons
    if not packages:
        return [{"Name": "Carton", "Length": 5, "Width": 5, "Height": 5, "Kg": 0.001, "Type": "Box"}]
    return packages


def _total_cbm(items: list) -> float:
    """Sum cart CBM (weight in kg = CBM convention)."""
    total = 0.0
    for item in items:
        qty = int(item.get("quantity", 1))
        grams = float(item.get("grams", 0) or 0)
        total += (grams / 1000.0) * qty
    return total


def _order_value_incl_gst(items: list) -> float:
    """Cart value from the Shopify rate request (item `price` is cents, GST-inclusive)."""
    try:
        return sum((int(i.get("price", 0)) / 100.0) * int(i.get("quantity", 1))
                   for i in (items or []))
    except Exception:
        return 0.0


def _cart_is_courier_only(items: list) -> bool:
    """
    True iff EVERY parcel we'd hand GoSweetSpot is courier-sized — i.e. Castle Parcels
    should physically be able to quote the whole cart. If CP then returns nothing, that's
    a GSS drop-out (→ courier fallback is right). A cart with any oversize / freight-sized
    parcel (a big single item) is genuinely NOT courierable, so MF/DF is the correct rate
    and the fallback must NOT fire (else we'd overcharge a real freight load).
    Uses the same package builder as the live CP quote, so the carton map is honoured
    (a multi-carton item like the Nova counts as courier-only).
    """
    if _cart_has_oversize(items):
        return False
    pkgs = _build_packages(items)
    if not pkgs:
        return False
    for p in pkgs:
        cbm = (float(p.get("Length", 0)) * float(p.get("Width", 0)) * float(p.get("Height", 0))) / 1_000_000
        if cbm > _COURIER_MAX_CBM + 1e-9:
            return False
    return True


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


# ── Castle Parcels resilience: quote cache + courier retry ───────────────────────
# GSS is one flaky, latency-bound call (Singapore→NZ) with no retry room inside
# Shopify's ~10s window, so transient misses drop the courier quote. Two defences:
#   1. Cache each SUCCESSFUL CP rate per lane (postcode + CBM bucket); reuse on a miss
#      so a lane that quoted recently serves its real last-good rate instead of the 8%
#      fallback — and repeat quotes skip GSS entirely (faster). Disk-backed so it
#      survives restarts. Analysis showed ~26 of 29 recurring "no-option" lanes were
#      transient (serviced on retry) — exactly what the cache rescues.
#   2. For a COURIER-ONLY cart (few small parcels → fast), a tightened timeout + one
#      retry fits the window and recovers transient timeouts for the carts that matter.
CP_CACHE_ENABLED = os.environ.get("CP_CACHE_ENABLED", "1") != "0"
CP_CACHE_TTL_HOURS = float(os.environ.get("CP_CACHE_TTL_HOURS", "24"))
CP_CACHE_BUCKET = float(os.environ.get("CP_CACHE_BUCKET", "0.05"))   # CBM bucket size (m³)
CP_CACHE_FILE = _PERSIST_DIR / "cp_quote_cache.json"

GSS_TIMEOUT = float(os.environ.get("GSS_TIMEOUT", "9.0"))             # full-budget single attempt
# Layer-2 retry is OFF by default: GSS has a genuine slow tail (5-8s on provincial lanes,
# the reason the timeout is 9s), and Shopify's ~10s window leaves no room for a second
# full attempt — splitting the budget into 4s×2 would TIME OUT valid slow responses and
# make accuracy WORSE on exactly the marginal lanes. The quote cache (Layer 1) is the real
# resilience win (serves the last-good rate instantly on any miss). Only enable the retry
# (ATTEMPTS=2, a reduced COURIER_TIMEOUT) if GSS latency is ever measured low enough.
GSS_COURIER_TIMEOUT = float(os.environ.get("GSS_COURIER_TIMEOUT", "9.0"))
GSS_COURIER_ATTEMPTS = int(os.environ.get("GSS_COURIER_ATTEMPTS", "1"))

_cp_cache = None


def _cp_cache_load() -> dict:
    global _cp_cache
    if _cp_cache is None:
        try:
            _cp_cache = json.loads(CP_CACHE_FILE.read_text()) if CP_CACHE_FILE.exists() else {}
        except Exception:
            _cp_cache = {}
    return _cp_cache


def _cp_cache_key(destination: dict, cart_cbm: float) -> Optional[str]:
    pc = (destination.get("postal_code") or destination.get("zip") or "").strip()
    if not pc:
        return None  # no stable lane key → don't cache
    country = (destination.get("country") or "NZ").strip().upper()
    bucket = round(round(cart_cbm / CP_CACHE_BUCKET) * CP_CACHE_BUCKET, 3)
    return f"{country}:{pc}:{bucket}"


def _cp_cache_put(destination: dict, cart_cbm: float, cp: dict) -> None:
    """Store a successful CP quote for this lane (best-effort, never raises)."""
    if not CP_CACHE_ENABLED:
        return
    key = _cp_cache_key(destination, cart_cbm)
    if not key:
        return
    try:
        c = _cp_cache_load()
        c[key] = {"raw_cost": cp.get("raw_cost"), "carrier": cp.get("carrier"),
                  "service": cp.get("service"), "ts": datetime.now(timezone.utc).isoformat()}
        CP_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = CP_CACHE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(c))
        tmp.replace(CP_CACHE_FILE)  # atomic
    except Exception:
        logger.exception("cp cache write failed")


def _cp_cache_get(destination: dict, cart_cbm: float) -> Optional[dict]:
    """Last-good CP quote for this lane if still within TTL, else None."""
    if not CP_CACHE_ENABLED:
        return None
    key = _cp_cache_key(destination, cart_cbm)
    if not key:
        return None
    e = _cp_cache_load().get(key)
    if not e or e.get("raw_cost") is None:
        return None
    try:
        age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(e["ts"])).total_seconds() / 3600
    except Exception:
        return None
    if age_h > CP_CACHE_TTL_HOURS:
        return None
    return {
        "carrier": e.get("carrier") or "Post Haste - Freight Forwards",
        "service": e.get("service") or "Courier",
        "raw_cost": e["raw_cost"],
        "_source": f"Castle Parcels cached last-good rate ({age_h:.0f}h old) — GSS unavailable this call",
        "_cached": True,
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

    # A COURIER-ONLY cart (few, small parcels) returns from GSS fast, so we take a
    # tightened timeout + one retry — two shots fit Shopify's ~10s window and recover
    # transient timeouts for exactly the carts that matter. A freight-sized cart is slow
    # and legitimately ships MF/DF, so it keeps a single full-budget attempt (a retry
    # there could stack to ~18s and blow the window → no rate at checkout). Every drop
    # is logged so the failure stays visible.
    _dest_label = f"{dest_payload['Address']['Suburb']} {dest_payload['Address']['PostCode']}".strip()
    courier_only = _cart_is_courier_only(items)
    timeout = GSS_COURIER_TIMEOUT if courier_only else GSS_TIMEOUT
    attempts = GSS_COURIER_ATTEMPTS if courier_only else 1
    data = None
    for attempt in range(1, attempts + 1):
        try:
            r = await _client.post(GSS_URL, json=payload, headers=headers, timeout=timeout)
            if r.status_code != 200:
                logger.warning("GSS dropped CP for %s: HTTP %s — falling back", _dest_label, r.status_code)
                return None
            data = r.json()
            break
        except Exception as e:
            if attempt < attempts:
                logger.info("GSS slow for %s (%s) — retry %d/%d", _dest_label, type(e).__name__, attempt, attempts - 1)
                continue
            logger.warning("GSS dropped CP for %s: %s: %s — falling back", _dest_label, type(e).__name__, e)
            return None
    if data is None:
        return None

    options = data.get("Available", [])
    if not options:
        # GSS geocodes off the City/Suburb text, so a bad label — a region name ("Bay of
        # Plenty") or a city mismatched to its postcode ("Christchurch" + rural 7616) — can
        # return zero options even on a serviceable lane. The POSTCODE is authoritative, so
        # retry once with the postcode-derived city before giving up. This "no options"
        # response came back fast (GSS answered), so the extra call stays inside the window.
        fb = _fallback_keys(destination)
        pc_city = fb[0] if fb else None
        if pc_city and _strip_diacritics(pc_city.lower()) != _strip_diacritics(gss_city.lower()):
            # Overwrite BOTH City and Suburb — a region name left in Suburb ("Bay of
            # Plenty") still breaks GSS geocoding even once the City is corrected.
            payload["Destination"]["Address"]["City"] = pc_city
            payload["Destination"]["Address"]["Suburb"] = pc_city
            try:
                r = await _client.post(GSS_URL, json=payload, headers=headers, timeout=timeout)
                if r.status_code == 200:
                    data = r.json()
                    options = data.get("Available", [])
                    if options:
                        logger.info("GSS rescued %s via postcode-city '%s'", _dest_label, pc_city)
            except Exception as e:
                logger.info("GSS postcode-city rescue failed for %s: %s", _dest_label, type(e).__name__)
        if not options:
            logger.warning("GSS dropped CP for %s: no Available options (rejected=%s) — falling back to MF/DF formula",
                           _dest_label, len(data.get("Rejected", [])))
            return None

    # SAFETY FILTER: drop suspiciously low quotes for large items.
    # GSS sometimes returns Kiwi Express Car-Economy at $13-86 for cube >1m³,
    # which is clearly wrong for sofa-sized parcels (real KX Oversize minimum
    # for ChCh local is ~$46, Auckland ~$87).
    # If filter removes everything → return None so MF/DF formulas take over.
    # The per-m³ term does the real work: a genuine underquote on a big item has a
    # low $/m³ (a $13 sofa quote ≈ $9/m³) and is dropped, while legitimate small
    # parcels have very high $/m³ (a real $7.85 mug ≈ $1700/m³) and pass. The small
    # absolute floor is only a backstop against a near-zero glitch quote — it sits
    # below Post Haste's genuine ~$6-8 courier minimum so it no longer clips real
    # tiny-parcel quotes (a single mug used to be rejected by 15c at the old $8 floor).
    cart_cbm = sum(float(p.get("Length", 0)) * float(p.get("Width", 0)) * float(p.get("Height", 0))
                   for p in payload["Packages"]) / 1_000_000  # cm³ → m³
    min_sensible_per_m3 = 40  # $/m³ raw cost — the primary anomaly test
    threshold = max(5, cart_cbm * min_sensible_per_m3)
    filtered = [
        o for o in options
        if (o.get("Cost", 0) / GSS_BUILTIN_MARKUP) >= threshold
    ]
    if not filtered:
        # All GSS quotes are unrealistic for this cart — skip CP entirely
        # so the cheapest of MF/DF (formula-based) gets picked. This is expected
        # for genuinely large carts; logged at INFO (not a fault).
        logger.info("GSS quotes below safety threshold ($%.2f/m³ floor) for %s — using MF/DF",
                    threshold, _dest_label)
        return None
    options = filtered

    # Choose cheapest available option (typically Post Haste 2-Day)
    cheapest = min(options, key=lambda o: o.get("Cost", float("inf")))
    gss_cost = float(cheapest.get("Cost", 0))
    true_cost = gss_cost / GSS_BUILTIN_MARKUP  # strip GSS's 8%

    # Rural buffer: GSS's quote omits the rural surcharge on lanes it mis-flags as
    # urban (e.g. Mangawhai) — add it to the cost basis so we don't quote below the
    # invoice. Per carton (Castle bills each parcel). No-op when GSS already flagged rural.
    is_rural = cheapest.get("IsRuralDelivery", False)
    carrier = cheapest.get("CarrierName", "Castle Parcels")
    service = cheapest.get("DeliveryType", "Standard")
    n_cartons = len(payload["Packages"])
    surcharge = _rural_surcharge(destination, is_rural, f"{carrier} {service}", n_cartons)
    source = "GoSweetSpot live API"
    if surcharge:
        true_cost += surcharge
        source += f" + rural ${surcharge:.2f} ({n_cartons} carton{'s' if n_cartons != 1 else ''})"
        logger.info("Rural buffer +$%.2f applied to %s (GSS rural=False, known-rural postcode)",
                    surcharge, _dest_label)

    return {
        "carrier":   carrier,
        "service":   service,
        "raw_cost":  round(true_cost, 2),  # incl FAF+GST already
        "is_rural":  is_rural or bool(surcharge),
        "_source":   source,
    }


def quote_mainfreight(cart_cbm: float, destination: dict, override_key: Optional[str] = None) -> Optional[dict]:
    """
    Compute Mainfreight cost from cached rate card.
    Formula: MAX(min_charge, base + per_m3 × cart_cbm). Result is excl GST & excl FAF.
    override_key forces a specific rate key (used by the postcode/province fallback).
    """
    rates = _load_carrier_rates()
    city = override_key or _normalise_city(destination.get("city", ""))
    mf_rates = rates["mainfreight"]["rates"]
    rate = mf_rates.get(city)
    # City aliases resolve to Dailyfreight *zone* keys (e.g. 'cromwell_z5',
    # 'auckland_z1'), but the Mainfreight rate card is keyed by bare hub names
    # ('cromwell', 'auckland'). Strip the _zN suffix so the MF formula fallback
    # still fires for aliased cities (Wanaka, Queenstown, Auckland, Hamilton…).
    # Without this, quote_mainfreight silently returned None for every aliased
    # destination, leaving only the pricier Dailyfreight formula as fallback.
    if not rate:
        hub = re.sub(r"_z\d+$", "", city)
        rate = mf_rates.get(hub)
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


import datetime as _datetime


def _next_business_day_iso() -> str:
    d = _datetime.date.today() + _datetime.timedelta(days=1)
    while d.weekday() >= 5:
        d += _datetime.timedelta(days=1)
    return f"{d.isoformat()}T09:00:00"


async def _mainfreight_rate(account: str, service: str, destination: dict,
                            packed_cbm: float, origin_address: Optional[dict] = None) -> Optional[float]:
    """
    Call Mainfreight Rating API for one account/service combo.
    origin_address: MF-shaped {suburb,city,postCode,countryCode}. Defaults to the
    Christchurch (Wigram) warehouse; pass ORIGIN_AKL_ADDRESS for ex-Auckland quotes.
    Returns TotalIncludingGSTAmount (carrier-billed price incl FAF + GST), or None.
    """
    if not MAINFREIGHT_API_KEY:
        return None
    pc = (destination.get("postal_code") or destination.get("zip") or "").strip()
    city = (destination.get("city") or "").strip()
    if not (city and pc):
        return None  # Mainfreight requires both city and postcode for NZ

    if origin_address is None:
        origin_address = {
            "suburb":      ORIGIN["Address"]["Suburb"],
            "city":        ORIGIN["Address"]["City"],
            "postCode":    ORIGIN["Address"]["PostCode"],
            "countryCode": ORIGIN["Address"]["CountryCode"],
        }

    body = {
        "account":      {"code": account},
        "serviceLevel": {"code": service},
        "origin": {
            "freightRequiredDateTime":     _next_business_day_iso(),
            "freightRequiredDateTimeZone": "New Zealand Standard Time",
            "address": origin_address,
        },
        "destination": {
            "address": {
                "suburb":      city,
                "city":        city,
                "postCode":    pc,
                "countryCode": destination.get("country", "NZ") or "NZ",
            }
        },
        "freightDetails": [{
            "units":        1,
            "packTypeCode": "CTN",
            # Real weight is unknown (Shopify weight = CBM in our convention); send a
            # placeholder of 1 kg and let MF compute on dimensional weight from volume.
            "weight":       1,
            "volume":       round(max(packed_cbm, 0.001), 4),
        }],
    }
    H = {
        "Authorization": f"Secret {MAINFREIGHT_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    # One fast retry on a transient miss (timeout / 5xx / connection reset). A 4xx or
    # a 200 with no charge line is permanent (bad address, unratable lane) — return
    # immediately, never loop. Keeps a single API blip from dropping to "Contact Us".
    for attempt in range(MF_RETRIES + 1):
        try:
            r = await _client.post(MAINFREIGHT_URL, headers=H, json=body, timeout=MF_TIMEOUT)
            if r.status_code == 200:
                for c in r.json().get("charges", []):
                    if c.get("name") == "TotalIncludingGSTAmount":
                        return float(c.get("value", 0))
                return None  # rated OK but no total charge -> not transient
            if r.status_code < 500:
                return None  # 4xx -> permanent, don't retry
            # 5xx -> transient, fall through to retry
        except (httpx.TimeoutException, httpx.TransportError):
            pass  # transient network error -> retry
        except Exception:
            logger.exception("Mainfreight rate call failed unexpectedly (%s/%s)", account, service)
            return None
    return None


# NOTE: live MF/DF quotes use RAW cart CBM, NOT packed. The _PACKING_FACTOR is a
# GoSweetSpot/courier concept (GSS prices a parcel cube, so we inflate raw → packed
# to match real cartons). Mainfreight/Dailyfreight price on declared consignment
# volume, and their rate cards (the formula fallbacks) are calibrated on raw cart
# CBM — so live(raw) tracks the formula within ~2.4%, whereas live(packed) overshot
# by up to ~64% on large carts. Verified 2026-06-10 across ChCh/Auckland/Wānaka/
# Whanganui at 0.03–1.89 m³.

async def quote_dailyfreight_live(cart_cbm: float, destination: dict) -> Optional[dict]:
    """Live Dailyfreight LCL quote via Mainfreight Rating API (NEDCOLDF account)."""
    cost = await _mainfreight_rate(MF_ACCOUNT_DF, "LCL", destination, cart_cbm)
    if cost is None:
        return None
    return {
        "carrier":  "Dailyfreight",
        "service":  "LCL Palletised",
        "raw_cost": round(cost, 2),  # already incl FAF + GST per API response
        "_source":  f"Mainfreight Rating API (live) — NEDCOLDF/LCL @ {cart_cbm:.3f}m³",
    }


async def quote_mainfreight_live(cart_cbm: float, destination: dict) -> Optional[dict]:
    """Live Mainfreight M2H quote via Mainfreight Rating API (NEDCOLCHC account)."""
    cost = await _mainfreight_rate(MF_ACCOUNT_MF, "M2H", destination, cart_cbm)
    if cost is None:
        return None
    return {
        "carrier":  "Mainfreight",
        "service":  "M2H Two-Man",
        "raw_cost": round(cost, 2),  # already incl FAF + GST per API response
        "_source":  f"Mainfreight Rating API (live) — NEDCOLCHC/M2H @ {cart_cbm:.3f}m³",
    }


async def quote_mainfreight_akl_live(cart_cbm: float, destination: dict) -> Optional[dict]:
    """Live Mainfreight M2H quote shipped FROM the Auckland 3PL (NEDCOLAKL account)."""
    cost = await _mainfreight_rate(MF_ACCOUNT_AKL, "M2H", destination, cart_cbm,
                                   origin_address=ORIGIN_AKL_ADDRESS)
    if cost is None:
        return None
    return {
        "carrier":  "Mainfreight",
        "service":  "M2H Two-Man (ex-Auckland)",
        "raw_cost": round(cost, 2),  # already incl FAF + GST per API response
        "_source":  f"Mainfreight Rating API (live) — NEDCOLAKL/M2H @ {cart_cbm:.3f}m³",
    }


async def quote_dailyfreight_akl_live(cart_cbm: float, destination: dict) -> Optional[dict]:
    """Live Dailyfreight LCL quote shipped FROM the Auckland 3PL (NEDCOLDF account, Māngere origin).

    NOTE: reuses the ex-CHCH NEDCOLDF account with an Auckland origin — the Rating API
    prices it correctly, but confirm with Mainfreight that NEDCOLDF can *collect & bill*
    ex-Māngere (M2H got its own NEDCOLAKL account rather than reusing NEDCOLCHC).
    """
    cost = await _mainfreight_rate(MF_ACCOUNT_DF, "LCL", destination, cart_cbm,
                                   origin_address=ORIGIN_AKL_ADDRESS)
    if cost is None:
        return None
    return {
        "carrier":  "Dailyfreight",
        "service":  "LCL Palletised (ex-Auckland)",
        "raw_cost": round(cost, 2),  # already incl FAF + GST per API response
        "_source":  f"Mainfreight Rating API (live) — NEDCOLDF/LCL ex-AKL @ {cart_cbm:.3f}m³",
    }


def quote_dailyfreight_akl(cart_cbm: float, destination: dict, override_key: Optional[str] = None) -> Optional[dict]:
    """
    Dailyfreight LCL quote shipped FROM the Auckland 3PL, using NED's NEGOTIATED ex-AKL
    rate card (data/carrier_rates.json -> 'dailyfreight_akl'). Reuses the same city-alias
    zone keys as the ex-CHCH table. Formula: MAX(base, per_m3_tier × cbm) × FAF × GST.

    This replaces the live-API DF ex-AKL quote, which returned generic (non-negotiated)
    rates because the Rating API doesn't hold NED's ex-Auckland Dailyfreight card.
    """
    rates = _load_carrier_rates()
    rate_key = override_key or _normalise_city(destination.get("city", ""))
    rate = rates.get("dailyfreight_akl", {}).get("rates", {}).get(rate_key)
    if not rate:
        fb = _fallback_keys(destination)  # resolve by postcode/region if city didn't map
        if fb:
            rate_key = fb[1]
            rate = rates.get("dailyfreight_akl", {}).get("rates", {}).get(rate_key)
    if not rate:
        return None
    tier_idx = _df_tier_index(cart_cbm)
    per_m3 = rate["tiers"][tier_idx]
    base = rate["base"]
    raw_cost = max(base, per_m3 * cart_cbm) * FAF_MULTIPLIER * GST_MULTIPLIER
    return {
        "carrier":  "Dailyfreight",
        "service":  "LCL Palletised (ex-Auckland)",
        "raw_cost": round(raw_cost, 2),
        "_source":  f"ex-AKL card {rate_key} tier {['<5m³','5-10m³','≥10m³'][tier_idx]}: "
                    f"MAX({base}, {per_m3} × {cart_cbm:.3f}) × FAF × GST",
    }


async def calculate_auckland_freight(items: list, destination: dict) -> dict:
    """
    Freight for a cart fulfilled from the Auckland 3PL warehouse — cheapest of the two
    ex-AKL carriers: Mainfreight M2H (NEDCOLAKL) and Dailyfreight LCL (NEDCOLDF, Māngere
    origin). Mirrors calculate_freight's return shape so the callback treats it identically.
    Both are live via the Mainfreight Rating API. Under LIVE_ONLY the live DF quote is
    authoritative; otherwise it falls back to NED's negotiated ex-AKL rate card (formula).
    NED's service rules apply here too (Auckland holds the large/oversize items): an
    oversize item forces Mainfreight M2H two-man. customer_price is GST-inclusive.
    """
    import asyncio as _asyncio
    cart_cbm = _total_cbm(items)
    m2h, df_live = await _asyncio.gather(
        quote_mainfreight_akl_live(cart_cbm, destination),   # live Mainfreight M2H ex-AKL
        quote_dailyfreight_akl_live(cart_cbm, destination),  # live Dailyfreight LCL ex-AKL
    )
    # DF: live authoritative under LIVE_ONLY, else fall back to the negotiated ex-AKL card
    df = df_live if LIVE_ONLY else (df_live or quote_dailyfreight_akl(cart_cbm, destination))
    quotes = [q for q in (m2h, df) if q]
    if not quotes:
        return {"success": False, "error": "no_akl_rate", "cart_cbm": round(cart_cbm, 4)}
    # Same service rules as ex-CHCH (oversize -> two-man; small cart -> avoid pallet).
    # Fail-safe: never empties a non-empty set.
    eligible = _apply_service_rules(quotes, items, cart_cbm) if PALLET_RULE_ENABLED else quotes
    q = min(eligible, key=lambda x: x["raw_cost"])
    return {
        "success":        True,
        "cart_cbm":       round(cart_cbm, 4),
        "chosen_carrier": q["carrier"],
        "chosen_service": q["service"],
        "raw_cost":       q["raw_cost"],
        "ned_markup":     NED_MARKUP,
        "customer_price": round(q["raw_cost"] * NED_MARKUP, 2),
    }


async def calculate_freight(items: list, destination: dict, debug: bool = False, count_alerts: bool = False) -> dict:
    """
    Main entry: get quotes from all three carriers, pick cheapest, apply markup.
    Returns a dict suitable for Shopify carrier service response.
    """
    cart_cbm = _total_cbm(items)
    quotes = []

    # Correct a destination whose typed city contradicts its postcode BEFORE quoting,
    # so all three carriers rate the real location (they geocode off city text). e.g.
    # "Auckland 0505" is actually Mangawhai — rated metro it came back ~half the true
    # two-man rate. Conservative: only fires on a recognised city/postcode conflict.
    destination, _city_fix = _resolve_destination_city(destination)
    if _city_fix:
        logger.warning("Destination city '%s' contradicts postcode %s — rating as '%s'",
                       _city_fix[0], destination.get("postal_code") or destination.get("zip"),
                       _city_fix[1])

    # Fire CP (GSS) + live MF + live DF in parallel — they're independent network
    # calls and serial awaits would triple the checkout latency.
    import asyncio as _asyncio
    cp, mf_live, df_live = await _asyncio.gather(
        quote_castle_parcels(items, destination),
        quote_mainfreight_live(cart_cbm, destination),
        quote_dailyfreight_live(cart_cbm, destination),
    )
    if cp:
        quotes.append(cp)
        _cp_cache_put(destination, cart_cbm, cp)  # remember this lane's good rate
    else:
        # GSS missed — serve this lane's last-good CP rate if we have a fresh one, so a
        # transient drop-out doesn't bounce a courier cart onto freight / the 8% fallback.
        cached = _cp_cache_get(destination, cart_cbm)
        if cached:
            quotes.append(cached)

    # Mainfreight — live API is authoritative. Under LIVE_ONLY the static rate-card
    # formula is retired (a live miss + one retry falls through to "Contact Us");
    # otherwise the formula stays as a resilient fallback for a full API outage.
    mf = mf_live if LIVE_ONLY else (mf_live or quote_mainfreight(cart_cbm, destination))
    if mf:
        quotes.append(mf)

    # Dailyfreight — same pattern.
    df = df_live if LIVE_ONLY else (df_live or quote_dailyfreight(cart_cbm, destination))
    if df:
        quotes.append(df)

    # Postcode/province fallback: if the carriers couldn't price the city (a typo like
    # "Roturua", a suburb like "Karori", or an amalgamated council name), resolve the
    # destination by postcode then region and quote off the rate card, so we never drop a
    # ratable NZ address to $0 "Contact Us" (NED3823: "Roturua" 3010 quoted nothing and
    # shipped free). The postcode is unchanged so the rate stays accurate, and it's
    # formula-based (instant — no extra network round, no Shopify ~10s-timeout risk).
    #
    #   * Not LIVE_ONLY: fires whenever both MF and DF missed (original behaviour).
    #   * LIVE_ONLY: live carriers stay authoritative — this fires ONLY when nothing
    #     quoted at all, as a last-resort rescue in place of no_carrier_match.
    need_fallback = (not quotes) if LIVE_ONLY else (not mf and not df)
    if need_fallback:
        fb = _fallback_keys(destination)
        if fb:
            mf_key, df_key = fb
            mf_fb = quote_mainfreight(cart_cbm, destination, override_key=mf_key)
            if mf_fb:
                mf_fb["_source"] += "  [postcode/province fallback]"
                quotes.append(mf_fb)
            df_fb = quote_dailyfreight(cart_cbm, destination, override_key=df_key)
            if df_fb:
                df_fb["_source"] += "  [postcode/province fallback]"
                quotes.append(df_fb)

    # COURIER FALLBACK: a small, non-oversize cart with NO courier quote means Castle
    # Parcels (GSS) dropped out — the MF/DF freight minimums (~$90-190) are nonsensical
    # for courier goods and lose the sale. Replace them with a % of the ex-GST order
    # value (close to NED's average courier cost) so the customer sees a sane rate.
    # Also rescues the empty-quotes case for a small courier cart.
    _dlabel = f"{destination.get('city', '?')} {destination.get('postal_code') or destination.get('zip') or ''}".strip()

    # Rural per-carton fee for the % fallbacks: the 8% models an urban courier cost, so a
    # rural address adds Castle's per-carton rural fee (marked up like any carrier cost).
    _rural_fb = _rural_surcharge(destination, False, "courier", len(_build_packages(items)))

    def _cbm_quote(note: str) -> dict:
        cost, lane = _cbm_courier_cost(cart_cbm, destination)
        price = round(max((cost + _rural_fb) * NED_MARKUP, COURIER_FALLBACK_MIN), 2)
        note += f" — est. Castle ${cost:.2f} ({lane}, {cart_cbm:.3f}m³)"
        if _rural_fb:
            note += f" + rural ${_rural_fb:.2f}"
        return {
            "carrier": "Standard Delivery", "service": "Courier",
            "raw_cost": round(price / NED_MARKUP, 2),
            "_source": note, "_fallback": True, "_price": price,
        }

    def _pct_quote(ex_gst: float, note: str) -> dict:
        price = round(max(ex_gst * COURIER_FALLBACK_PCT, COURIER_FALLBACK_MIN), 2)
        if _rural_fb and price > 0:
            price = round(price + _rural_fb * NED_MARKUP, 2)
            note += f" + rural ${_rural_fb:.2f}"
        return {
            "carrier": "Standard Delivery", "service": "Courier",
            # raw_cost back-calculated so the ×NED_MARKUP below lands exactly on `price`
            # — the % is already the final charge, not a cost to mark up again.
            "raw_cost": round(price / NED_MARKUP, 2),
            "_source": note, "_fallback": True, "_price": price,
        }

    # Courier fallback: a courier-only cart with NO courier quote means Castle Parcels
    # dropped out — the MF/DF freight minimums (~$90+) are nonsensical for courier goods.
    # Replace them with % of the ex-GST order (or the floor when the cart value is missing).
    #
    # EXCEPT on a rural-delivery address that still has a live MF/DF quote: rural courier
    # costs far more than the urban average the 8% models (NED4200, Morrinsville RD1:
    # CP $119 / MF $113 real vs $46 fallback), so keep the real freight quotes there.
    # Rural with nothing quoted still gets the % (below) rather than no rate.
    discarded = []
    rural_keep = _is_rural_address(destination) and bool(quotes)
    if (COURIER_FALLBACK_ENABLED
            and not rural_keep
            and not any(_is_courier_quote(q) for q in quotes)
            and _cart_is_courier_only(items)):
        if COURIER_FALLBACK_MODE == "pct":
            ex_gst = _order_value_incl_gst(items) / GST_MULTIPLIER
            fb = _pct_quote(ex_gst, f"Courier fallback {COURIER_FALLBACK_PCT*100:.0f}% of ex-GST order "
                                    f"(${ex_gst:.2f}) — Castle Parcels unavailable")
        else:
            fb = _cbm_quote("Courier fallback by volume — Castle Parcels unavailable")
        if fb["_price"] > 0:   # guard: no value AND no floor → don't zero-out shipping
            discarded = [{**q, "_discarded": "courier_fallback"} for q in quotes]
            quotes = [fb]
            logger.warning("Courier fallback for %s: CP unavailable on %.3fm³ → $%.2f", _dlabel, cart_cbm, fb["_price"])
            if count_alerts:
                alerts.record_fallback(f"{_dlabel}: courier {cart_cbm:.3f}m³ → ${fb['_price']}")

    # Last resort: NOTHING quoted at all (all carriers down), ANY cart type. Serve % of the
    # order so the customer still gets a rate instead of no_carrier_match (lost sale / free
    # ship). This is the "if all carriers are down, give the 8%" case — now covering freight
    # carts too, not just courier ones.
    if not quotes and COURIER_FALLBACK_ENABLED and COURIER_FALLBACK_MODE != "pct" and _cart_is_courier_only(items):
        fb = _cbm_quote("Last-resort by volume — ALL carriers unavailable")
        quotes = [fb]
        logger.error("ALL carriers down for %s — last-resort $%.2f on %.3fm³", _dlabel, fb["_price"], cart_cbm)
        if count_alerts:
            alerts.record_all_carriers_down(f"{_dlabel}: ${fb['_price']} by volume, {cart_cbm:.3f}m³")
    if not quotes and COURIER_FALLBACK_ENABLED:
        ov = _order_value_incl_gst(items)
        if ov > 0:
            fb = _pct_quote(ov / GST_MULTIPLIER, f"Last-resort {COURIER_FALLBACK_PCT*100:.0f}% of ex-GST "
                                                 f"order — ALL carriers unavailable")
            quotes = [fb]
            logger.error("ALL carriers down for %s — last-resort $%.2f on $%.2f order", _dlabel, fb["_price"], ov)
            if count_alerts:
                alerts.record_all_carriers_down(f"{_dlabel}: ${fb['_price']} on ${ov:.0f} order, {cart_cbm:.3f}m³")

    if not quotes:
        if count_alerts:
            alerts.record_no_rate(f"{_dlabel}: cart ${_order_value_incl_gst(items):.0f}, {cart_cbm:.3f}m³ — no carrier, no fallback")
        return {
            "success": False,
            "error":   "no_carrier_match",
            "cart_cbm": cart_cbm,
            "destination_city": destination.get("city", ""),
            "quotes": [],
        }

    # Apply NED's service rules (oversize -> Two-Man; small cart -> no LCL pallet)
    # before selection. Flag-gated and fail-safe: never empties a non-empty set.
    # Keep the FULL quote set for the debug/rate-log view so we can see what was
    # dropped and why; select only from the eligible set.
    eligible = _apply_service_rules(quotes, items, cart_cbm) if PALLET_RULE_ENABLED else quotes

    # Pick cheapest of eligible
    cheapest = min(eligible, key=lambda q: q["raw_cost"])
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
        # Full set considered, plus any MF/DF quotes the courier fallback replaced
        # (flagged _discarded) so the rate log shows what the carriers really returned.
        result["all_quotes"] = quotes + discarded
        result["eligible_quotes"] = eligible  # after service-rule filtering
        # Report the rule that actually changed the eligible set (not just what was tested)
        if cheapest.get("_fallback"):
            rule = "courier_fallback"
        elif not PALLET_RULE_ENABLED or eligible is quotes:
            rule = "none"
        elif _cart_has_oversize(items):
            rule = "oversize->two_man"
        else:
            rule = "small_cart->dropped_pallet"
        result["rule_applied"] = rule
        result["destination"] = destination
    return result
