# Tests for geo-gated warehouse pickup (2026-10-06).
# Pickup is offered only when the delivery address is in the branch's region AND that
# branch physically holds the whole cart:
#   Wigram (CHCH) -> Canterbury customers;  Māngere (AKL) -> Auckland customers.
# Run: .venv/bin/python scripts/test_pickup_geo.py   (no network — stock read is stubbed)
import sys, asyncio, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import server

passed = failed = 0
def check(name, got, want):
    global passed, failed
    ok = got == want; passed += ok; failed += (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ('' if ok else f"\n        got={got} want={want}"))

CAN = {"province": "CAN", "city": "Christchurch", "postal_code": "8011"}
AKL = {"province": "AUK", "city": "Auckland", "postal_code": "1010"}
WGN = {"province": "WGN", "city": "Wellington", "postal_code": "6011"}

ITEMS = [{"variant_id": 1, "quantity": 1}]

def stub_stock(rec):
    """Make server.get_location_stock return a canned record for variant '1'."""
    async def fake(variant_ids):
        if rec is None:
            return {}  # simulate a read error / empty
        return {"1": rec}
    server.get_location_stock = fake

def codes(rates):
    return sorted(r["service_code"] for r in rates)

def pickup(dest):
    return codes(asyncio.run(server._branch_pickup_rates(ITEMS, "NZD", dest)))

# --- region detector sanity ---
check("_is_auckland: province AUK", server._is_auckland({"province": "AUK"}), True)
check("_is_auckland: Auckland city", server._is_auckland({"city": "Manukau"}), True)
check("_is_auckland: AKL postcode 2016", server._is_auckland({"postal_code": "2016"}), True)
check("_is_auckland: Wellington -> False", server._is_auckland(WGN), False)
check("_is_auckland: Christchurch -> False", server._is_auckland(CAN), False)

# --- whole-cart pickup, geo-gated ---
CHCH_ONLY = {"akl": 0, "akl_oh": 0, "chch": 5, "chch_oh": 5}
AKL_ONLY  = {"akl": 5, "akl_oh": 5, "chch": 0, "chch_oh": 0}
DUAL      = {"akl": 5, "akl_oh": 5, "chch": 5, "chch_oh": 5}

stub_stock(CHCH_ONLY)
check("CHCH stock + Canterbury -> Wigram pickup", pickup(CAN), ["PICKUP"])
check("CHCH stock + Auckland -> no pickup (AKL doesn't hold it)", pickup(AKL), [])
check("CHCH stock + Wellington -> no pickup", pickup(WGN), [])

stub_stock(AKL_ONLY)
check("AKL stock + Auckland -> Māngere pickup", pickup(AKL), ["PICKUP_AKL"])
check("AKL stock + Canterbury -> no pickup (CHCH doesn't hold it)", pickup(CAN), [])

stub_stock(DUAL)
check("dual stock + Canterbury -> Wigram only", pickup(CAN), ["PICKUP"])
check("dual stock + Auckland -> Māngere only", pickup(AKL), ["PICKUP_AKL"])
check("dual stock + Wellington -> no pickup", pickup(WGN), [])

# --- fail-safe on stock read error: offer the customer's LOCAL branch only ---
stub_stock(None)
check("read error + Canterbury -> Wigram (fail-open local)", pickup(CAN), ["PICKUP"])
check("read error + Auckland -> Māngere (fail-open local)", pickup(AKL), ["PICKUP_AKL"])
check("read error + Wellington -> no pickup (never out-of-region)", pickup(WGN), [])

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
