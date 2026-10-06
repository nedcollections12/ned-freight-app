# Tests for the ex-GST trade twins on the Auckland-routing path (2026-10-06).
# On the shared retail+B2B carrier service (gst_divisor == 1.0) with DUAL_RATES on, every
# non-$0 rate must get a *_B2B twin priced ÷ GST, so the delivery function can show trade
# an ex-GST option. Pickups ($0) get no twin. The legacy b2b endpoint (gst_divisor == 1.15)
# and DUAL_RATES-off must NOT add twins.
# Run: .venv/bin/python scripts/test_gst_twins.py   (no network — routing + pickup stubbed)
import sys, asyncio, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import server

passed = failed = 0
def check(name, got, want):
    global passed, failed
    ok = got == want; passed += ok; failed += (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ('' if ok else f"\n        got={got} want={want}"))

AKL = {"province": "AUK", "city": "Auckland", "postal_code": "1010"}
A = {"variant_id": 1, "quantity": 1}
B = {"variant_id": 2, "quantity": 1}

def stub_route(decision):
    async def fake(dest, items): return decision
    server._route_decision = fake

def stub_pickup(rates):
    async def fake(items, currency, destination): return rates
    server._branch_pickup_rates = fake

def run(items, dest, gst_divisor, dual, akl_on=True):
    server.AKL_ROUTING = akl_on
    server.DUAL_RATES = dual
    res = asyncio.run(server._auckland_routing(dest, items, "NZD", gst_divisor=gst_divisor))
    return res["rates"] if res else None

def by_code(rates):
    return {r["service_code"]: r["total_price"] for r in rates}

# --- whole cart ex-AKL: NED_LIVE gets an ex-GST twin; $0 pickup does not ---
stub_route({"must_akl": [A], "dual": [], "chch_only": [],
            "akl_items": [A], "chch_items": [], "akl_price": 100.0, "chch_price": 0.0,
            "collectable": [A], "chch_only_price": None, "must_akl_price": 100.0, "scenario": "B"})
stub_pickup([{"service_name": "PICK UP - Māngere (Auckland)", "service_code": "PICKUP_AKL",
             "total_price": "0", "currency": "NZD", "description": ""}])

r = by_code(run([A], AKL, 1.0, dual=True))
check("NED_LIVE present (incl)", r.get("NED_LIVE"), "10000")           # ceil(100)*100
check("NED_LIVE_B2B twin present (ex-GST)", r.get("NED_LIVE_B2B"), "8696")  # round(100/1.15,2)=86.96
check("pickup kept as-is", r.get("PICKUP_AKL"), "0")
check("pickup NOT twinned ($0)", "PICKUP_AKL_B2B" in r, False)

# --- split cart: the partial-collect rate also gets an ex-GST twin ---
stub_route({"must_akl": [A], "dual": [], "chch_only": [B],
            "akl_items": [A], "chch_items": [B], "akl_price": 120.0, "chch_price": 60.0,
            "collectable": [A], "chch_only_price": 60.0, "must_akl_price": 120.0, "scenario": "B"})
stub_pickup([])
r = by_code(run([A, B], AKL, 1.0, dual=True))
check("split: NED_LIVE_B2B twin present", "NED_LIVE_B2B" in r, True)
check("split: partial NED_MIXED_COLLECT present", "NED_MIXED_COLLECT" in r, True)
check("split: partial twin NED_MIXED_COLLECT_B2B present", "NED_MIXED_COLLECT_B2B" in r, True)
check("split: partial twin is ex-GST", r.get("NED_MIXED_COLLECT_B2B"),
      str(int(round((int(r["NED_MIXED_COLLECT"]) / 100.0) / server.GST, 2) * 100)))

# --- guards: no twins on the legacy b2b endpoint (gst_divisor 1.15) or when DUAL_RATES off ---
stub_route({"must_akl": [A], "dual": [], "chch_only": [],
            "akl_items": [A], "chch_items": [], "akl_price": 100.0, "chch_price": 0.0,
            "collectable": [A], "chch_only_price": None, "must_akl_price": 100.0, "scenario": "B"})
stub_pickup([])
r = by_code(run([A], AKL, 1.15, dual=True))
check("gst_divisor=1.15 (legacy b2b) -> no twin added", any(c.endswith("_B2B") for c in r), False)
r = by_code(run([A], AKL, 1.0, dual=False))
check("DUAL_RATES off -> no twin added", any(c.endswith("_B2B") for c in r), False)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
