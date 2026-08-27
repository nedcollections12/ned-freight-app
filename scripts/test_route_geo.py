# Tests for the dual-stock geographic gate: dual items only ship ex-Auckland for a
# North-Island-north-of-Wellington customer. Run: .venv/bin/python scripts/test_route_geo.py
# No network — stock read and carrier quotes are mocked.
import sys, asyncio, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import server

passed = failed = 0
def check(name, got, want):
    global passed, failed
    ok = got == want; passed += ok; failed += (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ('' if ok else f"\n        got={got} want={want}"))

# --- _is_upper_north_island (pure) ---
def uni(**d): return server._is_upper_north_island(d)
print("Geo helper — upper NI (north of Wellington)")
check("Auckland province", uni(province="AUK"), True)
check("Waikato province", uni(province="WKO"), True)
check("Manawatu-Whanganui", uni(province="MWT"), True)
check("Hawke's Bay", uni(province="HKB"), True)
check("Wellington province -> False", uni(province="WGN"), False)
check("Canterbury -> False", uni(province="CAN"), False)
check("Otago -> False", uni(province="OTA"), False)
print("Geo helper — postcode fallback (no province)")
check("Auckland 0600", uni(postal_code="0600"), True)
check("Palmerston Nth 4410", uni(postal_code="4410"), True)
check("Wellington 6011 -> False", uni(postal_code="6011"), False)
check("Porirua 5024 -> False", uni(postal_code="5024"), False)
check("Christchurch 8011 -> False", uni(postal_code="8011"), False)

# --- routing gate ---
STOCK = {
    "A": {"akl": 5, "akl_oh": 5, "chch": 0, "chch_oh": 0},  # must_akl (only in AKL)
    "D": {"akl": 5, "akl_oh": 5, "chch": 5, "chch_oh": 5},  # dual (both)
    "C": {"akl": 0, "akl_oh": 0, "chch": 5, "chch_oh": 5},  # chch_only
}
ITEMS = [{"variant_id": "A", "quantity": 1},
         {"variant_id": "D", "quantity": 1},
         {"variant_id": "C", "quantity": 1}]

async def fake_stock(vids): return {k: STOCK[k] for k in vids if k in STOCK}
# ex-AKL cheap, ex-CHCH dear -> putting DUAL on AKL (scenario B) is the cheaper option
async def fake_akl(items, dest): return {"success": True, "customer_price": 30.0 * max(len(items), 1)}
async def fake_chch(items, dest, debug=False): return {"success": True, "customer_price": 60.0 * max(len(items), 1)}

server.get_location_stock = fake_stock
server.live_rates.calculate_auckland_freight = fake_akl
server.live_rates.calculate_freight = fake_chch

def route(dest): return asyncio.run(server._route_decision(dest, ITEMS))

print("Routing gate — dual stock follows geography")
akl_cust = route({"province": "AUK", "postal_code": "0600"})
check("Auckland customer: dual -> AKL (scenario B)", akl_cust["scenario"], "B")
check("  dual item on AKL leg", any(i["variant_id"] == "D" for i in akl_cust["akl_items"]), True)

wgtn_cust = route({"province": "WGN", "postal_code": "6011"})
check("Wellington customer: dual -> CHCH (scenario A, despite AKL cheaper)", wgtn_cust["scenario"], "A")
check("  dual item on CHCH leg", any(i["variant_id"] == "D" for i in wgtn_cust["chch_items"]), True)
check("  must_akl STILL on AKL leg (unaffected by gate)",
      any(i["variant_id"] == "A" for i in wgtn_cust["akl_items"]), True)

si_cust = route({"province": "CAN", "postal_code": "8011"})
check("Christchurch customer: dual -> CHCH", si_cust["scenario"], "A")

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
