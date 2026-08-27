# Unit tests for the oversize/pallet service rules + fail-safes.
# Run: .venv/bin/python scripts/test_service_rules.py
# No network — tests the pure decision logic in live_rates.
import sys, json, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import live_rates as lr

# Sample carrier quotes (shapes mirror the live quote dicts)
CP   = {"carrier": "Post Haste - Freight Forwards", "service": "2-Day", "raw_cost": 20.0}
KIWI = {"carrier": "Kiwi Express", "service": "Economy", "raw_cost": 15.0}
MF   = {"carrier": "Mainfreight", "service": "M2H Two-Man", "raw_cost": 90.0}
DF   = {"carrier": "Dailyfreight", "service": "LCL Palletised", "raw_cost": 60.0}

passed = failed = 0
def check(name, got, want):
    global passed, failed
    ok = got == want
    passed += ok; failed += (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    if not ok:
        print(f"        got={got}\n        want={want}")

def carriers(qs): return sorted(q["carrier"] for q in qs)

# --- Force an oversize list for the test (bypass the JSON file) ---
lr._oversize_ids_cache = {"111"}   # product_id 111 is oversize
OVER = [{"product_id": 111, "quantity": 1}]
NORMAL = [{"product_id": 222, "quantity": 1}]

print("Rule 1 — oversize cart -> force Mainfreight M2H two-man only")
check("drops CP/DF, keeps MF",
      carriers(lr._apply_service_rules([CP, KIWI, MF, DF], OVER, 1.2)),
      ["Mainfreight"])

print("Rule 1 fail-safe — oversize but MF didn't quote -> keep full set (never block)")
check("no MF -> original set kept",
      carriers(lr._apply_service_rules([CP, KIWI, DF], OVER, 1.2)),
      ["Dailyfreight", "Kiwi Express", "Post Haste - Freight Forwards"])

print("Rule 2 — small cart, courier CHEAPER than pallet -> drop pallet (courier wins anyway)")
# CP=20, DF=60 -> non-pallet cheaper -> pallet dropped
check("cheaper courier -> DF dropped",
      carriers(lr._apply_service_rules([CP, MF, DF], NORMAL, 0.3)),
      ["Mainfreight", "Post Haste - Freight Forwards"])

print("Rule 2 refined — small MEDIUM cart, pallet CHEAPEST (no courier) -> KEEP pallet (no perverse +$)")
# only DF(60) + MF(90) two-man; pallet is cheapest -> must NOT force two-man
check("pallet cheapest -> kept (not forced to two-man)",
      carriers(lr._apply_service_rules([MF, DF], NORMAL, 0.68)),
      ["Dailyfreight", "Mainfreight"])

print("Rule 2 fail-safe — small cart but ONLY DF quoted -> keep DF (never block)")
check("only DF -> kept",
      carriers(lr._apply_service_rules([DF], NORMAL, 0.3)),
      ["Dailyfreight"])

print("Rule 3 — big cart (>=0.8m3), nothing oversize -> all eligible")
check("all kept",
      carriers(lr._apply_service_rules([CP, MF, DF], NORMAL, 1.5)),
      ["Dailyfreight", "Mainfreight", "Post Haste - Freight Forwards"])

print("Boundary — exactly 0.8m3 is NOT < 0.8 -> pallet eligible")
check("0.8 keeps DF",
      carriers(lr._apply_service_rules([CP, MF, DF], NORMAL, 0.8)),
      ["Dailyfreight", "Mainfreight", "Post Haste - Freight Forwards"])

print("Empty input -> empty (no crash)")
check("empty stays empty", lr._apply_service_rules([], NORMAL, 1.0), [])

print("Empty oversize list -> _cart_has_oversize False")
lr._oversize_ids_cache = set()
check("no list -> not oversize", lr._cart_has_oversize(OVER), False)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
