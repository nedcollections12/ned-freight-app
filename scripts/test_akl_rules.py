# Tests for the Auckland path: service rules + live/formula DF fallback.
# Run: .venv/bin/python scripts/test_akl_rules.py   (no network — carrier calls are mocked)
import sys, asyncio, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import live_rates as lr

MF_AKL = {"carrier": "Mainfreight", "service": "M2H Two-Man (ex-Auckland)", "raw_cost": 120.0}
DF_LIVE = {"carrier": "Dailyfreight", "service": "LCL Palletised", "raw_cost": 80.0}
DF_FORM = {"carrier": "Dailyfreight", "service": "LCL Palletised (ex-Auckland)", "raw_cost": 85.0}

passed = failed = 0
def check(name, got, want):
    global passed, failed
    ok = got == want; passed += ok; failed += (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ('' if ok else f"\n        got={got} want={want}"))

async def run(items, *, live_only, pallet_rule, mf=MF_AKL, df_live=DF_LIVE, df_form=DF_FORM):
    lr.LIVE_ONLY = live_only
    lr.PALLET_RULE_ENABLED = pallet_rule
    async def _mf(c, d): return mf
    async def _dfl(c, d): return df_live
    def _dff(c, d, override_key=None): return df_form
    lr.quote_mainfreight_akl_live = _mf
    lr.quote_dailyfreight_akl_live = _dfl
    lr.quote_dailyfreight_akl = _dff
    return await lr.calculate_auckland_freight(items, {"city": "Auckland", "postal_code": "1010"})

OVER = [{"product_id": 111, "quantity": 1}]
NORMAL = [{"product_id": 222, "quantity": 1}]

# Oversize item held in Auckland -> forced onto ex-AKL Mainfreight two-man (not the cheaper pallet)
lr._oversize_ids_cache = {"111"}
r = asyncio.run(run(OVER, live_only=False, pallet_rule=True))
check("oversize ex-AKL -> two-man (not cheaper DF pallet)", r["chosen_carrier"], "Mainfreight")

# Normal cart -> cheapest wins (DF pallet 80 < MF 120)
lr._oversize_ids_cache = set()
r = asyncio.run(run(NORMAL, live_only=False, pallet_rule=True))
check("normal ex-AKL -> cheapest (Dailyfreight)", r["chosen_carrier"], "Dailyfreight")

# LIVE_ONLY on, live DF present -> uses live DF (service 'LCL Palletised', not the '(ex-Auckland)' formula)
r = asyncio.run(run(NORMAL, live_only=True, pallet_rule=False))
check("LIVE_ONLY -> live DF used", r["chosen_service"], "LCL Palletised")

# LIVE_ONLY on, live DF MISSES -> no formula fallback -> only Mainfreight remains
r = asyncio.run(run(NORMAL, live_only=True, pallet_rule=False, df_live=None))
check("LIVE_ONLY + DF miss -> Mainfreight only (no formula)", r["chosen_carrier"], "Mainfreight")

# LIVE_ONLY OFF, live DF MISSES -> formula DF backstops
r = asyncio.run(run(NORMAL, live_only=False, pallet_rule=False, df_live=None))
check("flags off + DF miss -> formula DF backstop", r["chosen_service"], "LCL Palletised (ex-Auckland)")

# Fail-safe: oversize but Mainfreight ex-AKL missed -> keep DF (never no-rate)
lr._oversize_ids_cache = {"111"}
r = asyncio.run(run(OVER, live_only=False, pallet_rule=True, mf=None))
check("oversize but no MF ex-AKL -> DF kept (never block)", r["chosen_carrier"], "Dailyfreight")

lr._oversize_ids_cache = set()
print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
