# Tests for the postcode/province fallback rescue (NED3823: a mistyped city must not
# drop a ratable NZ address to $0 "Contact Us"). No network — the live carrier calls
# are mocked to miss; the formula carriers read the real rate card.
# Run: .venv/bin/python scripts/test_postcode_fallback.py
import sys, asyncio, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import live_rates as lr

passed = failed = 0
def check(name, got, want):
    global passed, failed
    ok = got == want; passed += ok; failed += (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ('' if ok else f"\n        got={got} want={want}"))

async def run(dest, *, live_only=True, pallet_rule=True, cp=None, mf_live=None, df_live=None):
    lr.LIVE_ONLY = live_only
    lr.PALLET_RULE_ENABLED = pallet_rule
    lr._oversize_ids_cache = set()
    async def _cp(i, d): return cp
    async def _mf(c, d): return mf_live
    async def _df(c, d): return df_live
    lr.quote_castle_parcels     = _cp   # all live carriers "miss" by default
    lr.quote_mainfreight_live   = _mf
    lr.quote_dailyfreight_live  = _df
    # quote_mainfreight / quote_dailyfreight (formula) stay real -> read carrier_rates.json
    items = [{"grams": 116, "quantity": 1, "price": 108890, "variant_id": 0}]  # ~0.116 m3
    return await lr.calculate_freight(items, dest, debug=True)

# The real failing order: "Roturua" (typo) but a valid Rotorua postcode, LIVE_ONLY on,
# every live carrier missed -> fallback must rescue it to a real quote (not $0).
ROTORUA = {"city": "Roturua", "zip": "3010", "province": "Bay of Plenty", "country": "NZ"}
r = asyncio.run(run(ROTORUA))
check("typo city + valid postcode -> rescued (success)", r["success"], True)
check("rescued via Mainfreight formula", r["chosen_carrier"], "Mainfreight")
check("rescue tagged in source", "[postcode/province fallback]" in (r.get("eligible_quotes",[{}])[0].get("_source","")), True)

# Pallet rule still applies on the rescued set: 0.116 m3 is sub-floor, so Dailyfreight
# (if the card had it) is excluded; Mainfreight wins. Confirm DF is not the choice.
check("sub-floor rescue does not pick the pallet", r["chosen_carrier"] != "Dailyfreight", True)

# Unresolvable address (no postcode, unknown region) -> still no_carrier_match (no false rescue).
UNKNOWN = {"city": "Nowhereville", "zip": "", "province": "", "country": "NZ"}
r2 = asyncio.run(run(UNKNOWN))
check("unresolvable address -> still no_carrier_match", r2["success"], False)

# A live quote present -> rescue must NOT fire (live stays authoritative under LIVE_ONLY).
LIVE_MF = {"carrier": "Mainfreight", "service": "M2H Two-Man", "raw_cost": 90.0}
r3 = asyncio.run(run(ROTORUA, mf_live=LIVE_MF))
check("live quote present -> no fallback tag (live authoritative)",
      "[postcode/province fallback]" not in r3.get("eligible_quotes",[{}])[0].get("_source",""), True)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
