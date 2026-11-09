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

# NED4200: 33xx postcodes are mixed. Morrinsville/Te Aroha RDs must NOT resolve to Taupō.
check("3371 Morrinsville RD -> Hamilton hub", lr._fallback_keys({"zip": "3371"})[0], "hamilton")
check("3392 Te Aroha RD -> Hamilton hub",     lr._fallback_keys({"zip": "3392"})[0], "hamilton")
check("3330 Taupō -> Taupō",                  lr._fallback_keys({"zip": "3330"})[0], "taupo")
check("3378 Taupō RD -> Taupō",               lr._fallback_keys({"zip": "3378"})[0], "taupo")
MORR = {"city": "Morrinsville", "zip": "3371", "province": "WKO", "country": "NZ",
        "address1": "1204 Tahuroa Road", "address2": "RD1"}
check("correct town not rewritten to Taupō", lr._resolve_destination_city(MORR)[1], None)

# Rural courier cart with CP missing but MF live: keep the real freight quote, not 8%.
check("RD1 detected as rural",  lr._is_rural_address(MORR), True)
check("urban street not rural", lr._is_rural_address({"address1": "12 Rd Street", "zip": "8011"}), False)
r4 = asyncio.run(run(MORR, mf_live=LIVE_MF))
check("rural + MF live -> no courier fallback", r4["chosen_carrier"], "Mainfreight")
URBAN = {"city": "Rotorua", "zip": "3010", "province": "BOP", "country": "NZ", "address1": "5 Main St"}
r5 = asyncio.run(run(URBAN, mf_live=LIVE_MF))
check("urban + CP miss -> courier fallback still fires", r5.get("rule_applied"), "courier_fallback")
check("fallback keeps replaced MF quote in log (flagged)",
      any(q.get("_discarded") == "courier_fallback" and q["carrier"] == "Mainfreight"
          for q in r5["all_quotes"]), True)

# Misspelt towns snap to the right town only when the postcode agrees.
check("typo Morinsville -> Morrinsville",
      lr._resolve_destination_city({"city": "Morinsville", "zip": "3371"})[1], ("Morinsville", "Morrinsville"))
check("typo Christchurvh -> Christchurch",
      lr._resolve_destination_city({"city": "Christchurvh", "zip": "7678"})[1], ("Christchurvh", "Christchurch"))
check("typo with far-away postcode NOT corrected", lr._fuzzy_town("Wellingtn", {"zip": "9016"}), None)
check("real suburb left alone", lr._fuzzy_town("Whangaparaoa", {"zip": "0932"}), None)

# Rural fee is per carton, only when GSS didn't already flag rural.
lr.RURAL_SURCHARGE_ENABLED = True
check("rural fee x3 cartons", lr._rural_surcharge(MORR, False, "Post Haste", 3), round(lr.RURAL_SURCHARGE_COURIER * 3, 2))
check("no double fee when GSS flagged rural", lr._rural_surcharge(MORR, True, "Post Haste", 3), 0.0)
check("no fee on urban address", lr._rural_surcharge(URBAN, False, "Post Haste", 3), 0.0)
# Rural address no carrier or formula can place -> % fallback, plus the rural fee.
r6 = asyncio.run(run({"city": "Nowhereville", "address2": "RD 2", "zip": "", "country": "NZ"}))
check("rural last-resort fallback carries rural fee", "rural $" in r6["all_quotes"][0]["_source"], True)

# Postcode table: Western BoP RDs are Tauranga, not Rotorua; former gaps now resolve.
for pc, hub in [("3173", "tauranga"), ("3187", "tauranga"), ("3129", "tauranga"), ("3120", "rotorua"),
                ("3194", "rotorua"), ("3620", "thames"), ("3800", "hamilton"), ("3920", "hamilton"),
                ("4702", "palmerston north"), ("5711", "wellington"), ("7005", "nelson"),
                ("9230", "dunedin"), ("9600", "invercargill")]:
    check(f"postcode {pc} -> {hub}", lr._fallback_keys({"zip": pc})[0], hub)
check("Tauranga 3187 not rewritten to Rotorua",
      lr._resolve_destination_city({"city": "Tauranga", "zip": "3187"})[1], None)
check("Lawrence 9591 keeps its town", lr._resolve_destination_city({"city": "Lawrence", "zip": "9591"})[1], None)
_k = sorted(lr._POSTCODE_KEYS)
check("postcode ranges don't overlap", all(a[1] < b[0] for a, b in zip(_k, _k[1:])), True)

# Courier fallback is priced by volume, not order value (NED4200: trade carts had GST
# stripped twice). Same cart, retail vs trade prices -> same freight.
def _fb(dest, price_cents):
    async def go():
        lr.LIVE_ONLY = True; lr._oversize_ids_cache = set()
        async def miss(*a): return None
        lr.quote_castle_parcels = miss
        items = [{"grams": 80, "quantity": 1, "price": price_cents, "variant_id": 0}]
        return await lr.calculate_freight(items, dest, debug=True)
    return asyncio.run(go())
lr.COURIER_FALLBACK_MODE = "cbm"
AKL = {"city": "Auckland", "zip": "1010", "province": "AUK", "country": "NZ", "address1": "1 Queen St"}
CHC = {"city": "Christchurch", "zip": "8011", "province": "CAN", "country": "NZ", "address1": "1 High St"}
ra, rb = _fb(AKL, 100000), _fb(AKL, 86957)
check("fallback same for retail vs trade-priced cart", ra["customer_price"], rb["customer_price"])
check("fallback uses volume", "by volume" in ra["all_quotes"][0]["_source"], True)
import math
check("NI fallback = ($8.55 + $340/m3) x markup", ra["customer_price"],
      round((8.55 + 340*0.08) * lr.NED_MARKUP, 2))
check("SI fallback cheaper than NI", _fb(CHC, 100000)["customer_price"] < ra["customer_price"], True)
lr.COURIER_FALLBACK_MODE = "pct"
check("pct mode still available", "% of ex-GST" in _fb(AKL, 100000)["all_quotes"][0]["_source"], True)
lr.COURIER_FALLBACK_MODE = "cbm"

# Oversize rural fee: once per delivery (measured), courier: per carton.
check("oversize rural fee per consignment", lr._rural_surcharge(MORR, False, "Kiwi Express Oversize", 3), round(lr.RURAL_SURCHARGE_OVERSIZE, 2))

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
