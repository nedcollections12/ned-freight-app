# Troubleshooting

## Customer reports wrong shipping rate

1. Get the customer's destination (city, postcode, address)
2. Get the cart contents (products + quantities)
3. Open the embedded admin UI → **Test a quote** tab
4. Reproduce the cart and address
5. Compare the result to what they saw at checkout
6. Check **all three carrier quotes** in the result — was the right one picked?

If our quote matches what they saw → the rate is correct, this is a customer-facing pricing question.
If our quote differs from what they saw → likely a CBM data issue (see below).

## Customer's CBM looks wrong (over or under-charged)

1. Open admin UI → **Product CBMs** tab
2. Search for the product
3. Check the current CBM value
4. Compare with Cin7 Option Cubic Meters for that SKU
5. If wrong, edit inline → auto-saves to Shopify
6. Re-test the quote in the Test a quote tab to confirm

Bulk fixes — use `scripts/sync_cbm_from_cin7.py` after exporting a fresh Cin7 product CSV.

## "Freight — Contact Us" shown at checkout

This means our endpoint returned `success: false` for that destination.

Causes (most common first):
1. **Destination city not in our alias map** — Shopify sends a suburb we haven't mapped. Add to `data/carrier_rates.json` → `city_aliases`, redeploy.
2. **Macron in city name not normalised** — should be handled by `_strip_diacritics()` but check the city value in `/shopify/rates/debug` response.
3. **GSS API down or timing out** — our timeout is 4s. MF/DF formulas should always succeed unless their city isn't in the rate card.
4. **Cart has zero CBM** — all items have weight 0. Charge would be just the carrier minimum, which works, but check anyway.

To debug:
```bash
curl -X POST "https://ned-freight-app.onrender.com/shopify/rates/debug" \
  -H "Content-Type: application/json" \
  -d '{"rate":{"destination":{"country":"NZ","city":"CITY","postal_code":"PC","address1":"ADDR"},"items":[{"name":"x","sku":"x","quantity":1,"grams":500,"price":10000}],"currency":"NZD"}}' \
  | python3 -m json.tool
```

## Castle Parcels quotes look unrealistically low

GoSweetSpot sometimes returns Kiwi Express Car-Economy quotes (~$13) for genuinely oversized items.

We have a **safety filter** that drops any GSS quote below `MAX($8, cart_cbm × $40/m³)`. If you see one slip through, the threshold may need tuning in `live_rates.py` → `quote_castle_parcels()` → `min_sensible_per_m3` variable.

## Embedded admin UI shows "refused to connect" in Shopify

CSP issue. Check:
1. `server.py` middleware has `frame-ancestors https://*.myshopify.com https://admin.shopify.com`
2. Render deployed the latest version
3. Claude API custom app config has correct App URL

Hard-refresh the page (Cmd+Shift+R) — the iframe sometimes caches the failed load.

## Admin UI shows "Failed to load" on Carriers tab

Usually a JSON structure change broke a render function. Open browser dev tools → Console → check the JS error. Common culprits:
- Missing field in `carrier_rates.json` (e.g., we removed `satchels` once)
- New zone format the render function doesn't know about

Fix by adding defensive null checks in `static/index.html` rendering functions.

## Pickup option not appearing for a Canterbury customer

Check `_is_canterbury()` in `server.py`:
- Province must be `CAN` or `CANTERBURY` (case-insensitive), OR
- City must be in the hardcoded `canterbury_cities` set, OR
- Postcode must be in range 7000-8999

If a real Canterbury customer doesn't match, add their city to the set or extend the postcode range.

## Carrier service ID changed / service deleted

If "Standard Delivery" disappears from Shopify checkout:

1. List carrier services:
   ```bash
   curl "https://nedcollections.myshopify.com/admin/api/2024-10/carrier_services.json" \
     -H "X-Shopify-Access-Token: $SHOPIFY_ADMIN_TOKEN"
   ```
2. If missing, re-register:
   ```bash
   curl -X POST "https://nedcollections.myshopify.com/admin/api/2024-10/carrier_services.json" \
     -H "X-Shopify-Access-Token: $SHOPIFY_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"carrier_service":{"name":"Standard Delivery","callback_url":"https://ned-freight-app.onrender.com/shopify/rates","service_discovery":true}}'
   ```

Current carrier service ID: **77209862331** (as of May 2026)

## Render deploy fails

Check Render dashboard → Deploys → click the failed deploy → Logs:
- `ModuleNotFoundError` → missing entry in `requirements.txt`
- `port already in use` → restart the service manually
- Build hangs → clear build cache and retry

## A customer's order isn't being charged freight at all

1. Check the order in Shopify — what shipping line was used?
2. If it's "Free Pickup — Wigram Warehouse" → customer chose pickup (Canterbury only)
3. If it's something else → the carrier service may have returned $0 (cart total ≥ `FREE_SHIPPING_THRESHOLD`)
4. If FREE_SHIPPING_THRESHOLD is `999999` and they still got free → check Shopify discount codes / shipping promotions

## Render env var change didn't take effect

Render reads env vars at process start. After updating in dashboard:
- Render auto-restarts the service (look for "Deploy succeeded" in dashboard)
- If not, click "Manual Deploy" → "Clear build cache & deploy"
- Verify: `curl https://ned-freight-app.onrender.com/api/quote?city=Auckland&cbm=0.5` and check the returned `ned_markup` matches your new setting
