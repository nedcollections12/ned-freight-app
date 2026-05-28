# NED Freight

Live carrier-rate calculator wired into NED Collections' Shopify checkout. Replaces Shopify's static delivery profiles with real-time quotes from Castle Parcels, Mainfreight, and Dailyfreight — picks the cheapest, applies the right markup, returns one clean rate to the customer.

**Status:** ✅ Production. Powers shipping rates for every NED Collections order since 26 May 2026.

## URLs

| | |
|---|---|
| Production app | https://ned-freight-app.onrender.com |
| Admin UI (embedded in Shopify) | Shopify Admin → Apps → Claude API |
| Admin UI (direct) | https://ned-freight-app.onrender.com |
| GitHub | https://github.com/nedcollections12/ned-freight-app |
| Render dashboard | https://dashboard.render.com (search "ned-freight-app") |

## Quick start

```bash
# Clone + run locally
git clone https://github.com/nedcollections12/ned-freight-app
cd ned-freight-app
cp .env.example .env  # fill in credentials
python3 -m pip install -r requirements.txt
python3 server.py
# → http://localhost:10000
```

## Admin UI tabs

The embedded app in Shopify has four tabs:

1. **Product CBMs** — searchable table of every variant + current CBM. Edit inline, auto-saves to Shopify.
2. **Test a quote** — build a real cart from your products, enter a destination, see what NED Freight quotes at checkout (with all carrier comparisons).
3. **Carrier rate cards** — reference: Castle Parcels brackets, Mainfreight flat rates, Dailyfreight zones + volume tiers.
4. **How it works** — system docs for new operators.

## What problem this solves

**Before NED Freight:**
- 10+ static delivery profiles with hand-set rates per CBM band
- Oversized rates didn't cumulate (multiple sofas charged as one)
- B2B vs DTC tier mismatch, no way to factor real carrier costs
- Manual maintenance every time FAF or carrier rates changed

**After NED Freight:**
- One delivery profile, one rate at checkout, always reflects true cost + margin
- Live Castle Parcels quotes via GoSweetSpot API
- Mainfreight & Dailyfreight via cached rate cards (formulas)
- Cheapest carrier auto-selected per cart
- Free pickup option for Canterbury customers
- Single source of truth for all rate data

## See also

- **`CLAUDE.md`** — detailed dev notes (architecture, env vars, deployment, gotchas)
- **`docs/`** — architecture, deployment, troubleshooting
- **`scripts/`** — operational scripts (CBM bulk import, etc.)
- **`../ned-order-split/`** — sister app handling order splits + freight allocation per line
