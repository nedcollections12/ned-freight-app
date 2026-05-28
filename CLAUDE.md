# NED Freight App

Live carrier-rate calculator for NED Collections' Shopify checkout. Replaces static delivery profiles with real-time quotes from Castle Parcels (live API), Mainfreight & Dailyfreight (formula). Also serves an embedded admin UI inside Shopify.

## What it does

When a customer hits checkout in Shopify, Shopify POSTs the cart + destination to our `/shopify/rates` endpoint. We:

1. Sum the cart's CBM (each item's weight in Shopify = CBM in kg, 1m³ = 1kg)
2. Query Castle Parcels via GoSweetSpot API (live quote, per-parcel pricing)
3. Calculate Mainfreight + Dailyfreight from rate-card formulas
4. Pick the cheapest carrier across all three
5. Apply FAF (30%) × GST (15%) × NED markup (10%)
6. Return a single "Standard Delivery — $X" rate to checkout

Plus a free **Pickup option** for Canterbury customers (detected by province/city/postcode).

## Architecture

```
Customer at Shopify checkout
       ↓
Shopify POSTs cart+address → /shopify/rates  (this app, on Render)
       ↓
   ├─ Castle Parcels  → GoSweetSpot API (live, ~1-2s)
   ├─ Mainfreight    → formula from data/carrier_rates.json
   └─ Dailyfreight   → formula with zone+tier from carrier_rates.json
       ↓
Pick cheapest, apply FAF × GST × NED markup (×1.6445)
       ↓
Return "Standard Delivery $X" to Shopify  (also Pickup if Canterbury)
       ↓
Customer sees rate in checkout
```

**Hosting:** Render (paid Starter tier — no cold starts).
**Code:** Python 3.12, FastAPI + httpx.
**Domain:** `https://ned-freight-app.onrender.com`
**Embedded in Shopify** at: Apps → Claude API.

## Key files

| File | Purpose |
|---|---|
| `server.py` | FastAPI app — routes (`/shopify/rates`, `/api/cbm-list`, `/api/cbm`, `/api/carrier-info`, embedded admin UI), Canterbury pickup logic, Shopify HMAC/OAuth |
| `live_rates.py` | Carrier quote logic (CP via GSS API, MF/DF from rate card), safety filter for GSS anomalies, FAF/GST/markup math |
| `data/carrier_rates.json` | Single source of truth for MF/DF/CP rate cards + city aliases (~160 entries mapping suburbs to Dailyfreight zones) |
| `static/index.html` | Embedded admin SPA (4 tabs: Product CBMs, Test a quote, Carrier rates, How it works) |
| `zones.py` | Legacy NZ zone detection — only still used for Canterbury pickup eligibility |
| `render.yaml` | Render deployment config (Starter plan, health check at `/health`) |
| `.env` | Local credentials (gitignored) — see `.env.example` |

## Environment variables

Set in Render dashboard (Settings → Environment). All required unless marked optional.

| Var | Purpose |
|---|---|
| `GSS_ACCESS_KEY` | GoSweetSpot API auth (Castle Parcels live quotes) |
| `GSS_SITE_ID` | GoSweetSpot site ID |
| `SHOPIFY_STORE` | `nedcollections.myshopify.com` |
| `SHOPIFY_ADMIN_TOKEN` | Admin API token for CBM admin UI read/write |
| `SHOPIFY_API_KEY` / `SHOPIFY_API_SECRET` | For OAuth flow (only used if re-registering carrier service) |
| `ORIGIN_STREET` / `ORIGIN_SUBURB` / `ORIGIN_CITY` / `ORIGIN_POSTCODE` | Warehouse address for GSS quotes — must be Wigram not Sydenham |
| `FAF_MULTIPLIER` | Fuel adjustment factor (default 1.30) |
| `GST_MULTIPLIER` | NZ GST (default 1.15) |
| `NED_MARKUP` | NED margin (default 1.10) |
| `FREE_SHIPPING_THRESHOLD` | Free shipping over this cart value (default 999999 = effectively off) |
| `APP_URL` | `https://ned-freight-app.onrender.com` (used by OAuth callback) |

## Common operations

### Update a carrier rate (e.g., FAF change, new MF/DF card)
1. Edit `data/carrier_rates.json`
2. Commit + push → Render auto-deploys (~1-2 min), OR
3. Edit on server + POST `/api/reload-rates` for hot reload

### Test a quote
- **Embedded admin UI**: Shopify → Apps → Claude API → Test a quote tab
- **Direct URL**: `https://ned-freight-app.onrender.com/api/quote?city=Auckland&cbm=0.5`
- **Debug payload**: POST `/shopify/rates/debug` returns all carrier quotes

### Update a product's CBM
- Embedded admin UI → Product CBMs tab → search → edit inline → auto-saves to Shopify

### Bulk re-sync all products from Cin7
- Run `scripts/sync_cbm_from_cin7.py` (rebuilds Shopify weights from Cin7 Cubic Meters CSV)

### Add a new city/suburb to Dailyfreight zone
- Edit `data/carrier_rates.json` → `city_aliases` section
- Map the suburb name (lowercase) to a hub+zone key like `"queenstown": "cromwell_z5"`

### Re-register carrier service with Shopify (if accidentally deleted)
- Visit `https://ned-freight-app.onrender.com/shopify/install?shop=nedcollections.myshopify.com`
- Auto-creates "NED Freight" carrier service pointing to our app

## Pricing logic in detail

**Castle Parcels (Post Haste) — live via GoSweetSpot:**
- API returns "Cost" already incl. FAF + GST + 8% GSS markup
- We strip the 8% GSS markup → `true_cost = api_cost / 1.08`
- Customer price = `true_cost × 1.10` (NED markup)

**Mainfreight (formula):**
- Rate card has `base`, `min`, `per_m3` per destination — flat single zone
- `charge = MAX(min, base + per_m3 × cart_cbm) × 1.30 (FAF) × 1.15 (GST)`
- Customer price = `charge × 1.10` (NED markup)

**Dailyfreight (formula with zones + tiers):**
- Rate card has `base` + three `tiers` (per-m³ rates) per zone
- Tier 0 (<5m³), tier 1 (5-10m³), tier 2 (≥10m³) — per-m³ drops with cart size (volume discount)
- Hubs with subzones: Auckland (z1/z3/z9), Cromwell (z1/z5), Hamilton (z1/z3/z10)
- City alias map routes the customer's suburb to a specific zone (e.g., Wanaka → `cromwell_z5`)
- `charge = MAX(base, per_m3_tier × cart_cbm) × 1.30 × 1.15`
- Customer price = `charge × 1.10`

**Safety filter:** GSS occasionally returns Kiwi Express Car-Economy quotes that are unrealistically low for large items (~$13 for a 1.5m³ sofa). We drop any GSS quote below `MAX($8, cart_cbm × $40/m³)` — falls back to MF/DF formulas which always give sensible rates.

## Endpoints reference

| Method | Path | Purpose |
|---|---|---|
| POST | `/shopify/rates` | Shopify Carrier Service callback (production) |
| POST | `/shopify/rates/debug` | Same, but returns all carrier quotes for diagnosis |
| GET | `/api/quote?city=X&cbm=Y&qty=Z` | Quick test via query params |
| GET | `/api/carrier-info` | Returns full carrier_rates.json (for Carriers tab UI) |
| GET | `/api/cbm-list` | All active variants + current weight |
| PUT | `/api/cbm` | Update one variant's weight in Shopify |
| POST | `/api/reload-rates` | Hot-reload carrier_rates.json without restart |
| GET | `/health` | Render health check |
| GET | `/` | Embedded admin UI (4-tab SPA) |

## Gotchas / things to watch

- **GoSweetSpot Cost includes FAF + GST already** — confirmed with Castle Parcels rep. Don't double-apply. Our code strips only the 8% GSS markup.
- **Origin must be the warehouse (Wigram)** not the showroom (Sydenham) — GSS quotes pickup-location-dependent.
- **Macrons in Māori place names** — Shopify sends "Wānaka", we normalise via `_strip_diacritics` to match "wanaka" in alias map.
- **Carrier service is registered under "Claude API" custom app** — not a separate Shopify app. Token is the admin token we use for everything.
- **Shopify weights are CBM** — every product's weight field is its CBM in kg-equivalent. 1m³ = 1.0kg. Carriers price on volume so the unit conversion is transparent.
- **Render free tier sleeps after 15min idle** — we're on Starter plan to avoid cold starts. If accidentally downgraded, checkout will see 30+ second hangs.
- **`shop.email_logo_url` and `shop.email_accent_color` are unreliable in Liquid** — for the order confirmation template, hardcode the button color rather than relying on the Liquid variable.

## Hard rules

- Never modify Cin7 data directly — only read
- Never push to git without asking first
- Never auto-deploy to Render without asking first
- Don't delete the carrier service (id: 77209862331, name "Standard Delivery") without re-registering

## Related projects

- `../ned-order-split/` — Order Handler app (splits B2B 20th-terms orders, allocates freight by CBM, customizes order confirmation email)
- `../agents/` — broader NED Agents suite (email handler, procurement, stock management — separate scope)

## Reference docs

- `docs/architecture.md` — full system architecture + sequence diagrams
- `docs/deployment.md` — Render setup, env vars, redeploy steps
- `docs/carrier-rates-reference.md` — explanation of zone+tier system, rate sources, when to update
- `docs/troubleshooting.md` — common issues + how to diagnose
