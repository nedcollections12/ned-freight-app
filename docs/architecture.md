# Architecture

## System overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  Shopify Checkout                                                   │
│  Customer enters address → Shopify needs shipping rates             │
└────────────────────────────────┬────────────────────────────────────┘
                                 │ POST /shopify/rates
                                 │ { rate: { destination, items, currency } }
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│  NED Freight App (Render, FastAPI)                                  │
│  https://ned-freight-app.onrender.com                               │
│                                                                     │
│  1. Compute cart_cbm = sum(item.grams) / 1000                       │
│  2. Detect Canterbury destination → add Pickup option ($0)          │
│  3. Quote all three carriers in parallel:                           │
│                                                                     │
│     ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐│
│     │ Castle Parcels   │  │ Mainfreight      │  │ Dailyfreight     ││
│     │ (live API)       │  │ (formula)        │  │ (formula)        ││
│     └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘│
│              │                     │                     │          │
│              ↓                     ↓                     ↓          │
│        GoSweetSpot         carrier_rates.json     carrier_rates.json│
│        REST API            (MF section)           (DF zones+tiers)  │
│              │                     │                     │          │
│              └─────────────────────┼─────────────────────┘          │
│                                    ↓                                │
│  4. Apply safety filter (drop GSS quotes < cart_cbm × $40/m³)       │
│  5. Pick cheapest carrier                                           │
│  6. Apply NED markup (×1.10)                                        │
│  7. Round to nearest dollar                                         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │ { rates: [{ service_name, total_price, ... }] }
                                 ↓
                          Customer sees rate
```

## Components

### `server.py` — FastAPI HTTP layer

- Mounts `static/` for the embedded admin UI
- Routes:
  - `POST /shopify/rates` — production Carrier Service callback
  - `POST /shopify/rates/debug` — same but returns all quotes
  - `GET /api/quote` — quick test
  - `GET /api/carrier-info` — rate cards for UI
  - `GET /api/cbm-list` — variant CBMs for admin
  - `PUT /api/cbm` — update one variant's CBM
  - `POST /api/reload-rates` — hot reload carrier_rates.json
- CSP middleware to allow Shopify admin iframe embedding
- Canterbury detection logic (`_is_canterbury`) for pickup eligibility

### `live_rates.py` — carrier quote logic

- `quote_castle_parcels()` — async GSS API call, parses response, applies safety filter
- `quote_mainfreight()` — synchronous formula lookup from carrier_rates.json
- `quote_dailyfreight()` — synchronous formula with zone + volume-tier
- `calculate_freight()` — orchestrates all three, picks cheapest, applies markup
- `_normalise_city()` — strip macrons, lowercase, look up in alias table
- `_strip_diacritics()` — Unicode NFD decomposition (so Wānaka matches "wanaka")
- `_df_tier_index()` — picks the right per-m³ tier (<5m³, 5-10m³, ≥10m³)

### `data/carrier_rates.json` — single source of truth

- `castle_parcels` — reference rate card (live quotes come from GSS, this is for the Carrier Rates tab display)
- `mainfreight.rates` — flat per-destination (base, min, per_m3)
- `dailyfreight.rates` — zone-keyed (e.g., `auckland_z1`, `cromwell_z5`), each with `base` + `tiers[3]`
- `city_aliases` — ~160 suburb-to-zone mappings

### `static/index.html` — embedded admin SPA

- Vanilla JS, no framework
- 4 tabs (Product CBMs, Test a quote, Carrier rates, How it works)
- All data fetched via internal API endpoints
- Hosted at the app root, embedded in Shopify admin via the Claude API custom app's "App URL"

## Data flow at checkout

| Step | What happens | Latency |
|---|---|---|
| 1 | Customer enters delivery address | — |
| 2 | Shopify POSTs cart + destination to `/shopify/rates` | ~50ms |
| 3 | NED Freight builds packages, queries GSS API | ~1-2s |
| 4 | MF + DF formulas computed in-process | <1ms |
| 5 | Cheapest carrier picked + markup applied | <1ms |
| 6 | Response sent back to Shopify | ~50ms |
| 7 | Customer sees "Standard Delivery $X" | total ~3-4s |

On Render Starter tier (no cold starts) the total is consistent. On free tier (which we're not on) the first request after 15 min idle would add 30+ seconds.

## Why this design

**Why one carrier service vs multiple delivery profiles?**
- One source of truth for rates
- Easy to add carriers (Mainfreight API when available)
- Always reflects real cost + margin without manual maintenance

**Why Python + FastAPI on Render?**
- Fast iteration, Shopify-friendly, no cold starts on Starter
- Easy to extend (admin UI, scripts, new endpoints)

**Why store CBM in Shopify's weight field?**
- Standard field, every product has it
- 1m³ = 1kg convention means no unit conversion in the carrier service
- Carriers price on volume anyway — weight field is just a transport for volume

**Why GoSweetSpot API for Castle Parcels but formulas for MF/DF?**
- Castle Parcels has complex per-parcel pricing, hard to model from a flat rate card
- Mainfreight & Dailyfreight have clean per-m³ formulas — fast to compute, no API dependency
- Will swap to MF API when their team grants access (project: open with their rep)
