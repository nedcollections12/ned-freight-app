# Freight decision: South Island customers + Auckland‑only stock

**Status:** open — needs a business decision before Auckland routing goes live.
**Date raised:** 2026‑07‑20

## The situation

Stock lives in two branches:
- **Christchurch (Main)** — the default/home branch.
- **Auckland 3PL** — holds a subset of stock (mostly larger items).

Some items are currently **only available in Auckland** — i.e. Christchurch is out of
stock (often oversold to 0/negative, awaiting a restock PO). When a customer orders one
of these, it can either **ship from Auckland now**, or **wait and ship from Christchurch**
once CHCH is restocked.

The freight economics of that choice depend entirely on **where the customer is**:

| Example: Drift Rose Taupe (1.12 m³), CHCH out / AKL has 3 | Ship ex‑Auckland | Ship ex‑CHCH |
|---|---|---|
| To Auckland (NI) | **~$113** (local) | ~$146 (interisland) |
| To Wellington (NI) | ~$203 | ~$114 |
| To **Christchurch (SI)** | **~$288** (interisland) | ~$45 (local) |
| To **Dunedin (SI)** | **~$374** (interisland) | ~$106 |

- **North Island customers:** shipping from Auckland is **cheaper** — great, do it.
- **South Island customers:** shipping the Auckland unit interisland is **brutal**
  (~$288–$374, roughly a third of the item's value). It would kill the sale.

## What happens today (routing OFF)

Checkout quotes everything **ex‑Christchurch**, so an SI customer sees the cheap
local price (~$45) — but since CHCH has no stock, the order **backorders** and ships
from CHCH once restocked. So today's behaviour is effectively *"cheap price, but wait."*
(It also **under‑charges NI customers' interisland quote and over‑charges nothing** —
the pricing is just wrong in both directions because it ignores where stock actually is.)

## The decision (SI customer + item that's out at CHCH but in Auckland)

**Option A — Backorder at Christchurch (cheap, delayed).**
Don't ship the Auckland unit interisland. Quote ex‑CHCH (~$45) and fulfil from CHCH when
restocked. Auckland stock stays available for NI customers.
*Pro:* sane freight, protects margin. *Con:* customer waits — depends on CHCH restock ETA.

**Option B — Ship from Auckland now (true cost, immediate).**
Charge the real interisland freight (~$288) and ship immediately.
*Pro:* correct cost, no wait, depletes Auckland stock. *Con:* almost certainly won't convert.

**Option C — Cheaper of the two, automatically (recommended default).**
Compare ex‑Auckland vs ex‑CHCH(‑backorder) per cart and quote the cheaper. NI orders
ship from Auckland (cheap); SI orders backorder at CHCH (cheap). Best economics.
*Con:* SI fulfilment then depends on CHCH restock timing — need reliable ETAs.

**Option D — Offer the customer the choice.**
Show two options at checkout: *"Ship now from Auckland – $288"* and *"Backorder, ships
from Christchurch – $45 (allow extra time)."* Let them decide.
*Pro:* transparent, no lost sales. *Con:* more checkout complexity; needs clear ETA messaging.

**Option E — Subsidise interisland freight.**
Charge a capped amount (e.g. $X) and NED absorbs the rest to keep SI conversion.
*Pro:* keeps sales. *Con:* margin hit per order; needs a policy on the cap.

## Questions for the team

1. Is Auckland stock **meant** to serve interisland (SI) orders at all, or is it
   positioned purely to serve NI cheaply?
2. What are realistic **CHCH restock ETAs** for these lines? (Determines whether
   "backorder at CHCH" is acceptable to customers.)
3. Appetite to **subsidise** interisland freight vs. let the customer choose vs. just
   backorder?

## Recommendation

**Option C** (cheaper of the two) as the automatic default — it naturally sends NI
orders to Auckland (where it's cheapest and depletes 3PL holding cost) and SI orders to
a CHCH backorder (avoiding the prohibitive interisland charge), with **no manual work**.
If restock ETAs are long or unreliable, layer **Option D** (give SI customers the explicit
choice) on top so no sale is silently delayed.

## Implementation note

The router already computes ex‑AKL vs ex‑CHCH for every item; today it *forces*
out‑at‑CHCH items to ship ex‑Auckland. Option C is a change to let those items fall back
to an ex‑CHCH (backorder) quote when it's cheaper — small, contained, and it removes the
$288/$374 SI outliers. This is the last logic decision before Auckland routing can go live.
