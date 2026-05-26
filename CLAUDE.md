# Project: NED Freight App

## What this is
Freight cost calculator / zone lookup app for NED Collections.
**Tech stack:** Python, served via Render (render.yaml present)

## Key Files
- `server.py` — main app server
- `zones.py` — freight zone logic
- `render.yaml` — Render deployment config
- `static/` — frontend assets
- `data/` — zone/rate data files

## Credentials
Check `.env` in this folder before asking for any credentials.
If no `.env` exists, ask the user — then create one and save keys there.

## Hard Rules
- **Never modify Cin7 or Shopify data directly**
- Don't auto-deploy to Render without asking first
- Don't push to git without asking first

## Reference Docs
- `@docs/session-log.md` — task log with checkboxes for unfinished work
