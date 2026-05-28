# Deployment

## Hosting: Render (paid Starter)

- Service: `ned-freight-app`
- Plan: **Starter** ($7/month USD) — eliminates cold starts. Do NOT downgrade to Free or checkout will see 30+ second hangs on first request after 15-min idle.
- URL: `https://ned-freight-app.onrender.com`
- Region: Oregon (default)
- Auto-deploy: enabled — every push to `main` redeploys (~1-2 min)
- Health check: `/health` (configured in `render.yaml`)

## Initial setup (already done — for reference)

1. Connect GitHub repo `nedcollections12/ned-freight-app` to Render
2. Render reads `render.yaml`:
   - Type: web
   - Runtime: python
   - Build: `pip install -r requirements.txt`
   - Start: `python server.py`
3. Set environment variables in Render dashboard (see list below)
4. Deploy → check `/health` returns `{"status":"ok","version":"1.0.0"}`
5. Register carrier service with Shopify (one-time):
   - Visit `https://ned-freight-app.onrender.com/shopify/install?shop=nedcollections.myshopify.com`
   - Or POST manually to Shopify Admin API:
     ```bash
     curl -X POST "https://nedcollections.myshopify.com/admin/api/2024-10/carrier_services.json" \
       -H "X-Shopify-Access-Token: $SHOPIFY_ADMIN_TOKEN" \
       -H "Content-Type: application/json" \
       -d '{"carrier_service":{"name":"Standard Delivery","callback_url":"https://ned-freight-app.onrender.com/shopify/rates","service_discovery":true}}'
     ```
6. Update Claude API custom app config:
   - Shopify Admin → Settings → Develop apps → Claude API → Configuration
   - App URL: `https://ned-freight-app.onrender.com`
   - Allowed redirection URLs: `https://ned-freight-app.onrender.com/shopify/callback`
   - Save

## Environment variables (Render dashboard)

| Var | Value (production) | Notes |
|---|---|---|
| `GSS_ACCESS_KEY` | (from .env) | GoSweetSpot API |
| `GSS_SITE_ID` | `3658643` | |
| `SHOPIFY_STORE` | `nedcollections.myshopify.com` | |
| `SHOPIFY_ADMIN_TOKEN` | (from .env) | Admin API token |
| `SHOPIFY_API_KEY` | (from .env) | OAuth |
| `SHOPIFY_API_SECRET` | (from .env) | OAuth |
| `ORIGIN_STREET` | `7 Paradyne Place` | Warehouse, NOT showroom |
| `ORIGIN_SUBURB` | `Wigram` | |
| `ORIGIN_CITY` | `Christchurch` | |
| `ORIGIN_POSTCODE` | `8042` | |
| `ORIGIN_COUNTRY` | `NZ` | |
| `FAF_MULTIPLIER` | `1.30` | Update when fuel surcharge changes |
| `GST_MULTIPLIER` | `1.15` | NZ GST 15% |
| `NED_MARKUP` | `1.10` | NED's margin |
| `FREE_SHIPPING_THRESHOLD` | `999999` | Disable free shipping |
| `APP_URL` | `https://ned-freight-app.onrender.com` | For OAuth |
| `PORT` | `10000` | Render expects this |

## Redeploying

### Via git push (normal flow)
```bash
git add (files)
git commit -m "..."
git push origin main
# Render auto-deploys ~1-2 min
```

### Without code changes (env var update)
- Update env vars in Render dashboard
- Click "Manual Deploy" → "Clear build cache & deploy"
- OR change any env var and Render auto-restarts

### Hot-reload carrier_rates.json (no restart needed)
```bash
curl -X POST "https://ned-freight-app.onrender.com/api/reload-rates"
```

## Rollback

```bash
git log --oneline -5      # find last good commit
git revert <bad-commit>   # creates a revert commit
git push origin main      # auto-deploys
```

Or in Render dashboard: Deploys tab → click any previous successful deploy → "Redeploy this version".

## Monitoring

- Render dashboard → Logs tab → tail in real time
- Health check: `curl https://ned-freight-app.onrender.com/health` (Render also pings this every 30s)
- Test quote: `curl "https://ned-freight-app.onrender.com/api/quote?city=Auckland&cbm=0.5"`

## Common deployment issues

| Symptom | Likely cause | Fix |
|---|---|---|
| 30s+ delays on first request | Downgraded to Free tier | Upgrade to Starter |
| All quotes return "Contact Us" | GSS_ACCESS_KEY/SITE_ID missing or wrong | Update env vars |
| Quotes show wrong city zone | `carrier_rates.json` deployed but cached | POST `/api/reload-rates` |
| Embedded UI blank in Shopify | CSP header missing or wrong | Check `server.py` middleware sets `frame-ancestors` |
| Customer sees old static rates AND NED Freight | Old delivery profile still has static methods | Settings → Shipping → strip static rates from General profile |
