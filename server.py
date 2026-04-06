"""NED Freight App — FastAPI Server"""

import json, math, os
from pathlib import Path
from typing import Optional
import uvicorn
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from zones import detect_zone, get_oversized_zone

BASE_DIR = Path(__file__).parent
RATES_FILE = BASE_DIR / "data" / "rates.json"
PRODUCTS_FILE = BASE_DIR / "data" / "oversized_products.json"

app = FastAPI(title="NED Freight App", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def load_rates():
    with open(RATES_FILE) as f: return json.load(f)

def save_rates(data):
    with open(RATES_FILE, "w") as f: json.dump(data, f, indent=2)

def load_products():
    return json.loads(PRODUCTS_FILE.read_text()) if PRODUCTS_FILE.exists() else {}

def save_products(data):
    PRODUCTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PRODUCTS_FILE.write_text(json.dumps(data, indent=2))

def lookup_standard_rate(rates, zone_id, order_value):
    zone_data = next((z for z in rates["standard"]["zones"] if z["id"] == zone_id), None)
    if zone_data and zone_data.get("by_request"): return None
    zone_rates = rates["standard"]["rates"].get(zone_id)
    if not zone_rates: return None
    for i, tier in enumerate(rates["standard"]["tiers"]):
        if tier["min"] <= order_value <= tier["max"]:
            r = zone_rates[i]; return float(r) if r is not None else None
    return None

def lookup_oversized_rate(rates, oz_zone_id, cbm):
    zone_data = next((z for z in rates["oversized"]["zones"] if z["id"] == oz_zone_id), None)
    if zone_data and zone_data.get("by_request"): return None
    zone_rates = rates["oversized"]["rates"].get(oz_zone_id)
    if not zone_rates: return None
    cats = rates["oversized"]["categories"]
    for i, cat in enumerate(cats):
        if cat["min_cbm"] <= cbm < cat["max_cbm"]:
            r = zone_rates[i]; return float(r) if r is not None else None
    if cbm >= cats[-1]["min_cbm"]:
        r = zone_rates[-1]; return float(r) if r is not None else None
    return None

@app.post("/shopify/rates")
async def shopify_rates(request: Request):
    try: body = await request.json()
    except: raise HTTPException(400, "Invalid JSON")
    rate_request = body.get("rate", {})
    destination = rate_request.get("destination", {})
    items = rate_request.get("items", [])
    province = destination.get("province", "")
    city = destination.get("city", "")
    postcode = destination.get("zip", "")
    is_rural = str(destination.get("address2", "")).lower().strip() == "rural"
    std_zone = detect_zone(province=province, city=city, postcode=postcode)
    oz_zone = get_oversized_zone(std_zone)
    rates_data = load_rates()
    settings = rates_data["settings"]
    free_threshold = settings.get("free_freight_threshold", 7500)
    rural_surcharge = settings.get("rural_surcharge", 14)
    products = load_products()
    order_value = 0.0; total_cbm = 0.0; has_oversized = False
    for item in items:
        qty = int(item.get("quantity", 1))
        price = int(item.get("price", 0)) / 100.0
        order_value += price * qty
        cbm_each = None
        for key in [str(item.get("variant_id","")), str(item.get("product_id",""))]:
            if key in products: cbm_each = products[key].get("cbm"); break
        if cbm_each and float(cbm_each) > 0.160:
            has_oversized = True; total_cbm += float(cbm_each) * qty
    currency = settings.get("currency", "NZD")
    if order_value >= free_threshold:
        return {"rates": [{"service_name":"Free Freight","service_code":"FREE","total_price":"0","currency":currency}]}
    if has_oversized and total_cbm > 0:
        oz_rate = lookup_oversized_rate(rates_data, oz_zone, total_cbm)
        if oz_rate is not None:
            total = math.ceil(oz_rate + (rural_surcharge if is_rural else 0))
            return {"rates": [{"service_name":"Freight" + (" (Rural)" if is_rural else ""),"service_code":"OVERSIZED","total_price":str(int(total*100)),"currency":currency}]}
        return {"rates": [{"service_name":"Freight — Contact Us","service_code":"BY_REQUEST","total_price":"0","currency":currency}]}
    std_rate = lookup_standard_rate(rates_data, std_zone, order_value)
    if std_rate is not None:
        total = math.ceil(std_rate + (rural_surcharge if is_rural else 0))
        return {"rates": [{"service_name":"Freight" + (" (Rural)" if is_rural else ""),"service_code":"STANDARD","total_price":str(int(total*100)),"currency":currency}]}
    return {"rates": [{"service_name":"Freight — Contact Us","service_code":"BY_REQUEST","total_price":"0","currency":currency}]}

@app.get("/api/rates")
async def get_rates(): return load_rates()

@app.put("/api/rates")
async def update_rates(request: Request):
    save_rates(await request.json()); return {"status":"saved"}

@app.put("/api/settings")
async def update_settings(request: Request):
    body = await request.json(); rates = load_rates()
    rates["settings"].update(body); save_rates(rates); return {"status":"saved"}

@app.get("/api/products")
async def get_products(): return load_products()

@app.put("/api/products/{product_id}")
async def upsert_product(product_id: str, request: Request):
    products = load_products(); products[product_id] = await request.json()
    save_products(products); return {"status":"saved"}

@app.delete("/api/products/{product_id}")
async def delete_product(product_id: str):
    products = load_products()
    if product_id not in products: raise HTTPException(404, "Not found")
    del products[product_id]; save_products(products); return {"status":"deleted"}

@app.post("/api/upload/{rate_type}")
async def upload_rates(rate_type: str, file: UploadFile = File(...)):
    content = await file.read(); fname = file.filename or ""
    if fname.endswith(".csv"):
        import csv, io
        rows = list(csv.reader(io.StringIO(content.decode("utf-8-sig"))))
    elif fname.endswith((".xlsx",".xls")):
        import openpyxl, io
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        rows = [[str(c.value or "") for c in row] for row in wb.active.iter_rows()]
    else: raise HTTPException(400, "Use .csv or .xlsx")
    rates = load_rates(); rate_key = "standard" if rate_type == "standard" else "oversized"
    updated = []; errors = []
    for row in rows:
        if not row or not row[0] or row[0].lower() in ("zone_id","zone",""): continue
        zid = row[0].strip().lower()
        try: new = [float(v) if v.strip() not in ("","null","None") else None for v in row[1:11]]
        except ValueError as e: errors.append(f"{zid}: {e}"); continue
        if zid in rates[rate_key]["rates"]:
            rates[rate_key]["rates"][zid] = new; updated.append(zid)
        else: errors.append(f"Unknown zone: {zid}")
    save_rates(rates)
    return {"status":"ok","updated_zones":updated,"errors":errors}

@app.get("/api/test-rate")
async def test_rate(province:str="", city:str="", postcode:str="", order_value:float=0, cbm:float=0, is_rural:bool=False):
    rates_data = load_rates(); settings = rates_data["settings"]
    free_threshold = settings.get("free_freight_threshold",7500)
    rural_surcharge = settings.get("rural_surcharge",14) if is_rural else 0
    std_zone = detect_zone(province=province, city=city, postcode=postcode)
    oz_zone = get_oversized_zone(std_zone)
    result = {"zone":std_zone,"oz_zone":oz_zone,"order_value":order_value,"cbm":cbm,"is_rural":is_rural}
    if order_value >= free_threshold:
        result.update({"freight_charge":0,"service":"FREE"}); return result
    if cbm > 0.160:
        rate = lookup_oversized_rate(rates_data, oz_zone, cbm)
        result.update({"service":"OVERSIZED","base_rate":rate,"freight_charge":math.ceil(rate+rural_surcharge) if rate is not None else None})
    else:
        rate = lookup_standard_rate(rates_data, std_zone, order_value)
        result.update({"service":"STANDARD","base_rate":rate,"freight_charge":math.ceil(rate+rural_surcharge) if rate is not None else None})
    return result

@app.get("/health")
async def health(): return {"status":"ok","version":"1.0.0"}

static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
