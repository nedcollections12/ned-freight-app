# Parse a Mainfreight / Daily Freight weekly statement PDF into per-consignment
# rows. These statements (emailed to accounts@, one per account/week) share a
# fixed tabular layout — one line per consignment:
#
#   15/10 FWM63952603 CHH AKL 02 COCOHILLS [R] 2 1.97 60 57.01 32.70% 231.34 266.04
#   date  ourRef      org dst zn receiver   RS it cbm kg  fuel$ fuel%  exGST  inclGST
#
# Returns (meta, consignments) where meta carries the account/week/ending-date
# and the statement's own GST totals so the caller can assert the parse is
# complete (sum of line ex-GST == statement ex-GST).
import re
import datetime as _dt
import pdfplumber

_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

# One consignment line. Receiver is non-greedy up to the [R]/[S] sender/receiver tag.
_LINE = re.compile(
    r"^(?P<date>\d{2}/\d{2})\s+"
    r"(?P<ref>[A-Z]{2,4}\d{6,})\s+"
    r"(?P<origin>[A-Z]{3})\s+(?P<dest>[A-Z]{3})\s+(?P<zone>\d{2})\s+"
    # receiver or surcharge description, with or without a trailing [R]/[S] tag
    r"(?P<receiver>.+?)\s+"
    r"(?P<items>[\d,]+)\s+(?P<cbm>[\d.,]+)\s+(?P<kg>[\d,]+)\s+"
    r"(?P<fuel>-?[\d.,]+)\s+(?P<fuelpct>[\d.]+)%\s+"
    r"(?P<excl>-?[\d.,]+)\s+(?P<incl>-?[\d.,]+)$"
)


def _f(s):
    return float(str(s).replace(",", ""))

# NZ branch/destination codes → region label (extend as new codes appear).
_DEST_REGION = {
    "AKL": "Auckland", "WGN": "Wellington", "CHH": "Christchurch", "HAM": "Hamilton",
    "PMA": "Palmerston North", "NPY": "New Plymouth", "NAP": "Napier", "TPO": "Taupo",
    "QUE": "Queenstown", "CBG": "Cambridge", "WKE": "Waikanae", "WAN": "Wanaka",
    "RND": "Rangiora", "DUN": "Dunedin", "NEL": "Nelson", "TAU": "Tauranga",
    "ROT": "Rotorua", "GIS": "Gisborne", "BLE": "Blenheim", "INV": "Invercargill",
    "TIM": "Timaru", "OAM": "Oamaru", "WHK": "Whakatane", "WHG": "Whangarei",
}


def _parse_header(text):
    """Pull account code, week number, week-ending date and ex-GST total."""
    meta = {"account": None, "week": None, "weekEnding": None, "statementExclGst": None}
    m = re.search(r"Customer Code\s+([A-Z0-9]+)", text)
    if m:
        meta["account"] = m.group(1)
    m = re.search(r"Week Number\s+(\d+)", text)
    if m:
        meta["week"] = m.group(1)
    m = re.search(r"Week Ending\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
    if m:
        d, mon, y = int(m.group(1)), _MONTHS.get(m.group(2)), int(m.group(3))
        if mon:
            meta["weekEnding"] = _dt.date(y, mon, d)
    # "Charge (excl GST) 4,659.84" in the GST-details box
    m = re.search(r"Charge \(excl GST\)\s+([\d,]+\.\d{2})", text)
    if m:
        meta["statementExclGst"] = float(m.group(1).replace(",", ""))
    return meta


def _carrier_from_account(account):
    if not account:
        return "Mainfreight"
    if account.endswith("DF"):
        return "Daily Freight"
    return "Mainfreight"


def parse_statement(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    meta = _parse_header(text)
    year = meta["weekEnding"].year if meta["weekEnding"] else _dt.date.today().year
    carrier = _carrier_from_account(meta["account"])
    rows = []
    for ln in text.split("\n"):
        m = _LINE.match(ln.strip())
        if not m:
            continue
        g = m.groupdict()
        dd, mm = g["date"].split("/")
        # Week can straddle a year boundary; weekEnding anchors the year.
        ship = _dt.date(year, int(mm), int(dd))
        if meta["weekEnding"] and (ship - meta["weekEnding"]).days > 20:
            ship = _dt.date(year - 1, int(mm), int(dd))
        rows.append({
            "consignmentRef": g["ref"],
            "carrier": carrier,
            "carrierAccount": meta["account"],
            "statementWeek": meta["week"],
            "shipmentDate": ship.isoformat(),
            "receiver": re.sub(r"\s*\[[RS]\]\s*$", "", g["receiver"]).strip(),
            "originDest": f"{g['origin']} {g['dest']} {g['zone']}",
            "region": _DEST_REGION.get(g["dest"], g["dest"]),
            "items": int(_f(g["items"])),
            "cubicM3": _f(g["cbm"]),
            "weightKg": _f(g["kg"]),
            "fuelSurcharge": _f(g["fuel"]),
            "costExclGst": _f(g["excl"]),
            "source": "statement",
        })
    return meta, rows


if __name__ == "__main__":
    import sys, json
    meta, rows = parse_statement(sys.argv[1])
    parsed = round(sum(r["costExclGst"] for r in rows), 2)
    stated = meta["statementExclGst"]
    print(json.dumps(meta, default=str, indent=2))
    print(f"consignments parsed: {len(rows)}")
    print(f"sum(costExclGst) = {parsed}   statement ex-GST = {stated}   "
          f"{'✓ MATCH' if stated and abs(parsed - stated) < 0.01 else '✗ MISMATCH'}")
    for r in rows[:3]:
        print(" ", r["consignmentRef"], r["receiver"], r["region"], r["costExclGst"])
