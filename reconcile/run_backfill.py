"""
Freight reconciliation backfill / sync.

Pulls Mainfreight + Daily Freight weekly statements from the accounts@ mailbox
(Microsoft Graph, app-only), parses each consignment, joins to the Cin7 sales
order by trackingCode to get what we charged (freightTotal, ex-GST), computes
per-consignment margin, and POSTs the rows to the NED POS ingest endpoint.

Usage (from freight app root, venv active, .env loaded):
    python -m reconcile.run_backfill --since 2026-09-28 --dry-run
    python -m reconcile.run_backfill --since 2026-09-28           # live POST

Env: GRAPH_TENANT_ID/CLIENT_ID/CLIENT_SECRET/MAILBOX, CIN7_USERNAME/CIN7_API_KEY,
     AGENTS_API_KEY, POS_BASE_URL.
"""
import os, sys, base64, argparse, tempfile, re, time
from base64 import b64encode
import httpx

from reconcile.parse_statement import parse_statement

GRAPH = "https://graph.microsoft.com/v1.0"
# Statement streams: match by subject (the account code appears in every subject).
ACCOUNTS = ["NEDCOLCHC", "NEDCOLAKL", "NEDCOLDF"]


def _load_dotenv(path=".env"):
    """Load KEY=VALUE lines from .env into os.environ (no shell sourcing)."""
    try:
        for line in open(path):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass


_load_dotenv()


def _env(k):
    v = os.environ.get(k, "")
    if not v:
        sys.exit(f"missing env {k}")
    return v


def graph_token():
    tid, cid, sec = _env("GRAPH_TENANT_ID"), _env("GRAPH_CLIENT_ID"), _env("GRAPH_CLIENT_SECRET")
    r = httpx.post(f"https://login.microsoftonline.com/{tid}/oauth2/v2.0/token",
                   data={"grant_type": "client_credentials", "client_id": cid,
                         "client_secret": sec, "scope": "https://graph.microsoft.com/.default"},
                   timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def list_statement_messages(token, mailbox, account, since):
    """Message ids for an account's statement emails received since `since` (ISO date)."""
    H = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
    r = httpx.get(f"{GRAPH}/users/{mailbox}/messages",
                  params={"$search": f'"subject:{account}"',
                          "$select": "id,subject,receivedDateTime,hasAttachments", "$top": "50"},
                  headers=H, timeout=40)
    r.raise_for_status()
    out = []
    for m in r.json().get("value", []):
        if m.get("hasAttachments") and m.get("receivedDateTime", "") >= since:
            out.append(m)
    return out


def download_pdf_attachments(token, mailbox, message_id):
    """Yield (name, local_path) for each PDF attachment on a message."""
    H = {"Authorization": f"Bearer {token}"}
    r = httpx.get(f"{GRAPH}/users/{mailbox}/messages/{message_id}/attachments",
                  headers=H, timeout=60)
    r.raise_for_status()
    for a in r.json().get("value", []):
        name = a.get("name", "")
        if (a.get("contentType") == "application/pdf" or name.lower().endswith(".pdf")) and a.get("contentBytes"):
            fd, path = tempfile.mkstemp(suffix=".pdf")
            with os.fdopen(fd, "wb") as f:
                f.write(base64.b64decode(a["contentBytes"]))
            yield name, path


def cin7_auth():
    return {"Authorization": "Basic " + b64encode(
        f"{_env('CIN7_USERNAME')}:{_env('CIN7_API_KEY')}".encode()).decode()}


def cin7_join(ref, _cache={}):
    """Return {freightCharged, cin7Reference, customerType, matched} for a tracking code.
    Retries on 429/5xx (Cin7 rate-limits) so a throttle never masquerades as a no-match."""
    if ref in _cache:
        return _cache[ref]
    data, ok = [], False
    for attempt in range(5):
        try:
            r = httpx.get("https://api.cin7.com/api/v1/SalesOrders",
                          params={"where": f"trackingCode='{ref}'",
                                  "fields": "reference,company,freightTotal,branchId,memberId", "rows": 1},
                          headers=cin7_auth(), timeout=30)
            if r.status_code == 200:
                data, ok = r.json(), True
                break
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            break  # other 4xx — permanent
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    if not ok:
        return {"freightCharged": None, "cin7Reference": None, "customerType": None,
                "matched": False, "lookupFailed": True}  # not cached — retry next run
    if data:
        o = data[0]
        company = (o.get("company") or "").strip()
        res = {"freightCharged": o.get("freightTotal"), "cin7Reference": o.get("reference"),
               "customerType": "trade" if company else "retail", "matched": True}
    else:
        res = {"freightCharged": None, "cin7Reference": None, "customerType": None, "matched": False}
    _cache[ref] = res
    return res


def post_to_pos(rows):
    url = _env("POS_BASE_URL").rstrip("/") + "/api/freight/ingest"
    r = httpx.post(url, json={"consignments": rows},
                   headers={"X-Agents-Key": _env("AGENTS_API_KEY"), "Content-Type": "application/json"},
                   timeout=120)
    r.raise_for_status()
    return r.json()


def run(since="2026-09-28", dry_run=False):
    """Pull → parse → Cin7-join → (POST to POS). Returns a summary dict for
    logging / automation-health. Blocking (httpx + paced Cin7); call via a thread
    from async schedulers."""
    token = graph_token()
    mailbox = _env("GRAPH_MAILBOX")
    all_rows, seen_weeks = [], []

    for account in ACCOUNTS:
        msgs = list_statement_messages(token, mailbox, account, since)
        for m in msgs:
            for name, path in download_pdf_attachments(token, mailbox, m["id"]):
                try:
                    meta, rows = parse_statement(path)
                finally:
                    os.unlink(path)
                if not rows:
                    continue
                parsed_sum = round(sum(r["costExclGst"] for r in rows), 2)
                ok = meta.get("statementExclGst") and abs(parsed_sum - meta["statementExclGst"]) < 0.02
                seen_weeks.append(f"{meta['account']} wk{meta['week']} ({len(rows)} cons, ${parsed_sum} {'✓' if ok else '⚠'})")
                all_rows.extend(rows)

    # Join to Cin7 by tracking code (paced to stay under rate limits)
    for r in all_rows:
        j = cin7_join(r["consignmentRef"])
        r.update(j)
        time.sleep(0.2)
        if j["matched"] and j["freightCharged"] is not None:
            r["marginAmount"] = round(j["freightCharged"] - r["costExclGst"], 2)

    matched = [r for r in all_rows if r.get("matched") and r.get("freightCharged") is not None]
    cost = round(sum(r["costExclGst"] for r in all_rows), 2)
    charged = round(sum(r["freightCharged"] for r in matched), 2)
    mcost = round(sum(r["costExclGst"] for r in matched), 2)
    absorbed = [r for r in all_rows if r.get("matched") and r.get("costExclGst", 0) > 0
                and not r.get("freightCharged")]

    posted = 0
    if not dry_run and all_rows:
        for i in range(0, len(all_rows), 200):
            posted += post_to_pos(all_rows[i:i + 200]).get("upserted", 0)

    worst = sorted(matched, key=lambda x: (x.get("marginAmount") or 0))[:8]
    return {
        "statements": seen_weeks,
        "consignments": len(all_rows),
        "matched": len(matched),
        "unmatched": len(all_rows) - len(matched),
        "cost": cost,
        "charged": charged,
        "matchedMargin": round(charged - mcost, 2),
        "recoveryPct": round(charged / mcost * 100, 1) if mcost else None,
        "absorbedCount": len(absorbed),
        "absorbedCost": round(sum(r["costExclGst"] for r in absorbed), 2),
        "posted": posted,
        "worst": [{"ref": r["consignmentRef"], "receiver": r["receiver"], "region": r["region"],
                   "cost": r["costExclGst"], "charged": r["freightCharged"], "margin": r.get("marginAmount")}
                  for r in worst],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-09-28", help="only statements received on/after this ISO date")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    s = run(since=args.since, dry_run=args.dry_run)
    print("STATEMENTS:")
    for w in s["statements"]:
        print("  ", w)
    print(f"\nconsignments: {s['consignments']} | matched: {s['matched']} | unmatched: {s['unmatched']}")
    print(f"carrier cost (ex-GST):   ${s['cost']:,.2f}")
    print(f"charged (matched):       ${s['charged']:,.2f}")
    print(f"margin (matched):        ${s['matchedMargin']:,.2f}  (recover {s['recoveryPct']}%)")
    print(f"ABSORBED:                {s['absorbedCount']} = ${s['absorbedCost']:,.2f}")
    print("\nworst by margin:")
    for r in s["worst"]:
        print(f"  {r['ref']} {str(r['receiver'])[:22]:22s} {str(r['region'])[:12]:12s} "
              f"cost ${r['cost']:7.2f} charged ${r['charged']:7.2f} margin ${r['margin']:8.2f}")
    print(f"\n{'[dry-run] not posted' if args.dry_run else 'POSTed: ' + str(s['posted']) + ' upserted'}")


if __name__ == "__main__":
    main()
