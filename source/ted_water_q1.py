#!/usr/bin/env python3
"""
Fetch all water-related contract awards from TED and export
statistics by country and water sub-sector.

Usage:
    python3 source/ted_water_q1.py --from 2026-01-01 --to 2026-03-25
    python3 source/ted_water_q1.py --from 2026-01-01  # defaults --to to today
    python3 source/ted_water_q1.py --from 2026-01-01 --csv out.csv
"""

import argparse
import csv
import json
import sys
import time
from collections import defaultdict
from datetime import date
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# ── TED API ─────────────────────────────────────────────────────────────────

TED_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"
PAGE_SIZE = 100
MAX_RETRIES = 5
RETRY_WAIT = 8  # seconds

# Fields to request from the search API
SEARCH_FIELDS = [
    "notice-type",
    "classification-cpv",
    "total-value",
    "total-value-cur",
    "result-value-notice",
    "result-value-cur-notice",
    "estimated-value-lot",
    "estimated-value-cur-lot",
    "buyer-name",
    "buyer-country",
    "winner-name",
    "contract-title",
    "title-lot",
]

# ── ECB rates 24 Mar 2026 ──────────────────────────────────────────────────

ECB_RATES = {
    "EUR": 1.0,
    "PLN": 4.2743,
    "CZK": 24.477,
    "HUF": 389.88,
    "RON": 5.0951,
    "GBP": 0.86541,
    "SEK": 10.8238,
    "DKK": 7.4711,
    "NOK": 11.2305,
    "BGN": 1.9558,
    "CHF": 0.9535,
    "ISK": 147.0,
}

# ── Water sub-sector classification by CPV code ─────────────────────────────

WATER_CATEGORIES = [
    ("wwtp_construction", "WWTP construction", [
        "45252100", "45252120", "45252121", "45252122", "45252123",
        "45252124", "45252125", "45252127", "45252128", "45252130",
        "45232420", "45232421", "45232440",
        "90481000",
    ]),
    ("sewage_collection", "Sewage collection & services", [
        "90400000", "90410000", "90420000", "90430000", "90440000",
        "90450000", "90460000", "90470000", "90480000", "90481000",
        "90482000",
        "45232400", "45232410", "45232411",
        "45232130", "45232430",
        "45232450", "45232451", "45232452", "45232453", "45232454",
        "45231300", "45231110",
    ]),
    ("irrigation", "Irrigation works", [
        "45232120", "45232121", "45232122", "45232123",
    ]),
    ("wtp_construction", "WTP construction", [
        "45252126", "45252000", "42912350",
    ]),
    ("water_distribution", "Water distribution & services", [
        "65111000", "65110000", "65100000", "65130000",
        "41110000", "41100000",
        "45231100", "45231112",
        "45232100", "45232150", "45232151", "45232152",
    ]),
]

# Build lookup: longest-prefix-first for correct matching
_CPV_LOOKUP = []
for key, label, prefixes in WATER_CATEGORIES:
    for pfx in prefixes:
        _CPV_LOOKUP.append((pfx, key, label))
_CPV_LOOKUP.sort(key=lambda x: -len(x[0]))  # longest prefix first


def classify_cpv(cpv_code: str):
    """Return (category_key, category_label) for a CPV code, or (None, None)."""
    for pfx, key, label in _CPV_LOOKUP:
        if cpv_code.startswith(pfx):
            return key, label
    return None, None


# ── False-positive filters ──────────────────────────────────────────────────
# Primary CPV prefixes that indicate non-water contracts even if they appear
# alongside water CPV codes.

FALSE_POSITIVE_PRIMARY = [
    "45232140",  # district-heating mains
    "45232141",  # heating works
    "45251250",  # geothermal
    "09323000",  # district heating
    "50721000",  # heating-installation maintenance
    "45233",     # road surface works
    "45234",     # railway works
    "45221",     # bridges / tunnels
    "34632",     # railway signalling
    "45213",     # buildings (general)
    "45215",     # health/hospital buildings
    "45223",     # structural steel
    "45112",     # excavating (general site)
    "45212",     # leisure buildings
    "44622",     # heat-recovery systems
    "71321",     # engineering design (non-water-specific)
]


def is_false_positive_primary(cpv: str) -> bool:
    return any(cpv.startswith(fp) for fp in FALSE_POSITIVE_PRIMARY)


# ── TED API helpers ─────────────────────────────────────────────────────────

def ted_search(query, page=1):
    """POST to TED search API with retry."""
    payload = {
        "query": query,
        "page": page,
        "limit": PAGE_SIZE,
        "scope": 1,
        "fields": SEARCH_FIELDS,
    }
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(MAX_RETRIES):
        req = Request(TED_SEARCH_URL, data=data,
                      headers={"Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                wait = RETRY_WAIT * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"TED API failed after {MAX_RETRIES} retries")


# ── Search queries ──────────────────────────────────────────────────────────
# We run separate queries for distinct CPV families to avoid overly broad
# matches while keeping the query manageable.

def build_queries(date_from, date_to):
    """Build TED search queries for the given date range (YYYYMMDD format)."""
    return [
        # Water distribution / supply services
        f"(PC=6511* OR PC=6510* OR PC=6513* OR PC=4111* OR PC=4110*)"
        f" AND PD>={date_from} AND PD<={date_to}",
        # Sewage services
        f"(PC=9040* OR PC=9041* OR PC=9042* OR PC=9043*"
        f" OR PC=9044* OR PC=9045* OR PC=9046* OR PC=9047* OR PC=9048*)"
        f" AND PD>={date_from} AND PD<={date_to}",
        # Water/sewage pipeline construction
        f"(PC=45231300 OR PC=45231110 OR PC=4523213*"
        f" OR PC=4523240* OR PC=4523241* OR PC=4523242*"
        f" OR PC=4523243* OR PC=4523244* OR PC=4523245*)"
        f" AND PD>={date_from} AND PD<={date_to}",
        # Water/wastewater treatment plant construction
        f"(PC=45252* OR PC=42912350)"
        f" AND PD>={date_from} AND PD<={date_to}",
        # Irrigation
        f"(PC=4523212*)"
        f" AND PD>={date_from} AND PD<={date_to}",
    ]


def fetch_all_notices(queries):
    """Fetch all matching notices across all queries, deduplicating by ID."""
    seen = set()
    all_notices = []

    for qi, query in enumerate(queries):
        print(f"  Query {qi+1}/{len(QUERIES)}...", file=sys.stderr)
        page = 1
        while True:
            result = ted_search(query, page)
            items = result.get("notices", [])
            total = result.get("totalNoticeCount", 0)

            if page == 1:
                print(f"    {total} matches", file=sys.stderr)

            for n in items:
                pub_num = n.get("publication-number", "")
                if pub_num and pub_num not in seen:
                    seen.add(pub_num)
                    all_notices.append(n)

            if not items or len(items) < PAGE_SIZE:
                break
            page += 1
            time.sleep(0.5)

    print(f"  Total unique notices: {len(all_notices)}", file=sys.stderr)
    return all_notices


# ── Extract fields from notice ──────────────────────────────────────────────

def _get_text(obj):
    """Extract text from a TED multilingual field {lang: [values]}."""
    if not obj or not isinstance(obj, dict):
        return ""
    for lang in ("eng", "ENG", "en", "fra", "FRA", "fr", "deu", "DEU", "de"):
        if lang in obj:
            vals = obj[lang]
            if isinstance(vals, list):
                return "; ".join(str(v) for v in vals)
            return str(vals)
    # Fallback: first language
    for vals in obj.values():
        if isinstance(vals, list):
            return "; ".join(str(v) for v in vals)
        return str(vals)
    return ""


def parse_notice(n):
    """Parse a search-result notice into a flat dict, or None if not useful."""
    pub_num = n.get("publication-number", "")

    # CPV codes
    cpv_codes = n.get("classification-cpv", [])
    if not cpv_codes:
        return None
    # Deduplicate while preserving order
    seen_cpv = set()
    unique_cpvs = []
    for c in cpv_codes:
        c = str(c)
        if c not in seen_cpv:
            seen_cpv.add(c)
            unique_cpvs.append(c)
    cpv_codes = unique_cpvs

    # Value: prefer total-value, then result-value-notice, then estimated-value-lot
    amount = n.get("total-value")
    currency_list = n.get("total-value-cur", [])
    currency = currency_list[0] if currency_list else "EUR"

    if not amount:
        rv = n.get("result-value-notice")
        if rv:
            amount = rv
            rc = n.get("result-value-cur-notice", "EUR")
            currency = rc if isinstance(rc, str) else "EUR"

    if not amount:
        ev = n.get("estimated-value-lot")
        if isinstance(ev, list) and ev:
            # Sum lot values
            try:
                amount = sum(float(v) for v in ev if v)
            except (ValueError, TypeError):
                amount = None
            ec = n.get("estimated-value-cur-lot", [])
            currency = ec[0] if ec else "EUR"
        elif ev:
            amount = ev
            ec = n.get("estimated-value-cur-lot", "EUR")
            currency = ec if isinstance(ec, str) else "EUR"

    if not amount:
        return None

    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return None

    if amount <= 0:
        return None

    # Country
    countries = n.get("buyer-country", [])
    country = countries[0] if countries else ""

    # Buyer name
    buyer = _get_text(n.get("buyer-name"))

    # Winner name
    winners = _get_text(n.get("winner-name"))

    # Title
    title = _get_text(n.get("contract-title")) or _get_text(n.get("title-lot"))

    return {
        "notice_id": pub_num,
        "cpv_codes": cpv_codes,
        "amount": amount,
        "currency": currency,
        "country": country,
        "buyer": buyer,
        "winners": winners,
        "title": title,
    }


def to_eur(amount, currency):
    """Convert an amount to EUR using ECB rates."""
    if currency == "EUR":
        return amount
    rate = ECB_RATES.get(currency)
    if rate is None:
        return None
    return round(amount / rate, 2)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TED water contracts")
    parser.add_argument("--from", dest="date_from", required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to", default=str(date.today()),
                        help="End date (YYYY-MM-DD, default: today)")
    parser.add_argument("--csv", default="docs/ted_water_q1_2026.csv",
                        help="Output CSV path (default: docs/ted_water_q1_2026.csv)")
    args = parser.parse_args()

    date_from = args.date_from.replace("-", "")
    date_to = args.date_to.replace("-", "")

    print(f"Fetching TED water notices from {args.date_from} to {args.date_to}...",
          file=sys.stderr)
    queries = build_queries(date_from, date_to)
    raw_notices = fetch_all_notices(queries)

    records = []
    skipped_fp = 0
    skipped_no_cat = 0
    skipped_no_value = 0
    skipped_currency = 0

    for i, n in enumerate(raw_notices):
        parsed = parse_notice(n)
        if parsed is None:
            skipped_no_value += 1
            continue

        cpv_codes = parsed["cpv_codes"]
        primary_cpv = cpv_codes[0]

        # Skip known false positives by primary CPV
        if is_false_positive_primary(primary_cpv):
            skipped_fp += 1
            continue

        # Classify: try primary CPV first, then secondary
        cat_key, cat_label = classify_cpv(primary_cpv)
        if cat_key is None:
            for cpv in cpv_codes[1:]:
                if not is_false_positive_primary(cpv):
                    cat_key, cat_label = classify_cpv(cpv)
                    if cat_key:
                        break

        if cat_key is None:
            skipped_no_cat += 1
            continue

        eur_value = to_eur(parsed["amount"], parsed["currency"])
        if eur_value is None:
            print(f"  Unknown currency {parsed['currency']} for "
                  f"{parsed['notice_id']}", file=sys.stderr)
            skipped_currency += 1
            continue

        records.append({
            "notice_id": parsed["notice_id"],
            "country": parsed["country"],
            "buyer": parsed["buyer"],
            "awardees": parsed["winners"],
            "title": parsed["title"],
            "primary_cpv": primary_cpv,
            "all_cpvs": ";".join(cpv_codes[:5]),
            "category": cat_key,
            "category_label": cat_label,
            "original_value": parsed["amount"],
            "original_currency": parsed["currency"],
            "value_eur": eur_value,
        })

    print(f"\n  Classified: {len(records)} contracts", file=sys.stderr)
    print(f"  Skipped (false positive):  {skipped_fp}", file=sys.stderr)
    print(f"  Skipped (no water match):  {skipped_no_cat}", file=sys.stderr)
    print(f"  Skipped (no value):        {skipped_no_value}", file=sys.stderr)
    print(f"  Skipped (unknown currency):{skipped_currency}", file=sys.stderr)

    # ── Write CSV ───────────────────────────────────────────────────────────
    records.sort(key=lambda r: r["value_eur"], reverse=True)

    fieldnames = [
        "notice_id", "country", "buyer", "awardees", "title",
        "primary_cpv", "all_cpvs", "category", "category_label",
        "original_value", "original_currency", "value_eur",
    ]
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"\n  CSV written to {args.csv}", file=sys.stderr)

    # ── Print statistics ────────────────────────────────────────────────────
    total_eur = sum(r["value_eur"] for r in records)
    print(f"\n{'='*80}")
    print(f"WATER CONTRACT AWARDS ON TED — {args.date_from} to {args.date_to}")
    print(f"{'='*80}")
    print(f"\nTotal contracts: {len(records)}")
    print(f"Total value:     EUR {total_eur/1e9:.2f} billion\n")

    # By category
    cat_stats = defaultdict(lambda: {"count": 0, "value": 0.0})
    for r in records:
        cat_stats[r["category_label"]]["count"] += 1
        cat_stats[r["category_label"]]["value"] += r["value_eur"]

    print(f"{'Sub-sector':<35} {'Contracts':>10} {'Value (EUR M)':>15} {'Share':>8}")
    print("-" * 72)
    for _, label, _ in WATER_CATEGORIES:
        s = cat_stats.get(label, {"count": 0, "value": 0.0})
        share = s["value"] / total_eur * 100 if total_eur > 0 else 0
        print(f"{label:<35} {s['count']:>10} {s['value']/1e6:>15,.1f} {share:>7.1f}%")
    print("-" * 72)
    print(f"{'TOTAL':<35} {len(records):>10} {total_eur/1e6:>15,.1f} {'100.0%':>8}")

    # By country
    country_stats = defaultdict(lambda: {"count": 0, "value": 0.0})
    for r in records:
        country_stats[r["country"]]["count"] += 1
        country_stats[r["country"]]["value"] += r["value_eur"]

    print(f"\n{'Country':<10} {'Contracts':>10} {'Value (EUR M)':>15} {'Share':>8}")
    print("-" * 47)
    for country, s in sorted(country_stats.items(), key=lambda x: -x[1]["value"]):
        share = s["value"] / total_eur * 100 if total_eur > 0 else 0
        print(f"{country:<10} {s['count']:>10} {s['value']/1e6:>15,.1f} {share:>7.1f}%")
    print("-" * 47)
    print(f"{'TOTAL':<10} {len(records):>10} {total_eur/1e6:>15,.1f} {'100.0%':>8}")

    # Country × category pivot
    print(f"\n{'='*80}")
    print("BREAKDOWN: Country × Sub-sector (EUR millions)")
    print(f"{'='*80}")
    cat_labels = [label for _, label, _ in WATER_CATEGORIES]
    short = ["WWTP", "Sewage", "Irrig.", "WTP", "Water dist."]
    header = f"{'Country':<10}" + "".join(f"{s:>14}" for s in short) + f"{'TOTAL':>14}"
    print(header)
    print("-" * len(header))

    pivot = defaultdict(lambda: defaultdict(float))
    for r in records:
        pivot[r["country"]][r["category_label"]] += r["value_eur"]

    for country, cats in sorted(pivot.items(), key=lambda x: -sum(x[1].values())):
        row_total = sum(cats.values())
        row = f"{country:<10}"
        for label in cat_labels:
            v = cats.get(label, 0)
            row += f"{v/1e6:>14,.1f}" if v > 0 else f"{'—':>14}"
        row += f"{row_total/1e6:>14,.1f}"
        print(row)

    print()


if __name__ == "__main__":
    main()
