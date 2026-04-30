#!/usr/bin/env python3
"""
Enrich Spanish assets_operators with concession data from the contracts table.

Finds awarded/formalized water concessions, matches them to localities,
detects which asset types are covered from the contract title, and
upserts into assets_operators.

Usage:
    python3 source/es_concessions_enrich.py          # dry run
    python3 source/es_concessions_enrich.py --apply  # apply changes
"""

import argparse
import re
import sys
import unicodedata
from datetime import date

from dateutil.relativedelta import relativedelta
import psycopg2
import psycopg2.extras

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

# Patterns to extract municipality name from contracting_authority
AUTHORITY_PATTERNS = [
    # "Pleno del Ayuntamiento de X", "Alcaldía del Ayuntamiento de X", etc.
    r'(?:Pleno|Alcald[ií]a|Junta de Gobierno|Consejo de Administraci[oó]n)\s+del?\s+(?:Ayuntamiento|Concello|Ajuntament)\s+d(?:e\s+|el\s+|\')',
    # "PLENO DEL AYUNTAMIENTO DE X" (uppercase)
    r'PLENO\s+DEL\s+AYUNTAMIENTO\s+DE\s+',
    # "Ajuntament de X" / "Ajuntament d'X" (Catalan)
    r'Ajuntament\s+d(?:e\s+|e\s+l[\'a]\s+|\')',
    # "Ayuntamiento de X" standalone
    r'(?:Ayuntamiento|Concello|Ajuntament)\s+d(?:e\s+|el\s+|\')',
]

# Detect asset types from contract title (Spanish + Catalan)
def detect_asset_types(title):
    """Return list of asset_type codes based on keywords in the title."""
    t = title.lower()
    types = []
    is_ciclo_integral = 'ciclo integral' in t or 'cicle integral' in t
    if (is_ciclo_integral or 'abastecimiento' in t or 'agua potable' in t
            or 'suministro de agua' in t
            or 'abastament' in t or 'aigua potable' in t):
        types.append('water_network')
    if (is_ciclo_integral or 'alcantarillado' in t or 'saneamiento' in t
            or 'clavegueram' in t or 'sanejament' in t):
        types.append('sewer_network')
    if 'depuraci' in t or 'edar' in t:
        types.append('wwtp')
    return types


def parse_duration_months(duration_str):
    """Parse duration string into months. Handles both numeric (ES_PLACE)
    and Catalan format '20 anys 0 mesos 0 dies' (ES_GENCAT)."""
    if not duration_str:
        return None
    # Numeric (ES_PLACE): just months
    try:
        return int(float(duration_str))
    except (ValueError, TypeError):
        pass
    # Catalan format: "20 anys 0 mesos 0 dies"
    m = re.match(r'(\d+)\s*anys?\s+(\d+)\s*mes(?:os|es)?', str(duration_str))
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))
    return None


def extract_municipality(authority):
    """Try to extract municipality name from contracting authority string."""
    authority = normalize_apostrophes(authority)
    for pattern in AUTHORITY_PATTERNS:
        m = re.search(pattern, authority, re.IGNORECASE)
        if m:
            return authority[m.end():].strip()
    return None


def strip_accents(s):
    """Remove diacritics from a string."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def build_locality_lookup(cur):
    """Build multiple lookup dicts for fuzzy matching."""
    cur.execute("SELECT id, name FROM localities WHERE country = 'ES'")
    rows = cur.fetchall()

    exact = {}          # lower(name) -> id
    no_accents = {}     # strip_accents(lower(name)) -> id
    no_article = {}     # "Burgo de Ebro, El" -> "el burgo de ebro" -> id
    contains = {}       # for partial matching

    for row in rows:
        name = row['name']
        lower_name = normalize_apostrophes(name.lower())
        exact[lower_name] = row['id']
        no_accents[strip_accents(lower_name)] = row['id']
        contains[lower_name] = row['id']

        # Handle "Name, El/La/Los/Las/L'" -> "El/La/... Name"
        m = re.match(r'^(.+),\s*(el|la|los|las|l\'|es|sa|ses)$', lower_name, re.IGNORECASE)
        if m:
            article = m.group(2)
            base = m.group(1)
            # "l'" joins directly without space, others get a space
            if article.endswith("'"):
                flipped = f"{article}{base}"
            else:
                flipped = f"{article} {base}"
            exact[flipped] = row['id']
            no_accents[strip_accents(flipped)] = row['id']

    return exact, no_accents, contains


def normalize_apostrophes(s):
    """Normalize curly quotes to ASCII apostrophe."""
    return s.replace('\u2019', "'").replace('\u2018', "'").replace('\u00b4', "'")


def match_locality(municipality, exact, no_accents, contains):
    """Try progressively fuzzier matching strategies."""
    key = normalize_apostrophes(municipality.lower())

    # 1. Exact match
    if key in exact:
        return exact[key]

    # 2. Accent-insensitive
    key_na = strip_accents(key)
    if key_na in no_accents:
        return no_accents[key_na]

    # 3. Partial: municipality name is a prefix of a locality name
    # e.g., "Santa María de Guía" matches "Santa María de Guía de Gran Canaria"
    for name, lid in contains.items():
        if name.startswith(key) or strip_accents(name).startswith(key_na):
            return lid

    # 4. Strip leading article "l'" -> base name
    # e.g., "l'Ampolla" -> "Ampolla" matches "Ampolla, L'"
    m = re.match(r"^(?:l'|el\s+|la\s+|les\s+|los\s+|las\s+)", key, re.IGNORECASE)
    if m:
        base = key[m.end():]
        if base in exact:
            return exact[base]
        base_na = strip_accents(base)
        if base_na in no_accents:
            return no_accents[base_na]

    # 5. Strip leading "Vila de/d'" -> base name
    # e.g., "Vila d'Agullent" -> "Agullent"
    m = re.match(r"^(?:vila|ville)\s+d[e']?\s*", key, re.IGNORECASE)
    if m:
        base = key[m.end():]
        if base in exact:
            return exact[base]
        base_na = strip_accents(base)
        if base_na in no_accents:
            return no_accents[base_na]

    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='Apply changes (default is dry run)')
    args = parser.parse_args()

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch Spanish water concessions
    cur.execute("""
        SELECT c.source, c.source_id, c.lot_number, c.contract_title,
               c.contracting_authority, c.awardee, c.date_awarded,
               c.date_contract_start, c.contract_duration, c.source_url,
               c.award_value, c.status
        FROM contracts c
        JOIN contracts_tags t USING (source, source_id, lot_number)
        WHERE c.country = 'ES'
          AND c.is_concession = TRUE
          AND c.contract_nature = 'services'
          AND t.tag = 'water'
          AND (
            lower(c.contract_title) LIKE '%%abastecimiento%%agua%%'
            OR lower(c.contract_title) LIKE '%%agua potable%%'
            OR lower(c.contract_title) LIKE '%%alcantarillado%%'
            OR lower(c.contract_title) LIKE '%%depuraci%%'
            OR lower(c.contract_title) LIKE '%%saneamiento%%'
            OR lower(c.contract_title) LIKE '%%ciclo integral%%'
            OR lower(c.contract_title) LIKE '%%cicle integral%%'
            OR lower(c.contract_title) LIKE '%%abastament%%aigua%%'
            OR lower(c.contract_title) LIKE '%%aigua potable%%'
            OR lower(c.contract_title) LIKE '%%clavegueram%%'
            OR lower(c.contract_title) LIKE '%%sanejament%%'
          )
          AND status IN ('awarded', 'formalized')
        ORDER BY c.date_awarded DESC NULLS LAST
    """)
    concessions = cur.fetchall()
    print(f"Found {len(concessions)} concessions to process")

    exact, no_accents, contains = build_locality_lookup(cur)

    matched = 0
    unmatched = 0
    operators_inserted = 0
    operators_updated = 0

    for c in concessions:
        municipality = extract_municipality(c['contracting_authority'])
        if not municipality:
            if not args.apply:
                print(f"  SKIP (no municipality): {c['contracting_authority']}")
            unmatched += 1
            continue

        locality_id = match_locality(municipality, exact, no_accents, contains)
        if not locality_id:
            if not args.apply:
                print(f"  SKIP (no locality match): '{municipality}' from '{c['contracting_authority']}'")
            unmatched += 1
            continue

        asset_types = detect_asset_types(c['contract_title'])
        if not asset_types:
            if not args.apply:
                print(f"  SKIP (no asset type): {c['contract_title'][:80]}")
            unmatched += 1
            continue

        matched += 1

        # Compute start_date and end_date
        start_date = c['date_contract_start'] or c['date_awarded']
        end_date = None
        duration_months = parse_duration_months(c['contract_duration'])
        if start_date and duration_months:
            try:
                if isinstance(start_date, str):
                    start_dt = date.fromisoformat(start_date)
                else:
                    start_dt = start_date
                end_date = start_dt + relativedelta(months=duration_months)
            except (ValueError, TypeError):
                pass

        for asset_type in asset_types:
            # Find the asset
            cur.execute("""
                SELECT id FROM assets
                WHERE locality_id = %s AND asset_type = %s
            """, (locality_id, asset_type))
            asset_row = cur.fetchone()

            if not asset_row:
                if not args.apply:
                    print(f"  SKIP (no asset): locality={locality_id}, type={asset_type}")
                continue

            asset_id = asset_row['id']

            # Check if an operator record from this tender already exists
            cur.execute("""
                SELECT id FROM assets_operators
                WHERE asset_id = %s AND tender_link = %s
            """, (asset_id, c['source_url']))
            existing = cur.fetchone()

            if not args.apply:
                action = "UPDATE" if existing else "INSERT"
                end_str = f" -> {end_date}" if end_date else ""
                print(f"  {action}: {municipality} / {asset_type} -> {c['awardee']} ({start_date}{end_str})")
            else:
                if existing:
                    cur.execute("""
                        UPDATE assets_operators
                        SET operator = %s,
                            management_type = 'private',
                            contract_type = 'Concesión',
                            start_date = %s,
                            end_date = %s,
                            notes = %s
                        WHERE id = %s
                    """, (
                        c['awardee'],
                        start_date,
                        end_date,
                        f"Duration: {c['contract_duration']} months; Award value: {c['award_value']}; Status: {c['status']}",
                        existing['id'],
                    ))
                    operators_updated += 1
                else:
                    cur.execute("""
                        INSERT INTO assets_operators
                            (asset_id, operator, management_type, contract_type,
                             start_date, end_date, tender_link, notes)
                        VALUES (%s, %s, 'private', 'Concesión', %s, %s, %s, %s)
                        ON CONFLICT (asset_id) DO UPDATE SET
                            operator = EXCLUDED.operator,
                            management_type = EXCLUDED.management_type,
                            contract_type = EXCLUDED.contract_type,
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            tender_link = EXCLUDED.tender_link,
                            notes = EXCLUDED.notes
                    """, (
                        asset_id,
                        c['awardee'],
                        start_date,
                        end_date,
                        c['source_url'],
                        f"Duration: {c['contract_duration']} months; Award value: {c['award_value']}; Status: {c['status']}",
                    ))
                    operators_inserted += 1

    if args.apply:
        conn.commit()
        print(f"\nApplied: {operators_inserted} inserted, {operators_updated} updated")
    else:
        print(f"\nDry run summary:")

    print(f"  Concessions matched to locality: {matched}")
    print(f"  Concessions unmatched: {unmatched}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
