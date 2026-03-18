#!/usr/bin/env python3
"""
Incremental sync of French BOAMP procurement data into the contracts table.

Fetches notices from the BOAMP OpenDataSoft API, filtered by publication date,
and upserts into the contracts table.

Usage:
    python3 source/fr_boamp.py --from 2025-01-01 --to 2025-12-31
    python3 source/fr_boamp.py --from 2026-01-01  # defaults --to to today
"""

import argparse
import json
import requests
import sys
import time
from datetime import date

import psycopg2
import psycopg2.extras

API_URL = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
BATCH_SIZE = 100  # OpenDataSoft max per request

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

# --- Nature mapping ---
NATURE_MAP = {
    'APPEL_OFFRE': 'announced',
    'ATTRIBUTION': 'awarded',
    'RECTIFICATIF': 'modified',
    'ANNULATION': 'cancelled',
    'PRE-INFORMATION': 'prior-notice',
    'INTENTION_CONCLURE': 'announced',
    'EX_ANTE_VOLONTAIRE': 'announced',
    'MODIFICATION': 'modified',
    'PERIODIQUE': 'prior-notice',
    'QUALIFICATION': 'announced',
}

NOTICE_MAP = {
    'APPEL_OFFRE': 'cn-standard',
    'ATTRIBUTION': 'can-standard',
    'RECTIFICATIF': 'can-modif',
    'ANNULATION': 'cn-standard',
    'PRE-INFORMATION': 'pin-only',
    'INTENTION_CONCLURE': 'veat',
    'EX_ANTE_VOLONTAIRE': 'veat',
    'MODIFICATION': 'can-modif',
    'PERIODIQUE': 'pin-only',
}

# --- Type mapping ---
TYPE_MAP = {
    'TRAVAUX': 'works',
    'SERVICES': 'services',
    'FOURNITURES': 'supplies',
}

# --- Famille to concession ---
CONCESSION_FAMILLES = {'DSP'}

COLUMNS = [
    'source', 'source_id', 'lot_number',
    'reference_number', 'source_url',
    'status', 'contract_nature', 'is_concession', 'contract_type_original',
    'procedure_type', 'notice_type',
    'contract_title', 'lot_title', 'contract_duration',
    'contracting_authority', 'authority_id', 'authority_type', 'authority_dir3',
    'place_of_execution', 'nuts_code', 'country',
    'estimated_value', 'estimated_value_currency',
    'base_budget', 'base_budget_currency',
    'awardee', 'awardee_id',
    'award_value', 'award_value_with_tax', 'award_value_currency',
    'num_offers', 'excluded_low_offers',
    'date_published', 'date_updated', 'date_awarded', 'date_contract_start',
    'eu_funded', 'is_aggregated', 'cpv_codes',
]

UPSERT_SQL = """
    INSERT INTO contracts (
        source, source_id, lot_number,
        reference_number, source_url,
        status, contract_nature, is_concession, contract_type_original,
        procedure_type, notice_type,
        contract_title, lot_title, contract_duration,
        contracting_authority, authority_id, authority_type, authority_dir3,
        place_of_execution, nuts_code, country,
        estimated_value, estimated_value_currency,
        base_budget, base_budget_currency,
        awardee, awardee_id,
        award_value, award_value_with_tax, award_value_currency,
        num_offers, excluded_low_offers,
        date_published, date_updated, date_awarded, date_contract_start,
        eu_funded, is_aggregated, cpv_codes, last_synced_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
    )
    ON CONFLICT (source, source_id, lot_number) DO UPDATE SET
        reference_number = EXCLUDED.reference_number,
        source_url = EXCLUDED.source_url,
        status = EXCLUDED.status,
        contract_nature = EXCLUDED.contract_nature,
        is_concession = EXCLUDED.is_concession,
        contract_type_original = EXCLUDED.contract_type_original,
        procedure_type = EXCLUDED.procedure_type,
        notice_type = EXCLUDED.notice_type,
        contract_title = EXCLUDED.contract_title,
        lot_title = EXCLUDED.lot_title,
        contract_duration = EXCLUDED.contract_duration,
        contracting_authority = EXCLUDED.contracting_authority,
        authority_id = EXCLUDED.authority_id,
        authority_type = EXCLUDED.authority_type,
        authority_dir3 = EXCLUDED.authority_dir3,
        place_of_execution = EXCLUDED.place_of_execution,
        nuts_code = EXCLUDED.nuts_code,
        country = EXCLUDED.country,
        estimated_value = EXCLUDED.estimated_value,
        estimated_value_currency = EXCLUDED.estimated_value_currency,
        base_budget = EXCLUDED.base_budget,
        base_budget_currency = EXCLUDED.base_budget_currency,
        awardee = EXCLUDED.awardee,
        awardee_id = EXCLUDED.awardee_id,
        award_value = EXCLUDED.award_value,
        award_value_with_tax = EXCLUDED.award_value_with_tax,
        award_value_currency = EXCLUDED.award_value_currency,
        num_offers = EXCLUDED.num_offers,
        excluded_low_offers = EXCLUDED.excluded_low_offers,
        date_published = EXCLUDED.date_published,
        date_updated = EXCLUDED.date_updated,
        date_awarded = EXCLUDED.date_awarded,
        date_contract_start = EXCLUDED.date_contract_start,
        eu_funded = EXCLUDED.eu_funded,
        is_aggregated = EXCLUDED.is_aggregated,
        cpv_codes = EXCLUDED.cpv_codes,
        last_synced_at = NOW()
"""


def safe_str(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def parse_donnees(donnees_str):
    """Parse the nested donnees JSON string. Returns a dict of extracted fields."""
    result = {
        'cpv_codes': None,
        'nuts_code': None,
        'estimated_value': None,
        'reference_number': None,
        'description': None,
        'duration': None,
        'place_of_execution': None,
    }

    if not donnees_str:
        return result

    try:
        donnees = json.loads(donnees_str)
    except (json.JSONDecodeError, TypeError):
        return result

    # Navigate the nested structure — different for DSP vs regular markets
    # Try multiple paths
    for root_key in [donnees, donnees.get('DSP', {}), donnees.get('MARCHE', {})]:
        initial = root_key.get('initial', root_key)

        desc = initial.get('descriptionMarche', {})
        if not desc:
            desc = initial.get('description', {})

        # Reference number
        ref = desc.get('numeroReference')
        if ref:
            result['reference_number'] = ref

        # CPV codes
        cpv_section = desc.get('CPV', {})
        cpv_codes = []
        if isinstance(cpv_section, dict):
            principal = cpv_section.get('objetPrincipal', {}).get('classPrincipale')
            if principal:
                cpv_codes.append(str(principal))
            complementary = cpv_section.get('objetComplementaire', {})
            if isinstance(complementary, dict):
                c = complementary.get('classPrincipale')
                if c:
                    cpv_codes.append(str(c))
            elif isinstance(complementary, list):
                for comp in complementary:
                    if isinstance(comp, dict):
                        c = comp.get('classPrincipale')
                        if c:
                            cpv_codes.append(str(c))

        # Also check lots for CPV
        lot = desc.get('lot', {})
        if isinstance(lot, dict):
            lot_cpv = lot.get('CPV', {})
            if isinstance(lot_cpv, dict):
                principal = lot_cpv.get('objetPrincipal', {}).get('classPrincipale')
                if principal and str(principal) not in cpv_codes:
                    cpv_codes.append(str(principal))

            # Estimated value from lot
            est = lot.get('estimationValeur', {})
            if isinstance(est, dict):
                val = est.get('valeur')
                if val:
                    result['estimated_value'] = safe_float(val)

            # NUTS from lot
            nuts = lot.get('lieuCodeNUTS', {})
            if isinstance(nuts, dict):
                result['nuts_code'] = nuts.get('codeNUTS')

            # Place from lot
            lieu = lot.get('lieuExecutionLivraison')
            if lieu:
                result['place_of_execution'] = str(lieu)

            # Duration from lot
            duree = lot.get('dureeLot', {})
            if isinstance(duree, dict):
                date_from = duree.get('dateACompterDu', '')
                date_to = duree.get('dateJusquau', '')
                if date_from and date_to:
                    result['duration'] = f"{date_from} to {date_to}"

        if cpv_codes:
            result['cpv_codes'] = cpv_codes

        # NUTS from organisme
        org = root_key.get('organisme', {})
        if isinstance(org, dict) and not result['nuts_code']:
            result['nuts_code'] = org.get('codeNUTS')

        if result['cpv_codes']:
            break  # Found data, stop trying other root keys

    return result


def parse_procedure(procedure_libelle):
    """Map French procedure names to EU codes."""
    if not procedure_libelle:
        return None
    p = procedure_libelle.lower()
    if 'ouverte' in p or 'ouvert' in p:
        return 'open'
    if 'restreint' in p:
        return 'restricted'
    if 'négocié' in p or 'negocie' in p:
        return 'neg-w-call'
    if 'dialogue' in p:
        return 'comp-dial'
    if 'adapté' in p or 'adaptee' in p or 'adapte' in p:
        return 'open'
    return None


def transform_record(rec):
    """Transform a BOAMP API record into a contracts record dict.

    For ATTRIBUTION notices, source_id uses the original announcement's idweb
    (from annonce_lie) so awards update the existing announcement row.
    For all other notices, source_id is the notice's own idweb.
    """
    idweb = rec.get('idweb')
    if not idweb:
        return None

    nature = rec.get('nature') or ''
    famille = rec.get('famille') or ''
    type_marche = rec.get('type_marche') or []
    first_type = type_marche[0] if type_marche else None

    # Parse nested donnees
    donnees = parse_donnees(rec.get('donnees'))

    # Awardee
    titulaire = rec.get('titulaire')
    awardee = None
    if isinstance(titulaire, list) and titulaire:
        awardee = ' | '.join(str(t) for t in titulaire if t)
    elif isinstance(titulaire, str) and titulaire:
        awardee = titulaire

    # Department as place of execution fallback
    dept = rec.get('code_departement')
    place = donnees.get('place_of_execution')
    if not place and dept:
        place = ', '.join(dept) if isinstance(dept, list) else str(dept)

    # For ATTRIBUTION notices, use the original announcement's idweb as source_id
    # so the upsert updates the existing row instead of creating a duplicate
    source_id = idweb
    if nature == 'ATTRIBUTION':
        annonce_lie = rec.get('annonce_lie')
        if isinstance(annonce_lie, list) and annonce_lie:
            source_id = annonce_lie[0]  # Link to original announcement

    return {
        'source': 'FR_BOAMP',
        'source_id': source_id,
        'lot_number': '0',
        'reference_number': donnees.get('reference_number'),
        'source_url': rec.get('url_avis'),
        'status': NATURE_MAP.get(nature, 'announced'),
        'contract_nature': TYPE_MAP.get(first_type) if first_type else None,
        'is_concession': famille in CONCESSION_FAMILLES,
        'contract_type_original': first_type,
        'procedure_type': parse_procedure(rec.get('procedure_libelle')),
        'notice_type': NOTICE_MAP.get(nature),
        'contract_title': safe_str(rec.get('objet')),
        'lot_title': None,
        'contract_duration': donnees.get('duration'),
        'contracting_authority': safe_str(rec.get('nomacheteur')),
        'authority_id': None,
        'authority_type': None,
        'authority_dir3': None,
        'place_of_execution': place,
        'nuts_code': donnees.get('nuts_code'),
        'country': 'FR',
        'estimated_value': donnees.get('estimated_value'),
        'estimated_value_currency': 'EUR',
        'base_budget': None,
        'base_budget_currency': 'EUR',
        'awardee': awardee,
        'awardee_id': None,
        'award_value': None,
        'award_value_with_tax': None,
        'award_value_currency': 'EUR',
        'num_offers': None,
        'excluded_low_offers': None,
        'date_published': rec.get('dateparution'),
        'date_updated': rec.get('dateparution'),
        'date_awarded': rec.get('dateparution') if nature == 'ATTRIBUTION' else None,
        'date_contract_start': None,
        'eu_funded': None,
        'is_aggregated': None,
        'cpv_codes': donnees.get('cpv_codes'),
    }


# Only import announcements and awards — not rectifications or modifications
IMPORT_NATURES = "nature IN ('APPEL_OFFRE', 'ATTRIBUTION', 'ANNULATION', 'PRE-INFORMATION')"


def get_count(date_from, date_to):
    """Get total records matching the date range."""
    params = {
        'where': f"dateparution>='{date_from}' AND dateparution<='{date_to}' AND {IMPORT_NATURES}",
        'limit': 0,
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()['total_count']


def fetch_batch(date_from, date_to, offset, limit=BATCH_SIZE):
    """Fetch a batch of records from the API."""
    params = {
        'where': f"dateparution>='{date_from}' AND dateparution<='{date_to}' AND {IMPORT_NATURES}",
        'order_by': 'dateparution ASC',
        'limit': limit,
        'offset': offset,
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json().get('results', [])


def get_date_chunks(date_from, date_to):
    """Generate (chunk_start, chunk_end) tuples between two dates.

    Uses weekly chunks to stay safely under the 10K offset limit.
    Even the busiest weeks have well under 10K notices.
    """
    from datetime import datetime, timedelta
    start = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.strptime(date_to, '%Y-%m-%d')

    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=6), end)
        chunks.append((current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        current = chunk_end + timedelta(days=1)
    return chunks


def main():
    parser = argparse.ArgumentParser(description='Incremental sync of BOAMP procurement data')
    parser.add_argument('--from', dest='date_from', required=True,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to', dest='date_to', default=str(date.today()),
                        help='End date (YYYY-MM-DD), defaults to today')
    args = parser.parse_args()

    date_from = args.date_from
    date_to = args.date_to

    print(f"Syncing FR_BOAMP data from {date_from} to {date_to}")

    print("Getting total record count...")
    total = get_count(date_from, date_to)
    print(f"Total records: {total:,}")

    if total == 0:
        print("Nothing to sync.")
        return

    # Paginate by week to stay under the 10K offset limit
    chunks = get_date_chunks(date_from, date_to)
    print(f"Processing {len(chunks)} weekly chunks")

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False

    # Register source
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ref_source (code, label, url, country)
            VALUES ('FR_BOAMP', 'Bulletin Officiel des Annonces des Marches Publics', 'https://www.boamp.fr', 'FR')
            ON CONFLICT (code) DO NOTHING
        """)
    conn.commit()

    upserted = 0
    skipped = 0
    start_time = time.time()
    retries = 0
    max_retries = 5

    try:
        for chunk_start, chunk_end in chunks:
            chunk_count = get_count(chunk_start, chunk_end)
            if chunk_count == 0:
                continue

            offset = 0
            while offset < chunk_count:
                try:
                    data = fetch_batch(chunk_start, chunk_end, offset)
                    retries = 0
                except requests.exceptions.RequestException as e:
                    retries += 1
                    if retries > max_retries:
                        print(f"\n  Too many retries, skipping chunk")
                        break
                    print(f"\n  API error: {e}. Retry {retries}/{max_retries} in 5s...")
                    time.sleep(5)
                    continue

                if not data:
                    break

                records = []
                for rec in data:
                    row = transform_record(rec)
                    if row:
                        records.append(tuple(row.get(col) for col in COLUMNS))
                    else:
                        skipped += 1

                if records:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_batch(cur, UPSERT_SQL, records, page_size=100)
                    conn.commit()
                    upserted += len(records)

                offset += len(data)

            elapsed = time.time() - start_time
            rate = upserted / elapsed if elapsed > 0 else 0
            eta = (total - upserted) / rate if rate > 0 else 0
            if chunk_start[8:10] == '01' or chunk_start == chunks[-1][0]:
                print(f"  {chunk_start}: {upserted:,}/{total:,} ({100*upserted/total:.1f}%) "
                      f"- {rate:.0f} rec/s - ETA: {eta/60:.1f} min", flush=True)

    except Exception as e:
        conn.rollback()
        print(f"\nError: {e}")
        raise
    finally:
        conn.close()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Sync completed!")
    print(f"  Records upserted: {upserted:,}")
    print(f"  Records skipped:  {skipped:,}")
    print(f"  Duration:         {total_time/60:.1f} minutes")
    if total_time > 0:
        print(f"  Rate:             {upserted/total_time:.0f} records/second")


if __name__ == '__main__':
    main()
