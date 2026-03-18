#!/usr/bin/env python3
"""
Sync of French DECP procurement data into the contracts table.

Downloads the consolidated Parquet file from data.gouv.fr and upserts
into the contracts table. Only imports current records (donneesActuelles=True).

Usage:
    python3 source/decp.py
    python3 source/decp.py --from 2024-01-01 --to 2024-12-31
"""

import argparse
import os
import sys
import time
from datetime import date
from urllib.request import urlretrieve

import psycopg2
import psycopg2.extras

DATASET_API = "https://www.data.gouv.fr/api/1/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/"

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

# --- Contract type mapping ---
TYPE_MAP = {
    'Travaux': 'works',
    'Services': 'services',
    'Fournitures': 'supplies',
}

# --- Procedure mapping ---
PROCEDURE_MAP = {
    'Appel d\'offres ouvert': 'open',
    'Appel d offres ouvert': 'open',
    'Appel d\u2019offres ouvert': 'open',
    'Appel d\'offres restreint': 'restricted',
    'Appel d offres restreint': 'restricted',
    'Appel d\u2019offres restreint': 'restricted',
    'Procédure adaptée': 'open',
    'Procédure avec négociation': 'neg-w-call',
    'Procédure concurrentielle avec négociation': 'neg-w-call',
    'Procedure concurrentielle avec negociation': 'neg-w-call',
    'Procédure négociée avec mise en concurrence préalable': 'neg-w-call',
    'Marché passé sans publicité ni mise en concurrence préalable': 'neg-wo-call',
    'Marché négocié sans publicité ni mise en concurrence préalable': 'neg-wo-call',
    'Marché public négocié sans publicité ni mise en concurrence préalable': 'neg-wo-call',
    'Dialogue compétitif': 'comp-dial',
    'Procédure négociée restreinte': 'restricted',
}

# --- Authority type mapping ---
AUTHORITY_MAP = {
    'État': 'cga',
    'Région': 'ra',
    'Commune': 'la',
    'Département': 'la',
    'Département outre-mer': 'la',
    'Groupement de communes': 'la',
    'Syndicat mixte': 'la',
    'EPIC': 'body-pl',
    'Établissement hospitalier': 'body-pl',
}

# --- Nature to concession flag ---
CONCESSION_NATURES = {
    'Concession de service public',
    'Délégation de service public',
}

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


def get_parquet_url():
    """Find the decp.parquet URL from the data.gouv.fr API."""
    import json
    from urllib.request import urlopen
    data = json.loads(urlopen(DATASET_API).read())
    for r in data.get('resources', []):
        if r.get('title') == 'decp.parquet':
            return r['url']
    raise RuntimeError("Could not find decp.parquet in dataset resources")


def safe_str(val):
    if val is None or (isinstance(val, float) and val != val):  # NaN check
        return None
    return str(val).strip() or None


def safe_float(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def safe_int(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def safe_date(val):
    if val is None:
        return None
    try:
        return str(val)[:10]
    except (TypeError, ValueError):
        return None


def transform_row(row):
    """Transform a DECP DataFrame row into a contracts record dict."""
    uid = safe_str(row.get('uid'))
    if not uid:
        return None

    contract_id = safe_str(row.get('id'))
    nature = safe_str(row.get('nature')) or ''
    contract_type = safe_str(row.get('type')) or ''
    procedure = safe_str(row.get('procedure')) or ''
    acheteur_cat = safe_str(row.get('acheteur_categorie')) or ''

    cpv = safe_str(row.get('codeCPV'))
    cpv_codes = [cpv] if cpv else None

    # Build place_of_execution from commune + département
    commune = safe_str(row.get('acheteur_commune_nom'))
    dept = safe_str(row.get('acheteur_departement_nom'))
    region = safe_str(row.get('acheteur_region_nom'))
    place_parts = [p for p in [commune, dept, region] if p]
    place_of_execution = ', '.join(place_parts) if place_parts else None

    duration_months = safe_int(row.get('dureeMois'))
    duration = f"{duration_months} MON" if duration_months else None

    return {
        'source': 'FR_DECP',
        'source_id': uid,
        'lot_number': '0',
        'reference_number': contract_id,
        'source_url': safe_str(row.get('sourceFile')),
        'status': 'formalized',  # DECP only contains awarded/notified contracts
        'contract_nature': TYPE_MAP.get(contract_type),
        'is_concession': nature in CONCESSION_NATURES,
        'contract_type_original': contract_type or None,
        'procedure_type': PROCEDURE_MAP.get(procedure),
        'notice_type': 'can-standard',
        'contract_title': safe_str(row.get('objet')),
        'lot_title': None,
        'contract_duration': duration,
        'contracting_authority': safe_str(row.get('acheteur_nom')),
        'authority_id': safe_str(row.get('acheteur_id')),
        'authority_type': AUTHORITY_MAP.get(acheteur_cat),
        'authority_dir3': None,
        'place_of_execution': place_of_execution,
        'nuts_code': None,
        'country': 'FR',
        'estimated_value': safe_float(row.get('montant')),
        'estimated_value_currency': 'EUR',
        'base_budget': None,
        'base_budget_currency': 'EUR',
        'awardee': safe_str(row.get('titulaire_nom')),
        'awardee_id': safe_str(row.get('titulaire_id')),
        'award_value': safe_float(row.get('montant')),
        'award_value_with_tax': None,
        'award_value_currency': 'EUR',
        'num_offers': safe_int(row.get('offresRecues')),
        'excluded_low_offers': None,
        'date_published': safe_date(row.get('datePublicationDonnees')),
        'date_updated': safe_date(row.get('datePublicationDonnees')),
        'date_awarded': safe_date(row.get('dateNotification')),
        'date_contract_start': None,
        'eu_funded': None,
        'is_aggregated': None,
        'cpv_codes': cpv_codes,
    }


def main():
    parser = argparse.ArgumentParser(description='Sync French DECP procurement data')
    parser.add_argument('--from', dest='date_from', default=None,
                        help='Filter: notification date from (YYYY-MM-DD)')
    parser.add_argument('--to', dest='date_to', default=None,
                        help='Filter: notification date to (YYYY-MM-DD)')
    args = parser.parse_args()

    import pyarrow.parquet as pq
    import pandas as pd

    # Download Parquet
    print("Finding latest Parquet file...")
    url = get_parquet_url()
    parquet_path = '/tmp/decp.parquet'

    if os.path.exists(parquet_path):
        print(f"Using cached {parquet_path}")
    else:
        print(f"Downloading {url}...")
        urlretrieve(url, parquet_path)
        size_mb = os.path.getsize(parquet_path) / 1024 / 1024
        print(f"Downloaded {size_mb:.1f} MB")

    print("Reading Parquet file...")
    df = pq.read_table(parquet_path).to_pandas()
    print(f"Total rows in file: {len(df):,}")

    # Filter to current data only
    df = df[df['donneesActuelles'] == True]
    print(f"After donneesActuelles filter: {len(df):,}")

    # Apply date filters
    if args.date_from:
        df = df[df['dateNotification'].apply(lambda x: str(x)[:10] if x is not None else '') >= args.date_from]
        print(f"After --from {args.date_from}: {len(df):,}")
    if args.date_to:
        df = df[df['dateNotification'].apply(lambda x: str(x)[:10] if x is not None else '') <= args.date_to]
        print(f"After --to {args.date_to}: {len(df):,}")

    if len(df) == 0:
        print("Nothing to sync.")
        return

    # Register source if needed
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ref_source (code, label, url, country)
            VALUES ('FR_DECP', 'Données Essentielles de la Commande Publique', 'https://data.gouv.fr', 'FR')
            ON CONFLICT (code) DO NOTHING
        """)
    conn.commit()

    # Transform and upsert in batches
    batch_size = 10000
    total_upserted = 0
    total_skipped = 0
    start_time = time.time()

    for batch_start in range(0, len(df), batch_size):
        batch_df = df.iloc[batch_start:batch_start + batch_size]
        records = []

        for _, row in batch_df.iterrows():
            rec = transform_row(row)
            if rec:
                records.append(tuple(rec.get(col) for col in COLUMNS))
            else:
                total_skipped += 1

        if records:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, records, page_size=1000)
            conn.commit()
            total_upserted += len(records)

        elapsed = time.time() - start_time
        rate = total_upserted / elapsed if elapsed > 0 else 0
        remaining = len(df) - batch_start - len(batch_df)
        eta = remaining / rate if rate > 0 else 0
        print(f"  {total_upserted:,}/{len(df):,} ({100*total_upserted/len(df):.1f}%) "
              f"- {rate:.0f} rec/s - ETA: {eta/60:.1f} min", flush=True)

    conn.close()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Sync completed!")
    print(f"  Records upserted: {total_upserted:,}")
    print(f"  Records skipped:  {total_skipped:,}")
    print(f"  Duration:         {total_time/60:.1f} minutes")
    if total_time > 0:
        print(f"  Rate:             {total_upserted/total_time:.0f} records/second")


if __name__ == '__main__':
    main()
