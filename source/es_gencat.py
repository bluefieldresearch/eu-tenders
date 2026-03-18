#!/usr/bin/env python3
"""
Incremental sync of GENCAT procurement data into the contracts table.

Fetches records from the Catalunya Transparency API that were updated within
a given date range, and upserts them into the target database.

Usage:
    python3 source/gencat.py --from 2025-02-01 --to 2025-02-28
    python3 source/gencat.py --from 2025-02-01  # defaults --to to today
    python3 source/gencat.py --from 2025-02-01 --target bigquery
"""

import argparse
import json
import requests
import sys
import time
from datetime import date, datetime

API_URL = "https://analisi.transparenciacatalunya.cat/resource/ybgg-dgi6.json"
BATCH_SIZE = 50000

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

BQ_PROJECT = 'eu-spanish-tender-dataset'
BQ_DATASET = 'EU_Spanish_Tender_Dataset'

# Column names in order (shared between targets)
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

# --- Contract nature mapping ---
NATURE_MAP = {
    'Obres': 'works',
    "Concessió d'obres": 'works',
    'Serveis': 'services',
    'Concessió de serveis': 'services',
    "Contracte de serveis especials (annex IV)": 'services',
    "Concessió de serveis especials (annex IV)": 'services',
    'Administratiu especial': 'services',
    "Privat d'Administració Pública": 'services',
    'Altra legislació sectorial': 'services',
    'Subministraments': 'supplies',
}

CONCESSION_TYPES = {
    'Concessió de serveis',
    "Concessió d'obres",
    "Concessió de serveis especials (annex IV)",
}

# --- Procedure type mapping ---
PROCEDURE_MAP = {
    'Obert': 'open',
    'Obert simplificat': 'open',
    'Obert Simplificat': 'open',
    'Obert simplificat abreujat': 'open',
    'Restringit': 'restricted',
    'Negociat sense publicitat': 'neg-wo-call',
    'Negociat amb publicitat': 'neg-w-call',
    'Diàleg competitiu': 'comp-dial',
    'Associació per a la innovació': 'innovation',
    'Licitació amb negociació': 'neg-w-call',
    'Contracte menor': 'minor',
    'Concurs de projectes': 'design-contest',
    'Adjudicacions directes no menors': 'internal',
    'Altres procediments segons instruccions internes': 'internal',
    'Tramitació amb mesures de gestió eficient': 'oth-single',
    'Tramitacio amb mesures de gestió eficient': 'oth-single',
    "Específic de Sistema Dinàmic d'adquisició": 'dynamic-acq',
}

# --- Authority type mapping ---
AUTHORITY_MAP = {
    'Departaments i sector públic de la Generalitat de Catalunya': 'ra',
    "Entitats de l'administració local": 'la',
    'Universitats': 'university',
    'Organismes independents i/o estatutaris': 'body-pl-ra',
    'Altres ens': 'other',
}

# --- Notice type mapping ---
NOTICE_MAP = {
    'Publicació agregada de contractes': 'aggregated',
    'Anunci de licitació': 'cn-standard',
    'Adjudicació': 'can-standard',
    'Formalització': 'can-standard',
    'Anunci previ': 'pin-only',
    'Alerta futura': 'pin-only',
    'Consulta preliminar del mercat': 'pmc',
}


# ============================================================================
# API helpers
# ============================================================================

def get_count(date_from, date_to):
    where = f":updated_at between '{date_from}T00:00:00' and '{date_to}T23:59:59'"
    resp = requests.get(API_URL, params={'$select': 'count(*)', '$where': where})
    resp.raise_for_status()
    return int(resp.json()[0]['count'])


def fetch_batch(date_from, date_to, offset, limit=BATCH_SIZE):
    where = f":updated_at between '{date_from}T00:00:00' and '{date_to}T23:59:59'"
    params = {
        '$limit': limit,
        '$offset': offset,
        '$order': ':id',
        '$where': where,
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()


# ============================================================================
# Parsing helpers
# ============================================================================

def parse_numeric(value):
    if value is None or value == '':
        return None
    try:
        return float(str(value).replace(',', '.'))
    except (TypeError, ValueError):
        return None


def parse_int(value):
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def parse_timestamp(value):
    if value is None or value == '':
        return None
    try:
        return value.replace('T', ' ').split('.')[0]
    except (TypeError, AttributeError):
        return None


def parse_date(value):
    if value is None or value == '':
        return None
    try:
        return value.split('T')[0]
    except (TypeError, AttributeError):
        return None


def extract_url(field):
    if isinstance(field, dict):
        return field.get('url', '')
    return field or None


def compute_status(fase, resultat):
    if resultat == 'Desert':
        return 'deserted'
    if resultat in ('Desisitment', 'Desistiment', 'Renúncia'):
        return 'cancelled'
    if fase == 'Anul·lació':
        return 'cancelled'
    if fase == 'Formalització' or resultat == 'Formalització':
        return 'formalized'
    if fase == 'Adjudicació' or resultat == 'Adjudicació':
        return 'awarded'
    if fase == 'Expedient en avaluació':
        return 'evaluation'
    if fase in ('Anunci previ', 'Alerta futura', 'Consulta preliminar del mercat'):
        return 'prior-notice'
    if fase == 'Anunci de licitació':
        return 'announced'
    if fase == 'Publicació agregada de contractes':
        return 'formalized'
    return 'announced'


def greatest_timestamp(*values):
    valid = [v for v in values if v]
    return max(valid) if valid else None


# ============================================================================
# Transform
# ============================================================================

def transform_record(r):
    """Transform an API record into a dict matching COLUMNS."""
    codi_exp = r.get('codi_expedient')
    codi_org = r.get('codi_organ')
    if not codi_exp or not codi_org:
        return None

    lot = r.get('numero_lot') or '0'
    tipus = r.get('tipus_contracte') or ''
    fase = r.get('fase_publicacio') or ''
    resultat = r.get('resultat') or ''
    procediment = r.get('procediment') or ''
    nom_ambit = r.get('nom_ambit') or ''
    eu_funded = r.get('finançament_europeu') or r.get('financament_europeu') or ''
    es_agregada = r.get('es_agregada') or ''

    ts_anunci = parse_timestamp(r.get('data_publicacio_anunci'))
    ts_licitacio = parse_timestamp(r.get('data_publicacio_licitacio'))
    ts_adjudicacio = parse_timestamp(r.get('data_publicacio_adjudicacio'))
    ts_formalitzacio = parse_timestamp(r.get('data_publicacio_formalitzacio'))
    ts_contracte = parse_timestamp(r.get('data_publicacio_contracte'))

    cpv_raw = r.get('codi_cpv') or ''
    cpv_codes = [c.strip() for c in cpv_raw.split('||') if c.strip()] if cpv_raw else None

    return {
        'source': 'ES_GENCAT',
        'source_id': f"{codi_exp}/{codi_org}",
        'lot_number': lot,
        'reference_number': codi_exp,
        'source_url': extract_url(r.get('enllac_publicacio')),
        'status': compute_status(fase, resultat),
        'contract_nature': NATURE_MAP.get(tipus),
        'is_concession': tipus in CONCESSION_TYPES,
        'contract_type_original': tipus or None,
        'procedure_type': PROCEDURE_MAP.get(procediment),
        'notice_type': NOTICE_MAP.get(fase),
        'contract_title': r.get('objecte_contracte'),
        'lot_title': r.get('denominacio'),
        'contract_duration': r.get('durada_contracte'),
        'contracting_authority': r.get('nom_organ'),
        'authority_id': codi_org,
        'authority_type': AUTHORITY_MAP.get(nom_ambit),
        'authority_dir3': r.get('codi_dir3'),
        'place_of_execution': r.get('lloc_execucio'),
        'nuts_code': r.get('codi_nuts'),
        'country': 'ES',
        'estimated_value': parse_numeric(r.get('valor_estimat_contracte')),
        'estimated_value_currency': 'EUR',
        'base_budget': parse_numeric(r.get('pressupost_base_licitacio')),
        'base_budget_currency': 'EUR',
        'awardee': r.get('denominacio_adjudicatari'),
        'awardee_id': r.get('identificacio_adjudicatari'),
        'award_value': parse_numeric(r.get('import_adjudicacio_sense')),
        'award_value_with_tax': parse_numeric(r.get('import_adjudicacio_amb_iva')),
        'award_value_currency': 'EUR',
        'num_offers': parse_int(r.get('ofertes_rebudes')),
        'excluded_low_offers': None,
        'date_published': ts_anunci or ts_licitacio,
        'date_updated': greatest_timestamp(ts_anunci, ts_licitacio,
                                           ts_adjudicacio, ts_formalitzacio,
                                           ts_contracte),
        'date_awarded': parse_date(r.get('data_adjudicacio_contracte')),
        'date_contract_start': parse_date(r.get('data_formalitzacio_contracte')),
        'eu_funded': True if eu_funded == 'SÍ' else (False if eu_funded == 'NO' else None),
        'is_aggregated': True if es_agregada == 'SÍ' else (False if es_agregada == 'NO' else None),
        'cpv_codes': cpv_codes,
    }


# ============================================================================
# PostgreSQL target
# ============================================================================

class PostgresTarget:
    def __init__(self):
        import psycopg2
        import psycopg2.extras
        self._psycopg2 = psycopg2
        self._extras = psycopg2.extras
        self.conn = psycopg2.connect(**PG_CONFIG)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def upsert(self, records):
        sql = """
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
        rows = [tuple(rec[col] for col in COLUMNS) for rec in records]
        self._extras.execute_batch(self.cursor, sql, rows, page_size=1000)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def rollback(self):
        self.conn.rollback()


# ============================================================================
# BigQuery target
# ============================================================================

class BigQueryTarget:
    def __init__(self):
        from google.cloud import bigquery
        self.client = bigquery.Client(project=BQ_PROJECT)
        self.table = f"{BQ_PROJECT}.{BQ_DATASET}.contracts"
        self._staging = f"{BQ_PROJECT}.{BQ_DATASET}._staging_gencat"
        # Create staging table (temp)
        self.client.query(f"""
            CREATE TABLE IF NOT EXISTS `{self._staging}`
            AS SELECT * FROM `{self.table}` WHERE FALSE
        """).result()
        # Truncate staging
        self.client.query(f"TRUNCATE TABLE `{self._staging}`").result()
        self._buffer = []

    def upsert(self, records):
        from google.cloud import bigquery

        # Convert records to BigQuery-compatible rows
        rows = []
        for rec in records:
            row = {}
            for col in COLUMNS:
                val = rec[col]
                if val is None:
                    continue
                if col in ('date_awarded', 'date_contract_start') and val:
                    row[col] = val  # string YYYY-MM-DD
                elif col in ('date_published', 'date_updated') and val:
                    # Ensure ISO format for TIMESTAMP
                    row[col] = val.replace(' ', 'T')
                else:
                    row[col] = val
            row['last_synced_at'] = datetime.utcnow().isoformat()
            rows.append(row)

        self._buffer.extend(rows)

        # Flush to staging in batches of 10K
        if len(self._buffer) >= 10000:
            self._flush_staging()

    def _flush_staging(self):
        if not self._buffer:
            return
        from google.cloud import bigquery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = self.client.load_table_from_json(
            self._buffer, self._staging, job_config=job_config
        )
        job.result()
        self._buffer = []

    def close(self):
        # Flush remaining buffer
        self._flush_staging()

        # MERGE from staging into contracts
        merge_sql = f"""
            MERGE `{self.table}` AS target
            USING `{self._staging}` AS src
            ON target.source = src.source
               AND target.source_id = src.source_id
               AND target.lot_number = src.lot_number
            WHEN MATCHED THEN UPDATE SET
                reference_number = src.reference_number,
                source_url = src.source_url,
                status = src.status,
                contract_nature = src.contract_nature,
                is_concession = src.is_concession,
                contract_type_original = src.contract_type_original,
                procedure_type = src.procedure_type,
                notice_type = src.notice_type,
                contract_title = src.contract_title,
                lot_title = src.lot_title,
                contract_duration = src.contract_duration,
                contracting_authority = src.contracting_authority,
                authority_id = src.authority_id,
                authority_type = src.authority_type,
                authority_dir3 = src.authority_dir3,
                place_of_execution = src.place_of_execution,
                nuts_code = src.nuts_code,
                country = src.country,
                estimated_value = src.estimated_value,
                estimated_value_currency = src.estimated_value_currency,
                base_budget = src.base_budget,
                base_budget_currency = src.base_budget_currency,
                awardee = src.awardee,
                awardee_id = src.awardee_id,
                award_value = src.award_value,
                award_value_with_tax = src.award_value_with_tax,
                award_value_currency = src.award_value_currency,
                num_offers = src.num_offers,
                excluded_low_offers = src.excluded_low_offers,
                date_published = src.date_published,
                date_updated = src.date_updated,
                date_awarded = src.date_awarded,
                date_contract_start = src.date_contract_start,
                eu_funded = src.eu_funded,
                is_aggregated = src.is_aggregated,
                cpv_codes = src.cpv_codes,
                last_synced_at = src.last_synced_at
            WHEN NOT MATCHED THEN INSERT ROW
        """
        result = self.client.query(merge_sql).result()
        print(f"\n  BigQuery MERGE: {result.num_dml_affected_rows} rows affected")

        # Drop staging table
        self.client.query(f"DROP TABLE IF EXISTS `{self._staging}`").result()

    def rollback(self):
        # Drop staging table on error
        try:
            self.client.query(f"DROP TABLE IF EXISTS `{self._staging}`").result()
        except Exception:
            pass


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Incremental sync of GENCAT procurement data')
    parser.add_argument('--from', dest='date_from', required=True,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to', dest='date_to', default=str(date.today()),
                        help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--target', choices=['postgres', 'bigquery'], default='postgres',
                        help='Target database (default: postgres)')
    args = parser.parse_args()

    date_from = args.date_from
    date_to = args.date_to

    print(f"Syncing GENCAT data updated between {date_from} and {date_to}")
    print(f"Target: {args.target}")

    print("Getting record count...")
    total = get_count(date_from, date_to)
    print(f"Records to sync: {total:,}")

    if total == 0:
        print("Nothing to sync.")
        return

    if args.target == 'bigquery':
        target = BigQueryTarget()
    else:
        target = PostgresTarget()

    offset = 0
    fetched = 0
    upserted = 0
    skipped = 0
    start_time = time.time()

    try:
        while offset < total:
            print(f"\nFetching batch at offset {offset:,}...", end=" ", flush=True)

            try:
                data = fetch_batch(date_from, date_to, offset)
            except requests.exceptions.RequestException as e:
                print(f"\nAPI error: {e}. Retrying in 5s...")
                time.sleep(5)
                continue

            if not data:
                print("No data returned, stopping.")
                break

            print(f"got {len(data):,}.", end=" ", flush=True)

            records = []
            for r in data:
                row = transform_record(r)
                if row:
                    records.append(row)
                else:
                    skipped += 1

            target.upsert(records)

            fetched += len(data)
            upserted += len(records)
            total_time = time.time() - start_time
            rate = fetched / total_time if total_time > 0 else 0
            eta = (total - fetched) / rate if rate > 0 else 0

            print(f"Upserted {len(records):,}. "
                  f"Total: {fetched:,}/{total:,} ({100*fetched/total:.1f}%) "
                  f"- {rate:.0f} rec/s - ETA: {eta/60:.1f} min")

            offset += len(data)

        target.close()

    except Exception as e:
        target.rollback()
        print(f"\nError: {e}")
        raise

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Sync completed!")
    print(f"  Target:           {args.target}")
    print(f"  Records fetched:  {fetched:,}")
    print(f"  Records upserted: {upserted:,}")
    print(f"  Records skipped:  {skipped:,} (missing codi_expedient/codi_organ)")
    print(f"  Duration:         {total_time/60:.1f} minutes")
    print(f"  Rate:             {fetched/total_time:.0f} records/second")


if __name__ == '__main__':
    main()
