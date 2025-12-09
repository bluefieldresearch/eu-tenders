#!/usr/bin/env python3
"""
Import Catalan public procurement data from the Transparency API
"""

import requests
import psycopg2
from psycopg2.extras import execute_values
import time
import sys

API_URL = "https://analisi.transparenciacatalunya.cat/resource/ybgg-dgi6.json"
BATCH_SIZE = 50000  # Socrata API limit
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'licitaciones',
    'user': 'licitaciones',
    'password': 'licitaciones'
}

def get_total_count():
    """Get total number of records"""
    resp = requests.get(f"{API_URL}?$select=count(*)")
    resp.raise_for_status()
    return int(resp.json()[0]['count'])

def fetch_batch(offset, limit=BATCH_SIZE):
    """Fetch a batch of records from the API"""
    params = {
        '$limit': limit,
        '$offset': offset,
        '$order': ':id'  # Consistent ordering for pagination
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()

def extract_url(field):
    """Extract URL from nested dict field"""
    if isinstance(field, dict):
        return field.get('url', '')
    return field or ''

def parse_numeric(value):
    """Parse numeric value, return None if invalid"""
    if value is None or value == '':
        return None
    try:
        return float(str(value).replace(',', '.'))
    except:
        return None

def parse_int(value):
    """Parse integer value, return None if invalid"""
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except:
        return None

def parse_timestamp(value):
    """Parse timestamp value, return None if invalid"""
    if value is None or value == '':
        return None
    try:
        # Remove timezone info for PostgreSQL timestamp without time zone
        return value.replace('T', ' ').split('.')[0]
    except:
        return None

def transform_record(r):
    """Transform API record to database row"""
    return (
        r.get('codi_expedient'),
        r.get('codi_organ'),
        r.get('nom_organ'),
        r.get('codi_departament_ens'),
        r.get('nom_departament_ens'),
        r.get('codi_ambit'),
        r.get('nom_ambit'),
        r.get('codi_dir3'),
        r.get('codi_ine10'),
        r.get('tipus_contracte'),
        r.get('procediment'),
        r.get('fase_publicacio'),
        r.get('denominacio'),
        r.get('objecte_contracte'),
        r.get('codi_cpv'),
        parse_numeric(r.get('valor_estimat_contracte')),
        parse_numeric(r.get('pressupost_base_licitacio')),
        r.get('durada_contracte'),
        parse_timestamp(r.get('data_publicacio_anunci')),
        parse_timestamp(r.get('data_publicacio_licitacio')),
        parse_timestamp(r.get('data_publicacio_adjudicacio')),
        parse_timestamp(r.get('data_publicacio_formalitzacio')),
        parse_timestamp(r.get('data_publicacio_contracte')),
        parse_timestamp(r.get('data_adjudicacio_contracte')),
        parse_timestamp(r.get('data_formalitzacio_contracte')),
        r.get('numero_lot'),
        r.get('identificacio_adjudicatari'),
        r.get('denominacio_adjudicatari'),
        parse_numeric(r.get('import_adjudicacio_sense')),
        parse_numeric(r.get('import_adjudicacio_amb_iva')),
        parse_int(r.get('ofertes_rebudes')),
        r.get('resultat'),
        r.get('es_agregada'),
        extract_url(r.get('enllac_publicacio')),
        r.get('tipus_tramitacio'),
        r.get('tipus_identificacio'),
        r.get('codi_nuts'),
        r.get('lloc_execucio'),
        r.get('finançament_europeu')
    )

def insert_batch(cursor, records):
    """Insert a batch of records into the database"""
    sql = """
        INSERT INTO catalunya_licitaciones (
            codi_expedient, codi_organ, nom_organ, codi_departament_ens, nom_departament_ens,
            codi_ambit, nom_ambit, codi_dir3, codi_ine10, tipus_contracte, procediment,
            fase_publicacio, denominacio, objecte_contracte, codi_cpv, valor_estimat_contracte,
            pressupost_base_licitacio, durada_contracte, data_publicacio_anunci,
            data_publicacio_licitacio, data_publicacio_adjudicacio, data_publicacio_formalitzacio,
            data_publicacio_contracte, data_adjudicacio_contracte, data_formalitzacio_contracte,
            numero_lot, identificacio_adjudicatari, denominacio_adjudicatari,
            import_adjudicacio_sense, import_adjudicacio_amb_iva, ofertes_rebudes, resultat,
            es_agregada, enllac_publicacio, tipus_tramitacio, tipus_identificacio,
            codi_nuts, lloc_execucio, finançament_europeu
        ) VALUES %s
    """
    execute_values(cursor, sql, records)

def main():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Clear existing data
    print("Clearing existing data...")
    cursor.execute("TRUNCATE TABLE catalunya_licitaciones RESTART IDENTITY")
    conn.commit()

    print("Getting total record count...")
    total = get_total_count()
    print(f"Total records to import: {total:,}")

    offset = 0
    imported = 0
    start_time = time.time()

    while offset < total:
        batch_start = time.time()
        print(f"\nFetching batch at offset {offset:,}...", end=" ", flush=True)

        try:
            data = fetch_batch(offset)
            if not data:
                print("No data returned, stopping.")
                break

            print(f"got {len(data):,} records.", end=" ", flush=True)

            # Transform and insert
            records = [transform_record(r) for r in data]
            insert_batch(cursor, records)
            conn.commit()

            imported += len(data)
            batch_time = time.time() - batch_start
            total_time = time.time() - start_time
            rate = imported / total_time
            eta = (total - imported) / rate if rate > 0 else 0

            print(f"Inserted. Total: {imported:,}/{total:,} ({100*imported/total:.1f}%) - {rate:.0f} rec/s - ETA: {eta/60:.1f} min")

            offset += len(data)

        except requests.exceptions.RequestException as e:
            print(f"\nAPI error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except Exception as e:
            print(f"\nError: {e}")
            conn.rollback()
            raise

    cursor.close()
    conn.close()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Import completed!")
    print(f"Total records imported: {imported:,}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Average rate: {imported/total_time:.0f} records/second")

if __name__ == "__main__":
    main()
