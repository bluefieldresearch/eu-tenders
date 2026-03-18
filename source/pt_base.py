#!/usr/bin/env python3
"""
Sync of Portuguese BASE procurement data into the contracts table.

Downloads yearly XLSX files from dados.gov.pt and upserts into the contracts table.

Usage:
    python3 source/pt_base.py --from 2024-01-01 --to 2024-12-31
    python3 source/pt_base.py --from 2025-01-01  # defaults --to to today
"""

import argparse
import os
import sys
import time
from datetime import date
from urllib.request import urlretrieve

import pandas as pd
import psycopg2
import psycopg2.extras

BASE_URL = "https://dados.gov.pt/s/resources/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026"

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

# --- Contract type mapping ---
# Portuguese types can be multi-valued (newline separated), we take the first
TYPE_MAP = {
    'Aquisição de bens móveis': ('supplies', False),
    'Aquisição de serviços': ('services', False),
    'Empreitadas de obras públicas': ('works', False),
    'Locação de bens móveis': ('supplies', False),
    'Concessão de serviços públicos': ('services', True),
    'Concessão de obras públicas': ('works', True),
    'Sociedade': ('services', False),
    'Outros': (None, False),
}

# --- Procedure mapping ---
PROCEDURE_MAP = {
    'Ajuste Direto Regime Geral': 'neg-wo-call',
    'Ajuste direto simplificado': 'neg-wo-call',
    'Ajuste direto simplificado ao abrigo da Lei n.º 30/2021, de 21.05': 'neg-wo-call',
    'Ajuste Direto Regime Geral ao abrigo do artigo 7º da Lei n.º 30/2021, de 21.05': 'neg-wo-call',
    'Consulta Prévia': 'neg-w-call',
    'Consulta Prévia Simplificada': 'neg-w-call',
    'Consulta prévia ao abrigo do artigo 7º da Lei n.º 30/2021, de 21.05': 'neg-w-call',
    'Concurso público': 'open',
    'Concurso público simplificado': 'open',
    'Concurso limitado por prévia qualificação': 'restricted',
    'Procedimento de negociação': 'neg-w-call',
    'Ao abrigo de acordo-quadro (art.º 259.º)': 'oth-single',
    'Ao abrigo de acordo-quadro (art.º 258.º)': 'oth-single',
    'Contratação excluída II': 'oth-single',
    'Setores especiais – isenção parte II': 'oth-single',
    'Concurso de conceção simplificado': 'design-contest',
    'Concurso de ideias simplificado': 'design-contest',
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


def safe_str(val):
    if val is None or (isinstance(val, float) and val != val):
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
    if val is None or (isinstance(val, float) and val != val):
        return None
    s = str(val)
    if s in ('NaT', 'nan', 'None', ''):
        return None
    try:
        return s[:10]
    except (TypeError, ValueError):
        return None


def safe_timestamp(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    s = str(val)
    if s in ('NaT', 'nan', 'None', ''):
        return None
    try:
        return s[:19]
    except (TypeError, ValueError):
        return None


def parse_nif_name(val):
    """Parse 'NIF - Name' format. Returns (nif, name)."""
    s = safe_str(val)
    if not s:
        return None, None
    if ' - ' in s:
        parts = s.split(' - ', 1)
        nif = parts[0].strip()
        name = parts[1].strip()
        return nif if nif and nif != '-' else None, name
    return None, s


def parse_cpv_codes(val):
    """Parse CPV codes from 'CODE - Description' format, newline separated."""
    s = safe_str(val)
    if not s:
        return None
    codes = []
    for line in s.split('\n'):
        line = line.strip()
        if not line:
            continue
        code = line.split(' - ')[0].split(' ')[0].strip()
        if code and code[0].isdigit():
            codes.append(code)
    return codes if codes else None


def parse_contract_type(val):
    """Parse contract type, taking the first if multi-valued."""
    s = safe_str(val)
    if not s:
        return None, False, None
    first = s.split('\n')[0].strip()
    nature, is_concession = TYPE_MAP.get(first, (None, False))
    return nature, is_concession, s.replace('\n', '; ')


def parse_concorrentes_count(val):
    """Count number of bidders from the concorrentes field."""
    s = safe_str(val)
    if not s:
        return None
    return len([line for line in s.split('\n') if line.strip()])


def parse_awardee(val):
    """Parse awardee(s). If multiple, join with ' | '. Returns (name, id)."""
    s = safe_str(val)
    if not s:
        return None, None
    lines = [l.strip() for l in s.split('\n') if l.strip()]
    if not lines:
        return None, None

    names = []
    ids = []
    for line in lines:
        nif, name = parse_nif_name(line)
        if name:
            names.append(name)
        if nif:
            ids.append(nif)

    awardee = ' | '.join(names) if names else None
    awardee_id = ' | '.join(ids) if ids else None
    return awardee, awardee_id


def transform_row(row):
    """Transform a DataFrame row into a contracts record dict."""
    idcontrato = safe_str(row.get('idcontrato'))
    if not idcontrato:
        return None

    nature, is_concession, type_original = parse_contract_type(row.get('tipoContrato'))
    procedure = safe_str(row.get('tipoprocedimento'))
    authority_nif, authority_name = parse_nif_name(row.get('adjudicante'))
    awardee, awardee_id = parse_awardee(row.get('adjudicatarios'))
    cpv_codes = parse_cpv_codes(row.get('CPV'))
    num_offers = parse_concorrentes_count(row.get('concorrentes'))

    duration_days = safe_int(row.get('prazoExecucao'))
    duration = f"{duration_days} days" if duration_days else None

    source_url = f"https://www.base.gov.pt/Base4/pt/detalhe/?type=contratos&id={idcontrato}"

    return {
        'source': 'PT_BASE',
        'source_id': idcontrato,
        'lot_number': '0',
        'reference_number': safe_str(row.get('idprocedimento')),
        'source_url': source_url,
        'status': 'formalized',  # BASE only has awarded/signed contracts
        'contract_nature': nature,
        'is_concession': is_concession,
        'contract_type_original': type_original,
        'procedure_type': PROCEDURE_MAP.get(procedure),
        'notice_type': 'can-standard',
        'contract_title': safe_str(row.get('objectoContrato')),
        'lot_title': None,
        'contract_duration': duration,
        'contracting_authority': authority_name,
        'authority_id': authority_nif,
        'authority_type': None,  # Not available in dataset
        'authority_dir3': None,
        'place_of_execution': safe_str(row.get('LocalExecucao')),
        'nuts_code': None,
        'country': 'PT',
        'estimated_value': None,
        'estimated_value_currency': 'EUR',
        'base_budget': safe_float(row.get('precoBaseProcedimento')),
        'base_budget_currency': 'EUR',
        'awardee': awardee,
        'awardee_id': awardee_id,
        'award_value': safe_float(row.get('precoContratual')),
        'award_value_with_tax': None,
        'award_value_currency': 'EUR',
        'num_offers': num_offers,
        'excluded_low_offers': None,
        'date_published': safe_timestamp(row.get('dataPublicacao')),
        'date_updated': safe_timestamp(row.get('dataPublicacao')),
        'date_awarded': safe_date(row.get('dataDecisaoAdjudicacao')),
        'date_contract_start': safe_date(row.get('dataCelebracaoContrato')),
        'eu_funded': None,
        'is_aggregated': None,
        'cpv_codes': cpv_codes,
    }


def get_xlsx_url(year):
    """Get the download URL for a given year. Uses a known pattern."""
    # The URLs include a date-based prefix that changes; try the latest known pattern
    # Fallback: query the dados.gov.pt API
    import json
    from urllib.request import urlopen

    api_url = "https://dados.gov.pt/api/1/datasets/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/"
    data = json.loads(urlopen(api_url).read())
    for r in data.get('resources', []):
        if r.get('title') == f'contratos{year}.xlsx':
            return r['url']
    raise RuntimeError(f"Could not find contratos{year}.xlsx in dataset resources")


def main():
    parser = argparse.ArgumentParser(description='Sync Portuguese BASE procurement data')
    parser.add_argument('--from', dest='date_from', required=True,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to', dest='date_to', default=str(date.today()),
                        help='End date (YYYY-MM-DD), defaults to today')
    args = parser.parse_args()

    # Derive which yearly files to download from the date range
    year_from = int(args.date_from[:4])
    year_to = int(args.date_to[:4])
    years = list(range(year_from, year_to + 1))

    print(f"Syncing PT_BASE data from {args.date_from} to {args.date_to}")
    print(f"Years to download: {', '.join(str(y) for y in years)}")

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False

    # Register source if needed
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ref_source (code, label, url, country)
            VALUES ('PT_BASE', 'Portal BASE - Contratos Públicos', 'https://www.base.gov.pt', 'PT')
            ON CONFLICT (code) DO NOTHING
        """)
    conn.commit()

    total_upserted = 0
    total_skipped = 0
    start_time = time.time()

    for year in years:
        print(f"\n{'='*60}")
        print(f"Processing year {year}")

        xlsx_path = f'/tmp/contratos{year}.xlsx'
        if os.path.exists(xlsx_path):
            print(f"  Using cached {xlsx_path}")
        else:
            print(f"  Finding download URL...")
            url = get_xlsx_url(year)
            print(f"  Downloading {url}...")
            urlretrieve(url, xlsx_path)
            size_mb = os.path.getsize(xlsx_path) / 1024 / 1024
            print(f"  Downloaded {size_mb:.1f} MB")

        print(f"  Reading Excel file...")
        df = pd.read_excel(xlsx_path)
        print(f"  Total rows: {len(df):,}")

        # Apply date filters
        if args.date_from:
            df['_pub_date'] = df['dataPublicacao'].apply(lambda x: str(x)[:10] if pd.notna(x) else '')
            df = df[df['_pub_date'] >= args.date_from]
            print(f"  After --from {args.date_from}: {len(df):,}")
        if args.date_to:
            if '_pub_date' not in df.columns:
                df['_pub_date'] = df['dataPublicacao'].apply(lambda x: str(x)[:10] if pd.notna(x) else '')
            df = df[df['_pub_date'] <= args.date_to]
            print(f"  After --to {args.date_to}: {len(df):,}")

        if len(df) == 0:
            print(f"  Nothing to sync for {year}.")
            continue

        # Transform and upsert in batches
        batch_size = 10000
        year_upserted = 0

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
                year_upserted += len(records)
                total_upserted += len(records)

            elapsed = time.time() - start_time
            rate = total_upserted / elapsed if elapsed > 0 else 0
            print(f"    {year_upserted:,}/{len(df):,} ({100*year_upserted/len(df):.1f}%) "
                  f"- {rate:.0f} rec/s", flush=True)

        print(f"  Year {year}: {year_upserted:,} records upserted")

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
