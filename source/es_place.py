#!/usr/bin/env python3
"""
Incremental sync of PLACE procurement data into the contracts table.

Downloads monthly ATOM/CODICE XML archives from the Plataforma de Contratación
del Estado, parses them, and upserts into the contracts table.

Usage:
    python3 source/place.py --from 2025-02-01 --to 2025-02-28
    python3 source/place.py --from 2025-01-01  # defaults --to to today
"""

import argparse
import io
import os
import sys
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime
from urllib.request import urlretrieve

import psycopg2
import psycopg2.extras

BASE_URL = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643"

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'cbc': 'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2',
    'cac': 'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2',
    'cac-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2',
    'cbc-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2',
}

# --- TypeCode mapping (from CODICE ContractCode) ---
TYPE_MAP = {
    '1': ('supplies', False),
    '2': ('services', False),
    '3': ('works', False),
    '7': ('services', True),   # Gestión de Servicios Públicos
    '8': ('services', True),   # Concesión de Servicios
    '21': ('works', True),     # Concesión de Obras Públicas
    '22': ('works', True),     # Concesión de Obras
    '31': ('services', False), # Administrativo especial
    '40': ('services', False), # Colaboración público-privado
    '50': ('services', False), # Privado
    '99': (None, False),       # Patrimonial / Otros
}

# --- TypeCode to original name ---
TYPE_ORIGINAL = {
    '1': 'Suministros',
    '2': 'Servicios',
    '3': 'Obras',
    '7': 'Gestión de Servicios Públicos',
    '8': 'Concesión de Servicios',
    '21': 'Concesión de Obras Públicas',
    '22': 'Concesión de Obras',
    '31': 'Administrativo especial',
    '40': 'Colaboración entre el sector público y sector privado',
    '50': 'Privado',
    '99': 'Patrimonial',
}

# --- StatusCode mapping ---
STATUS_MAP = {
    'PUB': 'announced',
    'EV': 'evaluation',
    'ADJ': 'awarded',
    'RES': 'formalized',
    'ANUL': 'cancelled',
    'PRE': 'prior-notice',
}

# --- ContractingPartyTypeCode mapping ---
AUTHORITY_MAP = {
    '1': 'cga',               # Administración General del Estado
    '2': 'ra',                # Comunidad Autónoma
    '3': 'la',                # Entidad Local
    '4': 'body-pl',           # Organismo Autónomo
    '5': 'body-pl-cga',       # Entidad de Derecho Público (estatal)
    '6': 'pub-undert-cga',    # Sociedad mercantil pública (estatal)
    '7': 'body-pl-ra',        # Entidad de Derecho Público (autonómica)
    '8': 'pub-undert-ra',     # Sociedad mercantil (autonómica)
    '9': 'body-pl-la',        # Entidad de Derecho Público (local)
    '10': 'pub-undert-la',    # Sociedad mercantil (local)
    '11': 'other',            # Otras entidades del sector público
    '12': 'university',       # Universidad
}

# --- TenderResult ResultCode mapping ---
RESULT_MAP = {
    '1': 'announced',    # Pendiente de adjudicación
    '2': 'evaluation',   # Provisional
    '3': 'awarded',      # Adjudicado
    '4': 'deserted',     # Desierto
    '5': 'cancelled',    # Renunciado
    '6': 'cancelled',    # Desistido
    '7': 'cancelled',    # No adjudicado
    '8': 'formalized',   # Formalizado
    '9': 'formalized',   # Adjudicado (con contrato)
}


def text(el, path, ns=NS):
    """Get text content of an element, or None."""
    node = el.find(path, ns)
    if node is not None and node.text:
        return node.text.strip()
    return None


def get_months(date_from, date_to):
    """Generate YYYYMM strings for months between two dates."""
    from_y, from_m = int(date_from[:4]), int(date_from[5:7])
    to_y, to_m = int(date_to[:4]), int(date_to[5:7])

    months = []
    y, m = from_y, from_m
    while (y, m) <= (to_y, to_m):
        months.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def download_month(yyyymm, tmpdir):
    """Download a monthly ZIP file. Returns path or None if not found."""
    url = f"{BASE_URL}/licitacionesPerfilesContratanteCompleto3_{yyyymm}.zip"
    path = os.path.join(tmpdir, f"{yyyymm}.zip")
    print(f"  Downloading {url}...", end=" ", flush=True)
    try:
        urlretrieve(url, path)
        size_mb = os.path.getsize(path) / 1024 / 1024
        print(f"{size_mb:.1f} MB")
        return path
    except Exception as e:
        print(f"failed: {e}")
        return None


def parse_entry(entry):
    """Parse a single ATOM entry into contract records (one per lot/result)."""
    cfs = entry.find('cac-ext:ContractFolderStatus', NS)
    if cfs is None:
        return []

    # Core identifiers
    folder_id = text(cfs, 'cbc:ContractFolderID')
    if not folder_id:
        return []

    atom_id = text(entry, 'atom:id')
    source_url = None
    link = entry.find('atom:link', NS)
    if link is not None:
        source_url = link.get('href')

    updated = text(entry, 'atom:updated')
    if updated:
        updated = updated.replace('T', ' ')[:19]  # Trim timezone

    # Status
    status_code = text(cfs, 'cbc-ext:ContractFolderStatusCode')
    status = STATUS_MAP.get(status_code, 'announced')

    # Contracting party
    lcp = cfs.find('cac-ext:LocatedContractingParty', NS)
    authority_name = None
    authority_nif = None
    authority_dir3 = None
    authority_type_code = None

    if lcp is not None:
        authority_type_code = text(lcp, 'cbc:ContractingPartyTypeCode')
        party = lcp.find('cac:Party', NS)
        if party is not None:
            authority_name = text(party, 'cac:PartyName/cbc:Name')
            for pid in party.findall('cac:PartyIdentification', NS):
                id_el = pid.find('cbc:ID', NS)
                if id_el is not None:
                    scheme = id_el.get('schemeName', '')
                    if scheme == 'DIR3':
                        authority_dir3 = id_el.text
                    elif scheme == 'NIF':
                        authority_nif = id_el.text

    # Procurement project (contract-level)
    pp = cfs.find('cac:ProcurementProject', NS)
    contract_title = None
    type_code = None
    estimated_value = None
    base_budget = None
    cpv_codes = []
    nuts_code = None
    duration = None
    currency = 'EUR'

    if pp is not None:
        contract_title = text(pp, 'cbc:Name')
        type_code = text(pp, 'cbc:TypeCode')

        ba = pp.find('cac:BudgetAmount', NS)
        if ba is not None:
            ev = text(ba, 'cbc:EstimatedOverallContractAmount')
            if ev:
                estimated_value = float(ev)
            te = text(ba, 'cbc:TaxExclusiveAmount')
            if te:
                base_budget = float(te)

        for cc in pp.findall('cac:RequiredCommodityClassification', NS):
            code = text(cc, 'cbc:ItemClassificationCode')
            if code:
                cpv_codes.append(code)

        nuts_code = text(pp, 'cac:RealizedLocation/cbc:CountrySubentityCode')

        dur = pp.find('cac:PlannedPeriod/cbc:DurationMeasure', NS)
        if dur is not None and dur.text:
            unit = dur.get('unitCode', 'MON')
            duration = f"{dur.text} {unit}"

    # Contract nature and concession flag
    nature, is_concession = TYPE_MAP.get(type_code, (None, False))
    contract_type_original = TYPE_ORIGINAL.get(type_code)

    # Authority type
    authority_type = AUTHORITY_MAP.get(authority_type_code)

    # Base record (shared fields)
    base = {
        'source': 'ES_PLACE',
        'source_id': atom_id or folder_id,
        'reference_number': folder_id,
        'source_url': source_url,
        'contract_nature': nature,
        'is_concession': is_concession,
        'contract_type_original': contract_type_original,
        'contract_title': contract_title,
        'contract_duration': duration,
        'contracting_authority': authority_name,
        'authority_id': authority_nif,
        'authority_type': authority_type,
        'authority_dir3': authority_dir3,
        'nuts_code': nuts_code,
        'country': 'ES',
        'cpv_codes': cpv_codes if cpv_codes else None,
        'date_updated': updated,
        # Fields filled per result
        'lot_number': '0',
        'lot_title': None,
        'status': status,
        'estimated_value': estimated_value,
        'estimated_value_currency': currency,
        'base_budget': base_budget,
        'base_budget_currency': currency,
        'awardee': None,
        'awardee_id': None,
        'award_value': None,
        'award_value_with_tax': None,
        'award_value_currency': currency,
        'num_offers': None,
        'excluded_low_offers': None,
        'date_published': None,
        'date_awarded': None,
        'date_contract_start': None,
        'place_of_execution': None,
        'procedure_type': None,
        'notice_type': None,
        'eu_funded': None,
        'is_aggregated': None,
    }

    # Collect lot-level info
    lots = {}
    for lot_el in cfs.findall('cac:ProcurementProjectLot', NS):
        lot_id = text(lot_el, 'cbc:ID')
        lot_name = text(lot_el, 'cac:ProcurementProject/cbc:Name')
        if lot_id:
            lots[lot_id] = lot_name

    # Process tender results
    results = cfs.findall('cac:TenderResult', NS)

    if not results:
        # No results — single record for the tender
        if lots:
            # One record per lot
            records = []
            for lot_id, lot_name in lots.items():
                rec = dict(base)
                rec['lot_number'] = lot_id
                rec['lot_title'] = lot_name
                records.append(rec)
            return records
        return [base]

    records = []
    for tr in results:
        rec = dict(base)

        result_code = text(tr, 'cbc:ResultCode')
        if result_code:
            rec['status'] = RESULT_MAP.get(result_code, status)

        num_offers = text(tr, 'cbc:ReceivedTenderQuantity')
        if num_offers:
            rec['num_offers'] = int(num_offers)

        # Winner
        wp = tr.find('cac:WinningParty', NS)
        if wp is not None:
            rec['awardee'] = text(wp, 'cac:PartyName/cbc:Name')
            nif_el = wp.find('cac:PartyIdentification/cbc:ID', NS)
            if nif_el is not None:
                rec['awardee_id'] = nif_el.text

        # Contract date
        issue_date = text(tr, 'cac:Contract/cbc:IssueDate')
        if issue_date:
            rec['date_contract_start'] = issue_date

        # Awarded project (lot + amount)
        atp = tr.find('cac:AwardedTenderedProject', NS)
        if atp is not None:
            lot_id = text(atp, 'cbc:ID')
            if lot_id:
                rec['lot_number'] = lot_id
                rec['lot_title'] = lots.get(lot_id)

            lmt = atp.find('cac:LegalMonetaryTotal', NS)
            if lmt is not None:
                tea = text(lmt, 'cbc:TaxExclusiveAmount')
                if tea:
                    rec['award_value'] = float(tea)
                pa = text(lmt, 'cbc:PayableAmount')
                if pa:
                    rec['award_value_with_tax'] = float(pa)

        records.append(rec)

    return records


def parse_atom_file(content, date_from, date_to):
    """Parse an ATOM XML file and return records within the date range."""
    root = ET.fromstring(content)
    entries = root.findall('atom:entry', NS)
    records = []

    for entry in entries:
        updated = text(entry, 'atom:updated')
        if updated:
            entry_date = updated[:10]
            if entry_date < date_from or entry_date > date_to:
                continue

        entry_records = parse_entry(entry)
        records.extend(entry_records)

    return records


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


def upsert_batch(cursor, records):
    rows = [tuple(rec.get(col) for col in COLUMNS) for rec in records]
    psycopg2.extras.execute_batch(cursor, UPSERT_SQL, rows, page_size=1000)


def main():
    parser = argparse.ArgumentParser(description='Incremental sync of PLACE procurement data')
    parser.add_argument('--from', dest='date_from', required=True,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--to', dest='date_to', default=str(date.today()),
                        help='End date (YYYY-MM-DD), defaults to today')
    args = parser.parse_args()

    date_from = args.date_from
    date_to = args.date_to

    print(f"Syncing PLACE data updated between {date_from} and {date_to}")

    months = get_months(date_from, date_to)
    print(f"Months to download: {', '.join(months)}")

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False

    total_upserted = 0
    total_parsed = 0
    start_time = time.time()

    with tempfile.TemporaryDirectory() as tmpdir:
        for yyyymm in months:
            zip_path = download_month(yyyymm, tmpdir)
            if not zip_path:
                continue

            try:
                zf = zipfile.ZipFile(zip_path)
            except zipfile.BadZipFile:
                print(f"  Bad ZIP file, skipping.")
                continue

            atom_files = [f for f in zf.namelist() if f.endswith('.atom')]
            print(f"  {len(atom_files)} ATOM files in archive")

            month_records = 0
            for i, atom_file in enumerate(atom_files):
                content = zf.read(atom_file)
                try:
                    records = parse_atom_file(content, date_from, date_to)
                except ET.ParseError as e:
                    print(f"    XML parse error in {atom_file}: {e}")
                    continue

                total_parsed += len(records)

                if records:
                    with conn.cursor() as cur:
                        upsert_batch(cur, records)
                    conn.commit()
                    month_records += len(records)
                    total_upserted += len(records)

                if (i + 1) % 10 == 0 or i == len(atom_files) - 1:
                    print(f"    Processed {i+1}/{len(atom_files)} files, "
                          f"{month_records:,} records this month, "
                          f"{total_upserted:,} total")

            zf.close()
            os.remove(zip_path)

    conn.close()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Sync completed!")
    print(f"  Records parsed:   {total_parsed:,}")
    print(f"  Records upserted: {total_upserted:,}")
    print(f"  Duration:         {total_time/60:.1f} minutes")
    if total_time > 0:
        print(f"  Rate:             {total_upserted/total_time:.0f} records/second")


if __name__ == '__main__':
    main()
