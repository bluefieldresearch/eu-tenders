#!/usr/bin/env python3
"""
Load French water/wastewater operator data from SISPEA extracts into
the assets & assets_operators tables.

Reads AEP (drinking water) and AC (wastewater) XLS files from resources/,
maps them to water_network and sewer_network asset types, and upserts
into the database.

For commune-type entities, links to existing localities via INSEE code.
For EPCI/syndicat entities, stores with locality_id = NULL and entity
info in metadata for future enrichment.

Usage:
    python3 source/fr_sispea.py
"""

import json
import os
import sys
import zipfile
import tempfile

import xlrd
import psycopg2
import psycopg2.extras

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'tenders',
    'user': 'tenders',
    'password': 'tenders'
}

RESOURCES_DIR = os.path.join(os.path.dirname(__file__), '..', 'resources')

# SISPEA files and their asset type mapping
SOURCES = [
    {
        'zip': 'SISPEA_FR_2026_AEP.zip',
        'xls': 'SISPEA_FR_2026_AEP.xls',
        'asset_type': 'water_network',
        'sheet': 'Entités de gestion',
        # AEP has Code UGE at col 12, shifting subsequent columns
        'cols': {
            'dept': 0,
            'sispea_id': 1,
            'collectivite': 2,
            'type_collectivite': 3,
            'siren': 4,
            'insee': 5,
            'nb_communes': 6,
            'entity_name': 13,
            'nb_communes_entity': 14,
            'pop': 17,
            'mode_gestion': 22,
            'statut_operateur': 23,
            'nom_operateur': 24,
            'date_debut': 25,
            'date_fin': 26,
        },
    },
    {
        'zip': 'SISPEA_FR_2026_AC.zip',
        'xls': 'SISPEA_FR_2026_AC.xls',
        'asset_type': 'sewer_network',
        'sheet': 'Entités de gestion',
        # AC lacks Code UGE, columns shift by -1 after col 11
        'cols': {
            'dept': 0,
            'sispea_id': 1,
            'collectivite': 2,
            'type_collectivite': 3,
            'siren': 4,
            'insee': 5,
            'nb_communes': 6,
            'entity_name': 12,
            'nb_communes_entity': 13,
            'pop': 16,
            'mode_gestion': 21,
            'statut_operateur': 22,
            'nom_operateur': 23,
            'date_debut': 24,
            'date_fin': 25,
        },
    },
]

# Management type mapping
MANAGEMENT_TYPE_MAP = {
    'Régie': 'public',
    'Délégation': 'private',
}


def xldate_to_str(xldate, datemode):
    """Convert Excel date number to ISO date string, or return None."""
    if not xldate:
        return None
    try:
        dt = xlrd.xldate_as_datetime(xldate, datemode)
        return dt.strftime('%Y-%m-%d')
    except (ValueError, OverflowError):
        return None


def load_source(cur, source):
    """Load one SISPEA file (AEP or AC) into the database."""
    zip_path = os.path.join(RESOURCES_DIR, source['zip'])
    cols = source['cols']
    asset_type = source['asset_type']

    # Extract XLS from ZIP
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extract(source['xls'], tmpdir)
        xls_path = os.path.join(tmpdir, source['xls'])

        wb = xlrd.open_workbook(xls_path)
        ws = wb.sheet_by_name(source['sheet'])
        datemode = wb.datemode

    # Build INSEE -> locality_id lookup for French communes
    cur.execute(
        "SELECT municipality_code, id FROM localities "
        "WHERE country = 'FR' AND municipality_code IS NOT NULL"
    )
    locality_map = {row[0]: row[1] for row in cur.fetchall()}

    inserted_assets = 0
    inserted_operators = 0
    skipped_no_locality = 0

    for r in range(1, ws.nrows):
        val = lambda c: ws.cell_value(r, c)

        collectivite = str(val(cols['collectivite'])).strip()
        type_coll = str(val(cols['type_collectivite'])).strip()
        siren = str(val(cols['siren'])).strip()
        insee = str(val(cols['insee'])).strip()
        sispea_id = str(val(cols['sispea_id'])).strip()
        entity_name = str(val(cols['entity_name'])).strip()
        nb_communes = val(cols['nb_communes'])
        nb_communes_entity = val(cols['nb_communes_entity'])
        pop = val(cols['pop'])
        mode_gestion = str(val(cols['mode_gestion'])).strip()
        statut_operateur = str(val(cols['statut_operateur'])).strip()
        nom_operateur = str(val(cols['nom_operateur'])).strip()
        date_debut = xldate_to_str(val(cols['date_debut']), datemode)
        date_fin = xldate_to_str(val(cols['date_fin']), datemode)

        # Determine locality_id
        locality_id = None
        if type_coll == 'Commune' and insee:
            locality_id = locality_map.get(insee)

        if locality_id is None and type_coll == 'Commune':
            # Commune not found in our localities table — skip
            continue

        # Management type
        management_type = MANAGEMENT_TYPE_MAP.get(mode_gestion)

        # Contract type: use statut_operateur
        contract_type = statut_operateur if statut_operateur else None

        # Operator: for public management, use collectivite name if no operator
        operator = nom_operateur if nom_operateur else None
        if not operator and management_type == 'public':
            operator = collectivite

        # CA is the collectivité
        ca = collectivite

        # Build metadata
        metadata = {
            'sispea_id': sispea_id,
            'siren': siren,
            'type_collectivite': type_coll,
            'entity_name': entity_name,
        }
        if nb_communes:
            metadata['nb_communes'] = int(nb_communes)
        if nb_communes_entity:
            metadata['nb_communes_entity'] = int(nb_communes_entity)
        if pop:
            metadata['population_served'] = int(pop)
        if mode_gestion:
            metadata['mode_gestion'] = mode_gestion
        metadata = {k: v for k, v in metadata.items() if v}

        # Upsert asset
        if locality_id:
            # Commune-level: check if asset already exists
            cur.execute("""
                SELECT id FROM assets
                WHERE locality_id = %s AND asset_type = %s
            """, (locality_id, asset_type))
            row = cur.fetchone()
            if row:
                asset_id = row[0]
                cur.execute("""
                    UPDATE assets SET ca = %s, metadata = %s
                    WHERE id = %s
                """, (ca, json.dumps(metadata), asset_id))
            else:
                cur.execute("""
                    INSERT INTO assets (locality_id, asset_type, ca, metadata)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (locality_id, asset_type, ca, json.dumps(metadata)))
                asset_id = cur.fetchone()[0]
            inserted_assets += 1
        else:
            # EPCI/syndicat: no locality, store with NULL locality_id
            cur.execute("""
                INSERT INTO assets (locality_id, asset_type, ca, metadata)
                VALUES (NULL, %s, %s, %s)
                RETURNING id
            """, (asset_type, ca, json.dumps(metadata)))
            asset_id = cur.fetchone()[0]
            inserted_assets += 1
            skipped_no_locality += 1

        # Insert operator
        cur.execute("""
            INSERT INTO assets_operators
                (asset_id, operator, management_type, contract_type, start_date, end_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (asset_id, operator, management_type, contract_type, date_debut, date_fin))
        inserted_operators += 1

    return inserted_assets, inserted_operators, skipped_no_locality


def main():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        for source in SOURCES:
            print(f"Loading {source['xls']} -> {source['asset_type']}...")
            assets, operators, no_loc = load_source(cur, source)
            print(f"  Assets: {assets}, Operators: {operators}, No locality (EPCI): {no_loc}")

        conn.commit()
        print("Done.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
