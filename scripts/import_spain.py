#!/usr/bin/env python3
"""
Import Spanish tender data from Excel files into PostgreSQL database.
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import argparse

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'licitaciones',
    'user': 'postgres',
    'password': 'postgres'  # Change this!
}

# Excel files to process (default: all available years)
ALL_YEARS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

def parse_cpv_codes(cpv_string):
    """Parse semicolon-separated CPV codes into a list."""
    if pd.isna(cpv_string) or not cpv_string:
        return []
    # Split by semicolon and filter out empty strings
    codes = [c.strip() for c in str(cpv_string).split(';') if c.strip()]
    return codes

def clean_decimal(value):
    """Clean decimal values, handling NaN."""
    if pd.isna(value):
        return None
    return float(value)

def clean_int(value):
    """Clean integer values, handling NaN."""
    if pd.isna(value):
        return None
    return int(value)

def clean_string(value, max_length=None):
    """Clean string values, handling NaN and max length."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if max_length and len(s) > max_length:
        s = s[:max_length]
    return s if s else None

def clean_timestamp(value):
    """Clean timestamp values."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except:
            return None
    return value

def clean_date(value):
    """Clean date values."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).date()
        except:
            return None
    if hasattr(value, 'date'):
        return value.date()
    return value

def clean_boolean(value):
    """Clean boolean values from various representations."""
    if pd.isna(value):
        return None
    s = str(value).lower().strip()
    if s in ('sí', 'si', 'yes', 'true', '1', 's'):
        return True
    if s in ('no', 'false', '0', 'n'):
        return False
    return None

def connect_db():
    """Connect to PostgreSQL database."""
    return psycopg2.connect(**DB_CONFIG)

def create_schema(conn):
    """Create database schema from SQL file."""
    schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    print("Schema created successfully.")

def import_licitaciones(conn, df, year, batch_size=5000):
    """Import licitaciones data and return mapping of identificador to id."""
    print(f"  Importing {len(df)} licitaciones...")

    licitaciones_data = []
    cpv_data = []  # Will hold (row_index, cpv_code, position)

    for idx, row in df.iterrows():
        licitacion = (
            clean_int(row['Identificador']),
            clean_string(row['Link licitación']),
            clean_timestamp(row['Fecha actualización']),
            clean_string(row['Vigente/Anulada/Archivada'], 50),
            clean_string(row['Número de expediente'], 100),
            clean_string(row['Objeto del Contrato']),
            clean_decimal(row['Valor estimado del contrato']),
            clean_decimal(row['Presupuesto base sin impuestos']),
            clean_string(row['Tipo de contrato'], 100),
            clean_string(row['Lugar de ejecución'], 200),
            clean_string(row['Órgano de Contratación']),
            clean_string(row['NIF OC'], 20),
            clean_string(row['DIR3'], 50),
            clean_string(row['Tipo de Administración']),
            year
        )
        licitaciones_data.append(licitacion)

        # Parse CPV codes
        cpv_codes = parse_cpv_codes(row['CPV'])
        for pos, code in enumerate(cpv_codes, 1):
            cpv_data.append((len(licitaciones_data) - 1, code, pos))

    # Insert licitaciones in batches and get IDs
    inserted_ids = []
    with conn.cursor() as cur:
        for i in range(0, len(licitaciones_data), batch_size):
            batch = licitaciones_data[i:i+batch_size]
            result = execute_values(
                cur,
                """
                INSERT INTO licitaciones (
                    identificador, link_licitacion, fecha_actualizacion,
                    estado, numero_expediente, objeto_contrato,
                    valor_estimado, presupuesto_base_sin_impuestos,
                    tipo_contrato, lugar_ejecucion, organo_contratacion,
                    nif_oc, dir3, tipo_administracion, year_source
                ) VALUES %s
                ON CONFLICT (identificador, year_source) DO UPDATE SET
                    link_licitacion = EXCLUDED.link_licitacion,
                    fecha_actualizacion = EXCLUDED.fecha_actualizacion,
                    estado = EXCLUDED.estado,
                    objeto_contrato = EXCLUDED.objeto_contrato,
                    valor_estimado = EXCLUDED.valor_estimado,
                    presupuesto_base_sin_impuestos = EXCLUDED.presupuesto_base_sin_impuestos,
                    tipo_contrato = EXCLUDED.tipo_contrato,
                    lugar_ejecucion = EXCLUDED.lugar_ejecucion,
                    organo_contratacion = EXCLUDED.organo_contratacion,
                    nif_oc = EXCLUDED.nif_oc,
                    dir3 = EXCLUDED.dir3,
                    tipo_administracion = EXCLUDED.tipo_administracion
                RETURNING id
                """,
                batch,
                fetch=True
            )
            inserted_ids.extend([r[0] for r in result])
            print(f"    Inserted batch {i//batch_size + 1}/{(len(licitaciones_data) + batch_size - 1)//batch_size}")

    conn.commit()

    # Insert CPV codes
    if cpv_data:
        print(f"  Importing {len(cpv_data)} CPV codes for licitaciones...")
        cpv_records = [(inserted_ids[row_idx], code, pos) for row_idx, code, pos in cpv_data]

        with conn.cursor() as cur:
            for i in range(0, len(cpv_records), batch_size * 3):
                batch = cpv_records[i:i+batch_size*3]
                execute_values(
                    cur,
                    "INSERT INTO licitaciones_cpv (licitacion_id, cpv_code, position) VALUES %s",
                    batch
                )
        conn.commit()

    # Return mapping of (identificador, year) -> id
    return {(licitaciones_data[i][0], year): inserted_ids[i] for i in range(len(inserted_ids))}

def import_resultados(conn, df, year, batch_size=5000):
    """Import resultados data."""
    print(f"  Importing {len(df)} resultados...")

    resultados_data = []
    cpv_data = []

    for idx, row in df.iterrows():
        resultado = (
            clean_int(row['Identificador']),
            clean_string(row['Link licitación']),
            clean_timestamp(row['Fecha actualización']),
            clean_string(row['Número de expediente'], 100),
            clean_string(row['Lote'], 200),
            clean_string(row['Objeto licitación/lote']),
            clean_decimal(row['Valor estimado licitación/lote']),
            clean_decimal(row['Presupuesto base sin impuestos licitación/lote']),
            clean_int(row['Número de ofertas recibidas por licitación/lote']),
            clean_boolean(row['Se han excluído ofertas por ser anormalmente bajas por licitación/lote']),
            clean_date(row['Fecha entrada en vigor del contrato de licitación/lote']),
            clean_string(row['Adjudicatario licitación/lote']),
            clean_decimal(row['Importe adjudicación sin impuestos licitación/lote']),
            year
        )
        resultados_data.append(resultado)

        # Parse CPV codes
        cpv_codes = parse_cpv_codes(row['CPV licitación/lote'])
        for pos, code in enumerate(cpv_codes, 1):
            cpv_data.append((len(resultados_data) - 1, code, pos))

    # Insert resultados in batches
    inserted_ids = []
    with conn.cursor() as cur:
        for i in range(0, len(resultados_data), batch_size):
            batch = resultados_data[i:i+batch_size]
            result = execute_values(
                cur,
                """
                INSERT INTO resultados (
                    identificador, link_licitacion, fecha_actualizacion,
                    numero_expediente, lote, objeto_lote,
                    valor_estimado_lote, presupuesto_base_sin_impuestos_lote,
                    num_ofertas, ofertas_excluidas_bajas, fecha_entrada_vigor,
                    adjudicatario, importe_adjudicacion_sin_impuestos, year_source
                ) VALUES %s
                RETURNING id
                """,
                batch,
                fetch=True
            )
            inserted_ids.extend([r[0] for r in result])
            print(f"    Inserted batch {i//batch_size + 1}/{(len(resultados_data) + batch_size - 1)//batch_size}")

    conn.commit()

    # Insert CPV codes
    if cpv_data:
        print(f"  Importing {len(cpv_data)} CPV codes for resultados...")
        cpv_records = [(inserted_ids[row_idx], code, pos) for row_idx, code, pos in cpv_data]

        with conn.cursor() as cur:
            for i in range(0, len(cpv_records), batch_size * 3):
                batch = cpv_records[i:i+batch_size*3]
                execute_values(
                    cur,
                    "INSERT INTO resultados_cpv (resultado_id, cpv_code, position) VALUES %s",
                    batch
                )
        conn.commit()

def populate_cpv_codes(conn):
    """Populate cpv_codes table with unique codes found in the data."""
    print("Populating CPV codes reference table...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cpv_codes (code)
            SELECT DISTINCT cpv_code FROM (
                SELECT cpv_code FROM licitaciones_cpv
                UNION
                SELECT cpv_code FROM resultados_cpv
            ) all_codes
            ON CONFLICT (code) DO NOTHING
        """)
    conn.commit()

    # Get count
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cpv_codes")
        count = cur.fetchone()[0]
    print(f"  Found {count} unique CPV codes")

def print_stats(conn):
    """Print database statistics."""
    print("\n=== Database Statistics ===")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM licitaciones")
        print(f"Total licitaciones: {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM resultados")
        print(f"Total resultados: {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM licitaciones_cpv")
        print(f"Total CPV entries (licitaciones): {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM resultados_cpv")
        print(f"Total CPV entries (resultados): {cur.fetchone()[0]:,}")

        cur.execute("SELECT COUNT(*) FROM cpv_codes")
        print(f"Unique CPV codes: {cur.fetchone()[0]:,}")

        print("\nRecords per year:")
        cur.execute("""
            SELECT year_source, COUNT(*) as licitaciones,
                   (SELECT COUNT(*) FROM resultados WHERE year_source = l.year_source) as resultados
            FROM licitaciones l
            GROUP BY year_source
            ORDER BY year_source
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,} licitaciones, {row[2]:,} resultados")

def main():
    parser = argparse.ArgumentParser(description='Import tender data to PostgreSQL')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='licitaciones', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='postgres', help='Database password')
    parser.add_argument('--schema-only', action='store_true', help='Only create schema, do not import data')
    parser.add_argument('--no-schema', action='store_true', help='Skip schema creation (for adding new years)')
    parser.add_argument('--data-dir', default='.', help='Directory containing Excel files')
    parser.add_argument('--years', type=str, default=None,
                        help='Comma-separated years to import (e.g., "2017,2018,2019"). Default: all available')

    args = parser.parse_args()

    # Parse years argument
    if args.years:
        years_to_import = [int(y.strip()) for y in args.years.split(',')]
    else:
        years_to_import = ALL_YEARS

    # Update config from args
    DB_CONFIG.update({
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    })

    print("Connecting to database...")
    try:
        conn = connect_db()
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        print("\nMake sure PostgreSQL is running and the database exists.")
        print("You can create the database with:")
        print(f"  createdb {args.database}")
        print("Or in psql:")
        print(f"  CREATE DATABASE {args.database};")
        sys.exit(1)

    try:
        if not args.no_schema:
            print("Creating schema...")
            create_schema(conn)

        if args.schema_only:
            print("Schema created. Skipping data import.")
            return

        for year in years_to_import:
            filename = f"{year}.xlsx"
            filepath = os.path.join(args.data_dir, filename)
            if not os.path.exists(filepath):
                print(f"Warning: {filepath} not found, skipping...")
                continue

            print(f"\n=== Processing {filename} (year {year}) ===")

            # Read licitaciones
            print("Reading Licitaciones sheet...")
            df_lic = pd.read_excel(filepath, sheet_name='Licitaciones')
            import_licitaciones(conn, df_lic, year)

            # Read resultados
            print("Reading Resultados sheet...")
            df_res = pd.read_excel(filepath, sheet_name='Resultados')
            import_resultados(conn, df_res, year)

        # Populate CPV codes reference table
        populate_cpv_codes(conn)

        # Print statistics
        print_stats(conn)

        print("\nImport completed successfully!")

    finally:
        conn.close()

if __name__ == '__main__':
    main()
