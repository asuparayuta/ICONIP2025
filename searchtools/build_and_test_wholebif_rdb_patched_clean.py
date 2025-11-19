#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WholeBIF-RDB build & test script (integrated version)

This script automatically performs the following steps:
1. Connect to the PostgreSQL database
2. Create tables (project, references_tbl, circuits, connections, settings, changelog)
3. Read data from Google Spreadsheet
4. Insert data into the database (UPSERT)
5. Validate and test the database
6. Output results and export CSV files

Requirements:
- Python 3.6 or later
- pip install gspread oauth2client psycopg2-binary

Usage:
1. Edit the configuration section to match your environment
2. Run: python build_and_test_wholebif_rdb_patched_clean.py
"""

import os
import sys
import csv
import json
import psycopg2
from psycopg2.extras import DictCursor
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime
from typing import Dict, List, Optional

# =============================================================================
# Configuration section - please edit according to your environment
# =============================================================================

# Google Spreadsheet authentication
SERVICE_ACCOUNT_JSON = './wholebif-rdb-2ed0e13309cf.json'  # TODO: replace with your own credentials
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

# Google Spreadsheet key
SPREADSHEET_KEY = '1E22mAQftP9xf2lDWZ734l0teWgaUCcFCpjzf2Y8Sk30'  # TODO: replace with your own credentials

# PostgreSQL connection settings
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'wholebif_rdb'
DB_USER = 'wholebif'
DB_PASSWORD = 'Ashi12137'

# Output directory for test results and exported CSVs
OUTPUT_DIR = './test_results'

# =============================================================================
# Column name mapping dictionaries (Spreadsheet -> DB columns)
# =============================================================================

MAP_PROJECT = {
    "Project ID":            "project_id",
    "Contributor":           "contributor",
    "List of contributors":  "list_of_contributors",
    "Description":           "description",
    "BRA version":           "bra_version"
}

MAP_REFERENCES = {
    "Reference ID":   "reference_id",
    "Doc Link":       "doc_link",
    "BibTex link":    "bibtex_link",
    "DOI":            "doi",
    "BibTex ":        "bibtex",
    "Litterature type":"litterature_type",
    "Type":           "type",
    "Authors":        "authors",
    "Title":          "title",
    "Journal names":  "journal_names",
    "Alternative URL":"alternative_url",
    "Contributor":    "contributor",
    "Project ID":     "project_id",
    "Review results": "review_results",
    "Reviewer":       "reviewer"
}

MAP_CIRCUITS = {
    "Circuit ID":         "circuit_id",
    "Source of ID":       "source_of_id",
    "Names":              "names",
    "DHBA graph order":   "dhba_graph_order",
    "DHBA name":          "dhba_name",
    "Sub Circuits":       "sub_circuits",
    "Super Class":        "super_class",
    "Uniform":            "uniform",
    "Transmitter":        "transmitter",
    "Modulation Type":    "modulation_type",
    "Size":               "size",
    "Physiological Data": "physiological_data",
    "Comments":           "comments",
    "Contributor":        "contributor",
    "Project ID":         "project_id"
}

MAP_CONNECTIONS = {
    "Sender Circuit ID (sCID)":   "sender_circuit_id",
    "Receiver Circuit ID (rCID)": "receiver_circuit_id",
    "Sender Circuit ID":          "sender_circuit_id",
    "Receiver Circuit ID":        "receiver_circuit_id",
    "Reference ID":               "reference_id",
    "Taxon":                      "taxon",
    "Measurement method":         "measurement_method",
    "Pointers on Literature":     "pointers_on_literature",
    "Pointers on Figure":         "pointers_on_figure",
    "Credibility Rating":         "credibility_rating",
    "Summarized CR":              "summarized_cr",
    "Reviewer":                   "reviewer"
}

MAP_SETTINGS = {
    "WholeBIF file ID": "wholebif_file_id"
}

# Required columns and their default value generators
REQUIRED = {
    "project": {
        "project_id":  lambda r: r["project_id"],
        "contributor": lambda r: "(auto)",
        "bra_version": lambda r: "NA"
    },
    "references_tbl": {
        "reference_id": lambda r: r["reference_id"],
        "doi":    lambda r: "NO_DOI",
        "bibtex": lambda r: "NO_BIBTEX"
    },
    "circuits": {
        "circuit_id":   lambda r: r["circuit_id"],
        "source_of_id": lambda r: "UNKNOWN_SRC",
        "names":        lambda r: f"UNKNOWN_NAME_{r['circuit_id']}",
        "project_id":   lambda r: f"AUTO_PROJECT_{r['circuit_id']}"
    },
    "connections": {
        "sender_circuit_id":   lambda r: r["sender_circuit_id"],
        "receiver_circuit_id": lambda r: r["receiver_circuit_id"],
        "reference_id":        lambda r: r["reference_id"]
    },
    "settings": {
        "wholebif_file_id": lambda r: r["wholebif_file_id"]
    }
}

# =============================================================================
# Table creation SQL statements
# =============================================================================

CREATE_TABLES_SQL = [
    # ---------- project ----------
    """
    CREATE TABLE IF NOT EXISTS project (
        project_id            VARCHAR(255) PRIMARY KEY,
        contributor           VARCHAR(255) NOT NULL,
        list_of_contributors  TEXT,
        description           TEXT,
        bra_version           VARCHAR(50) NOT NULL
    );
    """,

    # ---------- references_tbl ----------
    """
    CREATE TABLE IF NOT EXISTS references_tbl (
        reference_id     VARCHAR(255) PRIMARY KEY,
        doc_link         VARCHAR(255),
        bibtex_link      VARCHAR(255),
        doi              VARCHAR(255) NOT NULL,
        bibtex           TEXT NOT NULL,
        litterature_type VARCHAR(50),
        type             VARCHAR(50),
        authors          TEXT,
        title            TEXT,
        journal_names    VARCHAR(255),
        alternative_url  TEXT,
        contributor      VARCHAR(255),
        project_id       VARCHAR(255),
        review_results   TEXT,
        reviewer         VARCHAR(255),
        CONSTRAINT fk_references_project
          FOREIGN KEY(project_id)
          REFERENCES project(project_id)
          ON UPDATE CASCADE
          ON DELETE SET NULL
    );
    """,

    # ---------- circuits ----------
    """
    CREATE TABLE IF NOT EXISTS circuits (
        circuit_id          VARCHAR(255) PRIMARY KEY,
        source_of_id        VARCHAR(255) NOT NULL,
        names               TEXT NOT NULL,
        dhba_graph_order    VARCHAR(50),
        dhba_name           VARCHAR(255),
        sub_circuits        TEXT,
        super_class         VARCHAR(255),
        uniform             BOOLEAN,
        transmitter         VARCHAR(255),
        modulation_type     VARCHAR(255),
        size                VARCHAR(50),
        physiological_data  TEXT,
        comments            TEXT,
        contributor         VARCHAR(255),
        project_id          VARCHAR(255) NOT NULL,
        CONSTRAINT fk_circuits_project
          FOREIGN KEY(project_id)
          REFERENCES project(project_id)
          ON UPDATE CASCADE
          ON DELETE SET NULL
    );
    """,

    # ---------- connections ----------
    """
    CREATE TABLE IF NOT EXISTS connections (
        sender_circuit_id    VARCHAR(255) NOT NULL,
        receiver_circuit_id  VARCHAR(255) NOT NULL,
        reference_id         VARCHAR(255) NOT NULL,
        taxon                VARCHAR(255),
        measurement_method   VARCHAR(255),
        pointers_on_literature TEXT,
        pointers_on_figure   TEXT,
        credibility_rating   FLOAT,
        summarized_cr        FLOAT,
        reviewer             VARCHAR(255),
        PRIMARY KEY (sender_circuit_id, receiver_circuit_id, reference_id),
        CONSTRAINT fk_connections_sender
          FOREIGN KEY(sender_circuit_id)
          REFERENCES circuits(circuit_id)
          ON UPDATE CASCADE
          ON DELETE CASCADE,
        CONSTRAINT fk_connections_receiver
          FOREIGN KEY(receiver_circuit_id)
          REFERENCES circuits(circuit_id)
          ON UPDATE CASCADE
          ON DELETE CASCADE,
        CONSTRAINT fk_connections_reference
          FOREIGN KEY(reference_id)
          REFERENCES references_tbl(reference_id)
          ON UPDATE CASCADE
          ON DELETE CASCADE
    );
    """,

    # ---------- settings ----------
    """
    CREATE TABLE IF NOT EXISTS settings (
        wholebif_file_id VARCHAR(255) PRIMARY KEY
    );
    """,

    # ---------- changelog ----------
    """
    CREATE TABLE IF NOT EXISTS changelog (
        changeid     SERIAL PRIMARY KEY,
        tablename    VARCHAR(50)  NOT NULL,
        recordid     VARCHAR(150) NOT NULL,
        changetype   VARCHAR(10)  NOT NULL,
        changedate   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        changedby    VARCHAR(100),
        changedetail TEXT
    );
    """
]

# =============================================================================
# Google Spreadsheet helper functions
# =============================================================================

def authorize_gspread():
    """Authenticate and return a gspread client"""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_JSON,
        scopes=SCOPES
    )
    gc = gspread.authorize(credentials)
    return gc

def get_spreadsheet(gc, spreadsheet_key):
    """Open and return a Spreadsheet object by key"""
    spreadsheet = gc.open_by_key(spreadsheet_key)
    return spreadsheet

def read_sheet(spreadsheet, sheet_name):
    """Read all values from a given worksheet"""
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    return data

# =============================================================================
# Database helper functions
# =============================================================================

def create_connection():
    """Create and return a connection to PostgreSQL"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    return conn

def create_tables(conn):
    """Create tables if they do not exist"""
    with conn.cursor() as cur:
        for sql in CREATE_TABLES_SQL:
            cur.execute(sql)
    conn.commit()

def log_change(conn, table_name, record_id, change_type, detail="", changed_by="system"):
    """Insert a record into the changelog table"""
    rid = (record_id or "")[:150]
    sql = """
      INSERT INTO changelog (
        tablename, recordid, changetype, changedate, changedby, changedetail
      ) VALUES (%s, %s, %s, NOW(), %s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table_name, rid, change_type, changed_by, detail[:1000]))
    conn.commit()

# =============================================================================
# Generic helper functions
# =============================================================================

def map_row(raw: dict, mapper: dict) -> dict:
    """Map a raw row using a mapping dict and normalize empty strings to None"""
    out = {}
    for k, v in raw.items():
        if k in mapper:
            out[mapper[k]] = v if v not in ("", None) else None
    for required in mapper.values():
        out.setdefault(required, None)
    return out

def is_blank_row(values: list) -> bool:
    """Return True if all values in a row are empty"""
    return all((v is None) or (str(v).strip() == "") for v in values)

def circuit_exists(conn, cid: str) -> bool:
    """Check whether a given circuit_id exists in circuits"""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM circuits WHERE circuit_id = %s;", (cid,))
        return bool(cur.fetchone())

# =============================================================================
# UPSERT helper functions
# =============================================================================

def insert_project(conn, rec):
    sql = """
      INSERT INTO project (
        project_id, contributor, list_of_contributors,
        description, bra_version
      ) VALUES (%(project_id)s, %(contributor)s, %(list_of_contributors)s,
                %(description)s, %(bra_version)s)
      ON CONFLICT (project_id) DO UPDATE SET
        contributor          = EXCLUDED.contributor,
        list_of_contributors = EXCLUDED.list_of_contributors,
        description          = EXCLUDED.description,
        bra_version          = EXCLUDED.bra_version;
    """
    with conn.cursor() as cur:
        cur.execute(sql, rec)
    log_change(conn, "project", rec["project_id"], "UPSERT", "project upsert")

def insert_references(conn, rec):
    # defensive: ensure project exists when project_id is provided
    if rec.get("project_id"):
        ensure_project_exists(conn, rec["project_id"])
    sql = """
      INSERT INTO references_tbl (
        reference_id, doc_link, bibtex_link, doi, bibtex,
        litterature_type, type, authors, title, journal_names,
        alternative_url, contributor, project_id,
        review_results, reviewer
      ) VALUES (
        %(reference_id)s, %(doc_link)s, %(bibtex_link)s, %(doi)s, %(bibtex)s,
        %(litterature_type)s, %(type)s, %(authors)s, %(title)s, %(journal_names)s,
        %(alternative_url)s, %(contributor)s, %(project_id)s,
        %(review_results)s, %(reviewer)s
      )
      ON CONFLICT (reference_id) DO UPDATE SET
        doc_link         = EXCLUDED.doc_link,
        bibtex_link      = EXCLUDED.bibtex_link,
        doi              = EXCLUDED.doi,
        bibtex           = EXCLUDED.bibtex,
        litterature_type = EXCLUDED.litterature_type,
        type             = EXCLUDED.type,
        authors          = EXCLUDED.authors,
        title            = EXCLUDED.title,
        journal_names    = EXCLUDED.journal_names,
        alternative_url  = EXCLUDED.alternative_url,
        contributor      = EXCLUDED.contributor,
        project_id       = EXCLUDED.project_id,
        review_results   = EXCLUDED.review_results,
        reviewer         = EXCLUDED.reviewer;
    """
    with conn.cursor() as cur:
        cur.execute(sql, rec)
    log_change(conn, "references_tbl", rec["reference_id"], "UPSERT", "references upsert")

def insert_circuits(conn, rec):
    sql = """
      INSERT INTO circuits (
        circuit_id, source_of_id, names, dhba_graph_order, dhba_name,
        sub_circuits, super_class, uniform, transmitter, modulation_type,
        size, physiological_data, comments, contributor, project_id
      ) VALUES (
        %(circuit_id)s, %(source_of_id)s, %(names)s, %(dhba_graph_order)s, %(dhba_name)s,
        %(sub_circuits)s, %(super_class)s, %(uniform)s, %(transmitter)s, %(modulation_type)s,
        %(size)s, %(physiological_data)s, %(comments)s, %(contributor)s, %(project_id)s
      )
      ON CONFLICT (circuit_id) DO UPDATE SET
        source_of_id        = EXCLUDED.source_of_id,
        names               = EXCLUDED.names,
        dhba_graph_order    = EXCLUDED.dhba_graph_order,
        dhba_name           = EXCLUDED.dhba_name,
        sub_circuits        = EXCLUDED.sub_circuits,
        super_class         = EXCLUDED.super_class,
        uniform             = EXCLUDED.uniform,
        transmitter         = EXCLUDED.transmitter,
        modulation_type     = EXCLUDED.modulation_type,
        size                = EXCLUDED.size,
        physiological_data  = EXCLUDED.physiological_data,
        comments            = EXCLUDED.comments,
        contributor         = EXCLUDED.contributor,
        project_id          = EXCLUDED.project_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, rec)
    log_change(conn, "circuits", rec["circuit_id"], "UPSERT", "circuits upsert")

def insert_connections(conn, rec):
    sql = """
      INSERT INTO connections (
        sender_circuit_id, receiver_circuit_id, reference_id,
        taxon, measurement_method, pointers_on_literature, pointers_on_figure,
        credibility_rating, summarized_cr, reviewer
      ) VALUES (
        %(sender_circuit_id)s, %(receiver_circuit_id)s, %(reference_id)s,
        %(taxon)s, %(measurement_method)s, %(pointers_on_literature)s,
        %(pointers_on_figure)s, %(credibility_rating)s, %(summarized_cr)s, %(reviewer)s
      )
      ON CONFLICT (sender_circuit_id, receiver_circuit_id, reference_id) DO UPDATE SET
        taxon                 = EXCLUDED.taxon,
        measurement_method    = EXCLUDED.measurement_method,
        pointers_on_literature= EXCLUDED.pointers_on_literature,
        pointers_on_figure    = EXCLUDED.pointers_on_figure,
        credibility_rating    = EXCLUDED.credibility_rating,
        summarized_cr         = EXCLUDED.summarized_cr,
        reviewer              = EXCLUDED.reviewer;
    """
    with conn.cursor() as cur:
        cur.execute(sql, rec)
    comp_key = f"{rec['sender_circuit_id']}_{rec['receiver_circuit_id']}_{rec['reference_id']}"
    log_change(conn, "connections", comp_key, "UPSERT", "connections upsert")

def insert_settings(conn, rec):
    sql = """
      INSERT INTO settings (wholebif_file_id)
      VALUES (%(wholebif_file_id)s)
      ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, rec)
    log_change(conn, "settings", rec["wholebif_file_id"], "INSERT", "settings insert")

# =============================================================================
# Data completion utilities
# =============================================================================

def ensure_project_exists(conn, pid: str):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM project WHERE project_id = %s;", (pid,))
        if cur.fetchone():
            return
    insert_project(conn, {
        "project_id":            pid,
        "contributor":           "(auto)",
        "list_of_contributors":  None,
        "description":           "(auto-insert)",
        "bra_version":           "NA"
    })

def ensure_circuit_exists(conn, cid: str):
    """Create a minimal circuits record if the ID does not exist"""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM circuits WHERE circuit_id = %s;", (cid,))
        if cur.fetchone():
            return

    auto_pid = f"AUTO_PROJECT_{cid}"
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM project WHERE project_id = %s;", (auto_pid,))
        if not cur.fetchone():
            insert_project(conn, {
                "project_id":           auto_pid,
                "contributor":          "(auto)",
                "list_of_contributors": None,
                "description":          "(auto-insert)",
                "bra_version":          "NA"
            })

    insert_circuits(conn, {
        "circuit_id":         cid,
        "source_of_id":       "AUTO_SRC",
        "names":              f"auto_{cid}",
        "dhba_graph_order":   None,
        "dhba_name":          None,
        "sub_circuits":       None,
        "super_class":        None,
        "uniform":            None,
        "transmitter":        None,
        "modulation_type":    None,
        "size":               None,
        "physiological_data": None,
        "comments":           None,
        "contributor":        "(auto)",
        "project_id":         auto_pid
    })

def ensure_reference_exists(conn, ref_id: str):
    """Create a minimal references_tbl record if the reference_id does not exist"""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM references_tbl WHERE reference_id = %s;", (ref_id,))
        if cur.fetchone():
            return

    insert_references(conn, {
        "reference_id":     ref_id,
        "doc_link":         None,
        "bibtex_link":      None,
        "doi":              "NO_DOI",
        "bibtex":           "NO_BIBTEX",
        "litterature_type": None,
        "type":             None,
        "authors":          None,
        "title":            f"auto {ref_id}",
        "journal_names":    None,
        "alternative_url":  None,
        "contributor":      "(auto)",
        "project_id":       None,
        "review_results":   None,
        "reviewer":         None
    })

def fill_required(table: str, rec: dict, conn):
    """Fill required columns with default values and ensure referential integrity"""
    if table in REQUIRED:
        for col, fn in REQUIRED[table].items():
            if rec.get(col) in (None, "", "NA", "(no DOI)", "(no BibTex)"):
                rec[col] = fn(rec)

    # Check that references_tbl.project_id points to an existing project
    if table == "references_tbl" and rec.get("project_id"):
        ensure_project_exists(conn, rec["project_id"])

    if table == "circuits" and rec.get("project_id"):
        ensure_project_exists(conn, rec["project_id"])

    if table == "connections":
        for col in ("sender_circuit_id", "receiver_circuit_id"):
            cid = rec.get(col)
            if cid:
                ensure_circuit_exists(conn, cid)

        ref_id = rec.get("reference_id")
        if ref_id:
            ensure_reference_exists(conn, ref_id)
    return rec

# =============================================================================
# Testing and validation functions
# =============================================================================

def run_tests(conn):
    """Run database validation, integrity checks, and CSV exports"""
    print("\n" + "="*80)
    print("Database validation and tests")
    print("="*80)
    
    results = {}
    
    # Create output directory if it does not exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Count records in each table
    print("\n1. Record counts per table:")
    print("-" * 60)
    tables = ['project', 'references_tbl', 'circuits', 'connections', 'settings', 'changelog']
    
    for table in tables:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            results[f"{table}_count"] = count
            print(f"  {table:20s}: {count:6d} rows")
    
    # 2. Show sample rows for each main table
    print("\n2. Sample data preview:")
    print("-" * 60)
    
    # Project
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM project LIMIT 5;")
        projects = cur.fetchall()
        print(f"\n  [Project] First 5 rows:")
        for i, p in enumerate(projects, 1):
            print(f"    {i}. {p['project_id']} - {p['contributor']}")
    
    # References
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT reference_id, title, doi FROM references_tbl LIMIT 5;")
        refs = cur.fetchall()
        print(f"\n  [References] First 5 rows:")
        for i, r in enumerate(refs, 1):
            title = (r['title'] or '')[:50]
            print(f"    {i}. {r['reference_id']} - {title}...")
    
    # Circuits
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT circuit_id, names FROM circuits LIMIT 5;")
        circuits = cur.fetchall()
        print(f"\n  [Circuits] First 5 rows:")
        for i, c in enumerate(circuits, 1):
            print(f"    {i}. {c['circuit_id']} - {c['names']}")
    
    # Connections
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT sender_circuit_id, receiver_circuit_id, reference_id 
            FROM connections LIMIT 5;
        """)
        conns = cur.fetchall()
        print(f"\n  [Connections] First 5 rows:")
        for i, c in enumerate(conns, 1):
            print(f"    {i}. {c['sender_circuit_id']} -> {c['receiver_circuit_id']} (ref: {c['reference_id']})")
    
    # 3. Data integrity checks for foreign-key relations
    print("\n3. Data integrity checks:")
    print("-" * 60)
    
    # Check foreign-key-related consistency
    checks = [
        ("References with invalid project_id", 
         "SELECT COUNT(*) FROM references_tbl WHERE project_id IS NOT NULL AND project_id NOT IN (SELECT project_id FROM project);"),
        
        ("Circuits with invalid project_id", 
         "SELECT COUNT(*) FROM circuits WHERE project_id NOT IN (SELECT project_id FROM project);"),
        
        ("Connections with invalid sender", 
         "SELECT COUNT(*) FROM connections WHERE sender_circuit_id NOT IN (SELECT circuit_id FROM circuits);"),
        
        ("Connections with invalid receiver", 
         "SELECT COUNT(*) FROM connections WHERE receiver_circuit_id NOT IN (SELECT circuit_id FROM circuits);"),
        
        ("Connections with invalid reference", 
         "SELECT COUNT(*) FROM connections WHERE reference_id NOT IN (SELECT reference_id FROM references_tbl);")
    ]
    
    integrity_ok = True
    for check_name, sql in checks:
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.fetchone()[0]
            status = "✅ OK" if count == 0 else f"⚠️  WARNING: {count} rows"
            print(f"  {check_name:40s}: {status}")
            if count > 0:
                integrity_ok = False
    
    # 4. Export data to CSV
    print("\n4. Export data to CSV files:")
    print("-" * 60)
    
    export_tables = {
        'project': 'SELECT * FROM project;',
        'references_tbl': 'SELECT * FROM references_tbl;',
        'circuits': 'SELECT * FROM circuits;',
        'connections': 'SELECT * FROM connections;',
        'changelog': 'SELECT * FROM changelog ORDER BY changedate DESC LIMIT 100;'
    }
    
    
    for table, sql in export_tables.items():
        csv_path = os.path.join(OUTPUT_DIR, f"{table}.csv")
        try:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                
                if rows:
                    # Safely obtain column names from cursor.description
                    cols = [desc.name for desc in cur.description]

                    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
                        writer.writeheader()
                        
                        # Write each row, filling missing columns with empty strings
                        for row in rows:
                            rd = dict(row)
                            writer.writerow({c: rd.get(c, "") for c in cols})
                    print(f"  ✅ {table:20s} -> {csv_path}")
                else:
                    print(f"  ⚠️  {table:20s} (no data)")
        except Exception as e:
            print(f"  ❌ {table:20s} ERROR: {e}")
    # 5. Summary report
    print("\n" + "="*80)
    print("Test result summary")
    print("="*80)
    
    total_records = sum(results.values())
    print(f"\nTotal number of records: {total_records:,}")
    print(f"Data integrity: {'✅ OK' if integrity_ok else '⚠️ WARNING'}")
    print(f"CSV output directory: {OUTPUT_DIR}/")
    
    # Write summary information to JSON
    summary_path = os.path.join(OUTPUT_DIR, 'test_summary.json')
    summary = {
        'timestamp': datetime.now().isoformat(),
        'record_counts': results,
        'total_records': total_records,
        'integrity_check': 'OK' if integrity_ok else 'WARNING',
        'output_directory': OUTPUT_DIR
    }
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"Summary report: {summary_path}")
    print("\n✅ Tests finished!")
    
    return integrity_ok

# =============================================================================
# Main process
# =============================================================================

def main():
    """Main entry point for building and testing WholeBIF-RDB"""
    print("="*80)
    print("WholeBIF-RDB build & test script")
    print("="*80)
    
    try:
        # 1. Google Spreadsheet authentication
        print("\n[1/6] Authenticating with Google Spreadsheet...")
        gc = authorize_gspread()
        ss = get_spreadsheet(gc, SPREADSHEET_KEY)
        print("  ✅ Authentication succeeded")
        
        # 2. PostgreSQL connection
        print("\n[2/6] Connecting to PostgreSQL...")
        conn = create_connection()
        print(f"  ✅ Connected: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # 3. Create tables
        print("\n[3/6] Creating tables...")
        create_tables(conn)
        print("  ✅ Tables created")
        
        # 4. Load data from Google Spreadsheet
        print("\n[4/6] Reading data from Google Spreadsheet...")
        proj_sheet = read_sheet(ss, "Project")
        ref_sheet  = read_sheet(ss, "References")
        cir_sheet  = read_sheet(ss, "Circuits")
        con_sheet  = read_sheet(ss, "Connections")
        set_sheet  = read_sheet(ss, "Settings")
        print(f"  ✅ Data loaded successfully")
        print(f"     Project: {len(proj_sheet)-1} rows")
        print(f"     References: {len(ref_sheet)-1} rows")
        print(f"     Circuits: {len(cir_sheet)-1} rows")
        print(f"     Connections: {len(con_sheet)-1} rows")
        print(f"     Settings: {len(set_sheet)-1} rows")
        
        # 5. Insert data into the database
        print("\n[5/6] Inserting data into the database...")
        
        proj_header = proj_sheet[0]
        ref_header  = ref_sheet[0]
        cir_header  = cir_sheet[0]
        con_header  = con_sheet[0]
        set_header  = set_sheet[0]
        
        # Project
        proj_count = 0
        for row in proj_sheet[1:]:
            if is_blank_row(row):
                continue
            rec = map_row(dict(zip(proj_header, row)), MAP_PROJECT)
            rec = fill_required("project", rec, conn)
            if rec.get("project_id"):
                insert_project(conn, rec)
                proj_count += 1
        print(f"  ✅ Project: inserted {proj_count} rows")
        
        # References
        ref_count = 0
        for row in ref_sheet[1:]:
            if is_blank_row(row):
                continue
            rec = map_row(dict(zip(ref_header, row)), MAP_REFERENCES)
            rec = fill_required("references_tbl", rec, conn)
            if rec.get("reference_id"):
                insert_references(conn, rec)
                ref_count += 1
        print(f"  ✅ References: inserted {ref_count} rows")
        
        # Circuits
        cir_count = 0
        for row in cir_sheet[1:]:
            if is_blank_row(row):
                continue
            rec = map_row(dict(zip(cir_header, row)), MAP_CIRCUITS)
            rec = fill_required("circuits", rec, conn)
            if rec.get("circuit_id"):
                insert_circuits(conn, rec)
                cir_count += 1
        print(f"  ✅ Circuits: inserted {cir_count} rows")
        
        # Connections
        con_count = 0
        for row in con_sheet[1:]:
            if is_blank_row(row):
                continue
            rec = map_row(dict(zip(con_header, row)), MAP_CONNECTIONS)
            if not (rec.get("sender_circuit_id") and
                    rec.get("receiver_circuit_id") and
                    rec.get("reference_id")):
                continue
            rec = fill_required("connections", rec, conn)
            if rec.pop("_skip", False):
                continue
            insert_connections(conn, rec)
            con_count += 1
        print(f"  ✅ Connections: inserted {con_count} rows")
        
        # Settings
        set_count = 0
        for row in set_sheet[1:]:
            rec = map_row(dict(zip(set_header, row)), MAP_SETTINGS)
            rec = fill_required("settings", rec, conn)
            if rec.get("wholebif_file_id"):
                insert_settings(conn, rec)
                set_count += 1
        print(f"  ✅ Settings: inserted {set_count} rows")
        
        # Commit all pending transactions
        conn.commit()
        print("\n  ✅ Finished inserting all data")
        
        # 6. Run tests
        print("\n[6/6] Running database validation and tests...")
        integrity_ok = run_tests(conn)
        
        # Done
        conn.close()
        
        print("\n" + "="*80)
        print("✅ WholeBIF-RDB build completed!")
        print("="*80)
        
        if not integrity_ok:
            print("\n⚠️  Note: There are warnings in data integrity. Please check the test_results/ directory.")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    main()