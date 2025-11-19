#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WholeBIF RDB 構築 & テストスクリプト (統合版)

このスクリプトは以下を自動実行します:
1. PostgreSQLデータベースへの接続
2. テーブルの作成（project, references_tbl, circuits, connections, settings, changelog）
3. Google Spreadsheetからデータの読み込み
4. データベースへのデータ投入（UPSERT）
5. データベースの検証とテスト
6. 結果の出力とCSVエクスポート

必要な環境:
- Python 3.6以上
- pip install gspread oauth2client psycopg2-binary

使い方:
1. 設定セクションを環境に合わせて編集
2. python build_and_test_wholebif_rdb.py を実行
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
# 設定セクション - 必ず環境に合わせて編集してください
# =============================================================================

# Google Spreadsheet 認証
SERVICE_ACCOUNT_JSON = './wholebif-rdb-2ed0e13309cf.json'  # 要修正
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

# Google Spreadsheet のキー
SPREADSHEET_KEY = '1E22mAQftP9xf2lDWZ734l0teWgaUCcFCpjzf2Y8Sk30'  # 要修正

# PostgreSQL 接続情報
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'wholebif_rdb'
DB_USER = 'wholebif'
DB_PASSWORD = 'Ashi12137'

# テスト結果の出力ディレクトリ
OUTPUT_DIR = './test_results'

# =============================================================================
# カラム名マッピング辞書
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

# 必須カラムと既定値
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
# テーブル作成 SQL
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
# Google Spreadsheet 関数
# =============================================================================

def authorize_gspread():
    """Google Spreadsheet認証"""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_JSON,
        scopes=SCOPES
    )
    gc = gspread.authorize(credentials)
    return gc

def get_spreadsheet(gc, spreadsheet_key):
    """スプレッドシート取得"""
    spreadsheet = gc.open_by_key(spreadsheet_key)
    return spreadsheet

def read_sheet(spreadsheet, sheet_name):
    """シートのデータ読み込み"""
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    return data

# =============================================================================
# データベース関数
# =============================================================================

def create_connection():
    """PostgreSQL接続"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    return conn

def create_tables(conn):
    """テーブル作成"""
    with conn.cursor() as cur:
        for sql in CREATE_TABLES_SQL:
            cur.execute(sql)
    conn.commit()

def log_change(conn, table_name, record_id, change_type, detail="", changed_by="system"):
    """changelogに履歴記録"""
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
# ヘルパー関数
# =============================================================================

def map_row(raw: dict, mapper: dict) -> dict:
    """マッピング変換 + 空文字をNoneへ"""
    out = {}
    for k, v in raw.items():
        if k in mapper:
            out[mapper[k]] = v if v not in ("", None) else None
    for required in mapper.values():
        out.setdefault(required, None)
    return out

def is_blank_row(values: list) -> bool:
    """すべて空のセルかチェック"""
    return all((v is None) or (str(v).strip() == "") for v in values)

def circuit_exists(conn, cid: str) -> bool:
    """circuits にcidが存在するかチェック"""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM circuits WHERE circuit_id = %s;", (cid,))
        return bool(cur.fetchone())

# =============================================================================
# UPSERT関数
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
# データ補完関数
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
    """cid が circuits に無ければ最小限で作成"""
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
    """ref_id が references_tbl に無ければ最小構成で追加"""
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
    """必須カラムの既定値補完"""
    if table in REQUIRED:
        for col, fn in REQUIRED[table].items():
            if rec.get(col) in (None, "", "NA", "(no DOI)", "(no BibTex)"):
                rec[col] = fn(rec)

    # references_tbl のproject_idの存在確認
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
# テスト・検証関数
# =============================================================================

def run_tests(conn):
    """データベースの検証とテストを実行"""
    print("\n" + "="*80)
    print("データベース検証とテスト")
    print("="*80)
    
    results = {}
    
    # 出力ディレクトリ作成
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. テーブルごとのレコード数確認
    print("\n1. テーブルごとのレコード数:")
    print("-" * 60)
    tables = ['project', 'references_tbl', 'circuits', 'connections', 'settings', 'changelog']
    
    for table in tables:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            results[f"{table}_count"] = count
            print(f"  {table:20s}: {count:6d} 件")
    
    # 2. サンプルデータの確認
    print("\n2. サンプルデータ確認:")
    print("-" * 60)
    
    # Project
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM project LIMIT 5;")
        projects = cur.fetchall()
        print(f"\n  【Project】最初の5件:")
        for i, p in enumerate(projects, 1):
            print(f"    {i}. {p['project_id']} - {p['contributor']}")
    
    # References
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT reference_id, title, doi FROM references_tbl LIMIT 5;")
        refs = cur.fetchall()
        print(f"\n  【References】最初の5件:")
        for i, r in enumerate(refs, 1):
            title = (r['title'] or '')[:50]
            print(f"    {i}. {r['reference_id']} - {title}...")
    
    # Circuits
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT circuit_id, names FROM circuits LIMIT 5;")
        circuits = cur.fetchall()
        print(f"\n  【Circuits】最初の5件:")
        for i, c in enumerate(circuits, 1):
            print(f"    {i}. {c['circuit_id']} - {c['names']}")
    
    # Connections
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT sender_circuit_id, receiver_circuit_id, reference_id 
            FROM connections LIMIT 5;
        """)
        conns = cur.fetchall()
        print(f"\n  【Connections】最初の5件:")
        for i, c in enumerate(conns, 1):
            print(f"    {i}. {c['sender_circuit_id']} -> {c['receiver_circuit_id']} (ref: {c['reference_id']})")
    
    # 3. データ整合性チェック
    print("\n3. データ整合性チェック:")
    print("-" * 60)
    
    # 外部キー制約の確認
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
            status = "✅ OK" if count == 0 else f"⚠️  WARNING: {count} 件"
            print(f"  {check_name:40s}: {status}")
            if count > 0:
                integrity_ok = False
    
    # 4. CSV出力
    print("\n4. データのCSV出力:")
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
                    # 列名は cursor.description から安全に取得
                    cols = [desc.name for desc in cur.description]

                    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
                        writer.writeheader()
                        
                        # 各行を書き込み（欠損列は空文字で埋める）
                        for row in rows:
                            rd = dict(row)
                            writer.writerow({c: rd.get(c, "") for c in cols})
                    print(f"  ✅ {table:20s} -> {csv_path}")
                else:
                    print(f"  ⚠️  {table:20s} (データなし)")
        except Exception as e:
            print(f"  ❌ {table:20s} エラー: {e}")
    # 5. サマリーレポート
    print("\n" + "="*80)
    print("テスト結果サマリー")
    print("="*80)
    
    total_records = sum(results.values())
    print(f"\n総レコード数: {total_records:,} 件")
    print(f"データ整合性: {'✅ 問題なし' if integrity_ok else '⚠️ 警告あり'}")
    print(f"CSV出力先: {OUTPUT_DIR}/")
    
    # サマリーをJSON出力
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
    
    print(f"サマリーレポート: {summary_path}")
    print("\n✅ テスト完了!")
    
    return integrity_ok

# =============================================================================
# メイン処理
# =============================================================================

def main():
    """メイン処理"""
    print("="*80)
    print("WholeBIF RDB 構築 & テストスクリプト")
    print("="*80)
    
    try:
        # 1. Google Spreadsheet認証
        print("\n[1/6] Google Spreadsheet認証中...")
        gc = authorize_gspread()
        ss = get_spreadsheet(gc, SPREADSHEET_KEY)
        print("  ✅ 認証成功")
        
        # 2. PostgreSQL接続
        print("\n[2/6] PostgreSQL接続中...")
        conn = create_connection()
        print(f"  ✅ 接続成功: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # 3. テーブル作成
        print("\n[3/6] テーブル作成中...")
        create_tables(conn)
        print("  ✅ テーブル作成完了")
        
        # 4. データ読み込み
        print("\n[4/6] Google Spreadsheetからデータ読み込み中...")
        proj_sheet = read_sheet(ss, "Project")
        ref_sheet  = read_sheet(ss, "References")
        cir_sheet  = read_sheet(ss, "Circuits")
        con_sheet  = read_sheet(ss, "Connections")
        set_sheet  = read_sheet(ss, "Settings")
        print(f"  ✅ データ読み込み完了")
        print(f"     Project: {len(proj_sheet)-1} 件")
        print(f"     References: {len(ref_sheet)-1} 件")
        print(f"     Circuits: {len(cir_sheet)-1} 件")
        print(f"     Connections: {len(con_sheet)-1} 件")
        print(f"     Settings: {len(set_sheet)-1} 件")
        
        # 5. データ投入
        print("\n[5/6] データベースへデータ投入中...")
        
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
        print(f"  ✅ Project: {proj_count} 件投入")
        
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
        print(f"  ✅ References: {ref_count} 件投入")
        
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
        print(f"  ✅ Circuits: {cir_count} 件投入")
        
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
        print(f"  ✅ Connections: {con_count} 件投入")
        
        # Settings
        set_count = 0
        for row in set_sheet[1:]:
            rec = map_row(dict(zip(set_header, row)), MAP_SETTINGS)
            rec = fill_required("settings", rec, conn)
            if rec.get("wholebif_file_id"):
                insert_settings(conn, rec)
                set_count += 1
        print(f"  ✅ Settings: {set_count} 件投入")
        
        # コミット
        conn.commit()
        print("\n  ✅ すべてのデータ投入完了")
        
        # 6. テスト実行
        print("\n[6/6] データベース検証とテスト実行中...")
        integrity_ok = run_tests(conn)
        
        # 完了
        conn.close()
        
        print("\n" + "="*80)
        print("✅ WholeBIF RDB 構築完了!")
        print("="*80)
        
        if not integrity_ok:
            print("\n⚠️  注意: データ整合性に警告があります。test_results/ を確認してください。")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# =============================================================================
# エントリーポイント
# =============================================================================

if __name__ == "__main__":
    main()