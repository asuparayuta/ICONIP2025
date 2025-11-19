#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gradio_wholebif_query_app_v3.py
---------------------------------------------------
v3 Simplified:
- Query Explorer: revised (remove receiver side, improved connections view, DOI links, scores ascending)
- Flex Pair Finder: revised (DOI links, evidence removed, scores ascending with NULLs last, year removed)
- Pair Lookup: removed

Usage:
  python gradio_wholebif_query_app_v3.py --share --auth user:pass
  python gradio_wholebif_query_app_v3.py --host 0.0.0.0 --port 7860 --auth user:pass
"""

import os
import json
import argparse
import traceback
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
import gradio as gr


# ==============================
# DB & pooling
# ==============================

@dataclass
class DBFlags:
    has_evidence: bool
    has_refs_view: bool
    refs_source: str
    has_scores: bool
    has_pg_trgm: bool
    has_connections_std: bool


_POOL: Optional[SimpleConnectionPool] = None

def get_dsn() -> Dict[str, Any]:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db   = os.getenv("POSTGRES_DB", "wholebif_rdb")
    user = os.getenv("POSTGRES_USER", "wholebif")
    pwd  = os.getenv("POSTGRES_PASSWORD", "")
    return dict(host=host, port=port, dbname=db, user=user, password=pwd)

def pool() -> SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        dsn = get_dsn()
        minc = int(os.getenv("DB_POOL_MIN", "1"))
        maxc = int(os.getenv("DB_POOL_MAX", "6"))
        _POOL = SimpleConnectionPool(minc, maxc, **dsn)
    return _POOL

def with_conn(fn):
    """Decorator to borrow/return a pooled connection"""
    def wrapper(*args, **kwargs):
        p = pool()
        conn = p.getconn()
        try:
            conn.autocommit = True
            return fn(conn, *args, **kwargs)
        finally:
            p.putconn(conn)
    return wrapper


# ==============================
# Flags / detection
# ==============================

@with_conn
def detect_flags(conn) -> DBFlags:
    def _exists(kind: str, name: str) -> bool:
        if kind == "tables":
            qry = "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=%s"
        else:
            qry = "SELECT 1 FROM information_schema.views  WHERE table_schema='public' AND table_name=%s"
        with conn.cursor() as cur:
            cur.execute(qry, (name,))
            return cur.fetchone() is not None

    def _ext(name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname=%s", (name,))
            return cur.fetchone() is not None

    has_evidence        = _exists("tables", "evidence")
    has_refs_view       = _exists("views", "refs")
    has_scores          = _exists("tables", "scores")
    has_pg_trgm         = _ext("pg_trgm")
    has_connections_std = _exists("views", "connections_std")
    refs_source         = "refs" if has_refs_view else ("references_tbl" if _exists("tables", "references_tbl") else "refs")

    return DBFlags(
        has_evidence=has_evidence,
        has_refs_view=has_refs_view,
        refs_source=refs_source,
        has_scores=has_scores,
        has_pg_trgm=has_pg_trgm,
        has_connections_std=has_connections_std,
    )


def refs_join_cols_for_source(refs_source: str) -> Tuple[str, str]:
    if refs_source == "refs":
        # refsビュー: authorsなし、journalあり
        return ("LEFT JOIN refs r ON r.reference_id = x.reference_id",
                "r.reference_id, r.doi, r.title, r.journal, r.year, r.url")
    else:
        # references_tblテーブル: authorsあり、journal_namesあり
        return ("LEFT JOIN references_tbl r ON r.reference_id = x.reference_id",
                "r.reference_id, r.doi, r.title, r.journal_names AS journal, NULL::int AS year, COALESCE(r.doc_link, r.alternative_url) AS url")


# ==============================
# Suggestors (pooled)
# ==============================

@with_conn
def _suggest_circuit_ids(conn, q: str, topn: int = 12) -> List[str]:
    flags = detect_flags()
    with conn.cursor() as cur:
        if flags.has_pg_trgm:
            cur.execute(
                """
                SELECT circuit_id
                FROM circuits
                WHERE circuit_id %% %(q)s OR COALESCE(names,'') %% %(q)s
                ORDER BY GREATEST(similarity(circuit_id, %(q)s), similarity(COALESCE(names,''), %(q)s)) DESC
                LIMIT %(limit)s
                """,
                {"q": q, "limit": topn}
            )
        else:
            cur.execute(
                """
                SELECT circuit_id
                FROM circuits
                WHERE circuit_id ILIKE %(pat)s OR COALESCE(names,'') ILIKE %(pat)s
                ORDER BY circuit_id
                LIMIT %(limit)s
                """,
                {"pat": f"%{q}%", "limit": topn}
            )
        return [r[0] for r in cur.fetchall()]

def suggest_circuit_ids(partial: str, topn: int = 12):
    q = (partial or "").strip()
    if len(q) < 2:
        return gr.update(choices=[], value=None)
    try:
        return gr.update(choices=_suggest_circuit_ids(q, topn), value=None)
    except Exception as e:
        print("suggest_circuit_ids error:", e)
        return gr.update(choices=[], value=None)


@with_conn
def _suggest_receiver_ids(conn, q: str, topn: int = 12) -> List[str]:
    flags = detect_flags()
    with conn.cursor() as cur:
        if flags.has_pg_trgm:
            cur.execute(
                """
                SELECT DISTINCT receiver_circuit_id AS receiver_id,
                       similarity(receiver_circuit_id, %(q)s) AS sim
                FROM connections
                WHERE receiver_circuit_id %% %(q)s
                ORDER BY sim DESC
                LIMIT %(limit)s
                """,
                {"q": q, "limit": topn}
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT receiver_circuit_id AS receiver_id
                FROM connections
                WHERE receiver_circuit_id ILIKE %(pat)s
                ORDER BY receiver_circuit_id
                LIMIT %(limit)s
                """,
                {"pat": f"%{q}%", "limit": topn}
            )
        return [r[0] for r in cur.fetchall()]

def suggest_receiver_ids(partial: str, topn: int = 12):
    q = (partial or "").strip()
    if len(q) < 2:
        return gr.update(choices=[], value=None)
    try:
        return gr.update(choices=_suggest_receiver_ids(q, topn), value=None)
    except Exception as e:
        print("suggest_receiver_ids error:", e)
        return gr.update(choices=[], value=None)


@with_conn
def _suggest_any_region(conn, q: str, topn: int = 12) -> List[str]:
    flags = detect_flags()
    cids, rids = [], []
    with conn.cursor() as cur:
        if flags.has_pg_trgm:
            cur.execute(
                """
                SELECT circuit_id
                FROM circuits
                WHERE circuit_id %% %(q)s OR COALESCE(names,'') %% %(q)s
                ORDER BY GREATEST(similarity(circuit_id, %(q)s), similarity(COALESCE(names,''), %(q)s)) DESC
                LIMIT %(limit)s
                """,
                {"q": q, "limit": topn}
            )
            cids = [r[0] for r in cur.fetchall()]
            cur.execute(
                """
                SELECT DISTINCT receiver_circuit_id AS receiver_id, 
                       similarity(receiver_circuit_id, %(q)s) AS sim
                FROM connections
                WHERE receiver_circuit_id %% %(q)s
                ORDER BY sim DESC
                LIMIT %(limit)s
                """,
                {"q": q, "limit": topn}
            )
            rids = [r[0] for r in cur.fetchall()]
        else:
            cur.execute(
                """
                SELECT circuit_id
                FROM circuits
                WHERE circuit_id ILIKE %(pat)s OR COALESCE(names,'') ILIKE %(pat)s
                ORDER BY circuit_id
                LIMIT %(limit)s
                """,
                {"pat": f"%{q}%", "limit": topn}
            )
            cids = [r[0] for r in cur.fetchall()]
            cur.execute(
                """
                SELECT DISTINCT receiver_circuit_id AS receiver_id
                FROM connections
                WHERE receiver_circuit_id ILIKE %(pat)s
                ORDER BY receiver_circuit_id
                LIMIT %(limit)s
                """,
                {"pat": f"%{q}%", "limit": topn}
            )
            rids = [r[0] for r in cur.fetchall()]
    combined = list(dict.fromkeys(cids + rids))
    return combined[:topn]

def suggest_any_region(partial: str, topn: int = 12):
    q = (partial or "").strip()
    if len(q) < 2:
        return gr.update(choices=[], value=None)
    try:
        return gr.update(choices=_suggest_any_region(q, topn), value=None)
    except Exception as e:
        print("suggest_any_region error:", e)
        return gr.update(choices=[], value=None)


def apply_selection_to_text(sel):
    return sel if sel else ""


# ==============================
# Query logic
# ==============================

def circuits_like_sql(flags: DBFlags) -> str:
    if flags.has_pg_trgm:
        return """
        SELECT circuit_id, names,
               GREATEST(
                   similarity(circuit_id, %(q)s),
                   similarity(COALESCE(names,''), %(q)s)
               ) AS sim
        FROM circuits
        WHERE circuit_id %% %(q)s OR COALESCE(names,'') %% %(q)s
        ORDER BY sim DESC
        LIMIT %(limit)s;
        """
    else:
        return """
        SELECT circuit_id, names, 1.0::float AS sim
        FROM circuits
        WHERE circuit_id ILIKE %(pat)s OR COALESCE(names,'') ILIKE %(pat)s
        ORDER BY circuit_id
        LIMIT %(limit)s;
        """

def receivers_like_sql(flags: DBFlags) -> str:
    if flags.has_pg_trgm:
        return """
        SELECT DISTINCT receiver_circuit_id AS receiver_id, similarity(receiver_circuit_id, %(q)s) AS sim
        FROM connections
        WHERE receiver_circuit_id %% %(q)s
        ORDER BY sim DESC
        LIMIT %(limit)s;
        """
    else:
        return """
        SELECT DISTINCT receiver_circuit_id AS receiver_id, NULL::float AS sim
        FROM connections
        WHERE receiver_circuit_id ILIKE %(pat)s
        ORDER BY receiver_circuit_id
        LIMIT %(limit)s;
        """

def run_query(query: str, limit: int = 20):
    """
    Revised: remove receiver side, improve connections view, sort scores ascending, link DOIs
    """
    q = (query or "").strip()
    empty = pd.DataFrame()
    if q == "":
        return (empty, empty, empty, empty, empty, "Please enter a query.")
    try:
        flags = detect_flags()
        # 1) circuits
        @with_conn
        def _fetch_circuits(conn):
            with conn.cursor() as cur:
                sql_cir = circuits_like_sql(flags)
                params = {"q": q, "limit": limit, "pat": f"%{q}%"}
                cur.execute(sql_cir, params)
                crow = cur.fetchall()
                ccols = [d.name for d in cur.description]
                return pd.DataFrame(crow, columns=ccols)
        df_circuits = _fetch_circuits()

        circuit_ids: List[str] = df_circuits["circuit_id"].dropna().astype(str).tolist()

        # Circuit-side Connections (修正版: 表示順とDOI追加)
        if circuit_ids:
            @with_conn
            def _fetch_conn_c(conn):
                placeholders = ",".join([f"%s" for _ in circuit_ids])
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT 
                            c.sender_circuit_id as circuit_id,
                            c.receiver_circuit_id as receiver_id,
                            c.taxon,
                            c.reference_id,
                            r.doi,
                            c.pointers_on_literature,
                            c.journal_score,
                            c.csi_score,
                            c.pder_score,
                            c.credibility_rating
                        FROM connections c
                        LEFT JOIN {flags.refs_source} r ON c.reference_id = r.reference_id
                        WHERE c.sender_circuit_id IN ({placeholders})
                        ORDER BY c.sender_circuit_id, c.receiver_circuit_id, c.reference_id
                    """, circuit_ids)
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
                    df = pd.DataFrame(rows, columns=cols)
                    
                    # DOIをURL形式で表示
                    if not df.empty and 'doi' in df.columns:
                        df['doi'] = df['doi'].apply(
                            lambda x: f'https://doi.org/{x}' if pd.notna(x) and x else ''
                        )
                    return df
            df_conn_c = _fetch_conn_c()
        else:
            df_conn_c = pd.DataFrame(columns=[
                "circuit_id","receiver_id","taxon","reference_id","doi",
                "pointers_on_literature",
                "journal_score","csi_score","pder_score",
                "credibility_rating"
            ])

        # References for circuit-side
        if not df_conn_c.empty:
            ref_ids = df_conn_c["reference_id"].dropna().astype(str).unique().tolist()
            if ref_ids:
                placeholders = ",".join([f"%s" for _ in ref_ids])
                join_clause, ref_cols = refs_join_cols_for_source(flags.refs_source)
                @with_conn
                def _fetch_refs_c(conn):
                    with conn.cursor() as cur:
                        cur.execute(f"""
                            WITH x AS (
                              SELECT DISTINCT unnest(ARRAY[{placeholders}]::text[]) AS reference_id
                            )
                            SELECT {ref_cols}
                            FROM x
                            {join_clause}
                            ORDER BY 1
                        """, ref_ids)
                        rows = cur.fetchall()
                        cols = [d.name for d in cur.description]
                        df = pd.DataFrame(rows, columns=cols)
                        
                        # DOIをURL形式で表示
                        if not df.empty and 'doi' in df.columns:
                            df['doi'] = df['doi'].apply(
                                lambda x: (f'<a href="https://doi.org/{x}" target="_blank">https://doi.org/{x}</a>'
                                           if pd.notna(x) and x else '')
                            )
                        return df
                df_refs_c = _fetch_refs_c()
            else:
                df_refs_c = pd.DataFrame(columns=["reference_id","doi","title","journal","year","url"])
        else:
            df_refs_c = pd.DataFrame(columns=["reference_id","doi","title","journal","year","url"])

        # Evidence for circuit-side (変更なし)
        flags_now = detect_flags()
        if flags_now.has_evidence and not df_conn_c.empty:
            @with_conn
            def _fetch_evi_c(conn):
                placeholders = ",".join([f"%s" for _ in circuit_ids])
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT e.evidence_id, e.circuit_id, e.receiver_id, e.reference_id,
                               e.connection_flag, e.method, e.taxon, e.modulation_type, e.output_semantics,
                               e.pointers_on_literature, e.pointers_on_figure, e.status
                        FROM evidence e
                        WHERE e.circuit_id IN ({placeholders})
                        ORDER BY e.circuit_id, e.receiver_id, e.evidence_id
                    """, circuit_ids)
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
                    return pd.DataFrame(rows, columns=cols)
            df_evi_c = _fetch_evi_c()
        elif not flags_now.has_evidence and not df_conn_c.empty:
            df_evi_c = df_conn_c.rename(columns={
                "circuit_id":"circuit_id", "receiver_id":"receiver_id",
                "reference_id":"reference_id",
                "pointers_on_literature":"pointers_on_literature"
            }).assign(connection_flag=True, evidence_id=None, method=None, taxon=None, 
                     modulation_type=None, output_semantics=None, pointers_on_figure=None, status="SURROGATE")
            df_evi_c = df_evi_c[[
                "evidence_id","circuit_id","receiver_id","reference_id","connection_flag",
                "method","taxon","modulation_type","output_semantics","pointers_on_literature","pointers_on_figure","status"
            ]]
        else:
            df_evi_c = pd.DataFrame(columns=[
                "evidence_id","circuit_id","receiver_id","reference_id","connection_flag",
                "method","taxon","modulation_type","output_semantics","pointers_on_literature","pointers_on_figure","status"
            ])

        # Scores for circuit-side (修正版: 昇順ソート)
        if flags_now.has_scores and circuit_ids:
            try:
                @with_conn
                def _fetch_score_c(conn):
                    placeholders = ",".join([f"%s" for _ in circuit_ids])
                    with conn.cursor() as cur:
                        cur.execute(f"""
                            SELECT circuit_id, score_mean, score_summary
                            FROM scores
                            WHERE circuit_id IN ({placeholders})
                            ORDER BY score_mean ASC NULLS LAST, circuit_id
                        """, circuit_ids)
                        rows = cur.fetchall()
                        cols = [d.name for d in cur.description]
                        return pd.DataFrame(rows, columns=cols)
                df_score_c = _fetch_score_c()
            except Exception:
                df_score_c = pd.DataFrame(columns=["circuit_id","score_mean","score_summary"])
        else:
            df_score_c = pd.DataFrame(columns=["circuit_id","score_mean","score_summary"])

        diag = json.dumps({"flags": vars(flags_now), "circuit_matches": len(circuit_ids)}, ensure_ascii=False, indent=2)
        return (df_circuits, df_conn_c, df_refs_c, df_evi_c, df_score_c, diag)
    except Exception as e:
        tb = traceback.format_exc()
        empty = pd.DataFrame()
        return (empty, empty, empty, empty, empty, f"DB error: {e}\n\n{tb}")


# ==============================
# Flex Pair logic (pooled)
# ==============================

@with_conn
def _pair_exists(conn, sender: str, receiver: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM connections
            WHERE sender_circuit_id = %s AND receiver_circuit_id = %s
        """, (sender, receiver))
        return cur.fetchone()[0]

@with_conn
def _fetch_pair_details(conn, sender: str, receiver: str, flags: DBFlags):
    """
    修正版: Connections表示改善、References Year削除、Evidence削除、Scores昇順・NULL除外、DOIリンク化
    """
    with conn.cursor() as cur:
        # Connections (修正版)
        cur.execute(f"""
            SELECT 
                c.sender_circuit_id as circuit_id,
                c.receiver_circuit_id as receiver_id,
                c.taxon,
                c.reference_id,
                r.doi,
                c.pointers_on_literature,
                c.journal_score,
                c.csi_score,
                c.pder_score,
                c.credibility_rating
            FROM connections c
            LEFT JOIN {flags.refs_source} r ON c.reference_id = r.reference_id
            WHERE c.sender_circuit_id = %s AND c.receiver_circuit_id = %s
            ORDER BY c.reference_id
        """, (sender, receiver))
        conn_rows = cur.fetchall()
        conn_cols = [d.name for d in cur.description]
        df_conn = pd.DataFrame(conn_rows, columns=conn_cols)
        
        # DOIをURL形式で表示
        if not df_conn.empty and 'doi' in df_conn.columns:
            df_conn['doi'] = df_conn['doi'].apply(
                lambda x: f'https://doi.org/{x}' if pd.notna(x) and x else ''
            )

    # References (修正版: Year削除、DOIリンク化、カラム名対応)
    ref_ids = df_conn["reference_id"].dropna().astype(str).unique().tolist()
    if ref_ids:
        placeholders = ",".join([f"%s" for _ in ref_ids])
        with conn.cursor() as cur:
            # refs_sourceに応じてカラムを選択
            if flags.refs_source == "refs":
                # refsビューの場合（authorsカラムなし）
                cur.execute(f"""
                    SELECT DISTINCT r.reference_id, r.title, NULL AS authors, r.journal, r.doi
                    FROM connections c
                    LEFT JOIN refs r ON c.reference_id = r.reference_id
                    WHERE c.sender_circuit_id = %s AND c.receiver_circuit_id = %s
                    ORDER BY r.reference_id
                """, (sender, receiver))
            else:
                # references_tblテーブルの場合（authorsカラムあり）
                cur.execute(f"""
                    SELECT DISTINCT r.reference_id, r.title, r.authors, r.journal_names AS journal, r.doi
                    FROM connections c
                    LEFT JOIN {flags.refs_source} r ON c.reference_id = r.reference_id
                    WHERE c.sender_circuit_id = %s AND c.receiver_circuit_id = %s
                    ORDER BY r.reference_id
                """, (sender, receiver))
            
            refs_rows = cur.fetchall()
            refs_cols = [d.name for d in cur.description]
            df_refs = pd.DataFrame(refs_rows, columns=refs_cols)
            
            # DOIをURL形式で表示
            if not df_refs.empty and 'doi' in df_refs.columns:
                df_refs['doi'] = df_refs['doi'].apply(
                    lambda x: f'https://doi.org/{x}' if pd.notna(x) and x else ''
                )
    else:
        df_refs = pd.DataFrame(columns=["reference_id","title","authors","journal","doi"])

    # Evidence - 削除
    df_evi = pd.DataFrame()

    # Scores (修正版: 昇順、NULL除外)
    flags_now = flags
    if flags_now.has_scores:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT circuit_id, score_mean, score_summary
                FROM scores
                WHERE circuit_id IN (%s, %s)
                  AND (score_mean IS NOT NULL OR score_summary IS NOT NULL)
                ORDER BY score_mean ASC NULLS LAST, circuit_id
            """, (sender, receiver))
            score_rows = cur.fetchall()
            score_cols = [d.name for d in cur.description]
            df_scores = pd.DataFrame(score_rows, columns=score_cols)
    else:
        df_scores = pd.DataFrame()

    return df_conn, df_refs, df_evi, df_scores


@with_conn
def _counterparts(conn, rid: str) -> Tuple[List[str], List[str]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT receiver_circuit_id
            FROM connections
            WHERE sender_circuit_id = %s
            ORDER BY 1
        """, (rid,))
        receivers = [r[0] for r in cur.fetchall() if r[0] is not None]

        cur.execute("""
            SELECT DISTINCT sender_circuit_id
            FROM connections
            WHERE receiver_circuit_id = %s
            ORDER BY 1
        """, (rid,))
        senders = [r[0] for r in cur.fetchall() if r[0] is not None]

    return receivers, senders


def update_counterparts(selected_region: str):
    rid = (selected_region or "").strip()
    if rid == "":
        return (gr.update(value="領域を選択してください。"),
                gr.update(value="Auto"),
                gr.update(choices=[], value=None, interactive=False),
                gr.update(choices=[], value=None, interactive=False),
                json.dumps({"cnt_as_sender":0,"cnt_as_receiver":0}))
    try:
        receivers, senders = _counterparts(rid)
        cnt_s, cnt_r = len(receivers), len(senders)
        if cnt_s > 0 and cnt_r == 0:
            mode_val = "Use as Sender"
        elif cnt_s == 0 and cnt_r > 0:
            mode_val = "Use as Receiver"
        else:
            mode_val = "Auto"

        msg = f"選択: `{rid}` — as **Sender**: {cnt_s} 件 / as **Receiver**: {cnt_r} 件"
        return (gr.update(value=msg),
                gr.update(value=mode_val),
                gr.update(choices=receivers, value=None, interactive=(cnt_s>0 and (mode_val!='Use as Receiver'))),
                gr.update(choices=senders,   value=None, interactive=(cnt_r>0 and (mode_val!='Use as Sender'))),
                json.dumps({"cnt_as_sender":cnt_s,"cnt_as_receiver":cnt_r}))
    except Exception as e:
        print("update_counterparts error:", e)
        return (gr.update(value=f"DB error: {e}"),
                gr.update(value="Auto"),
                gr.update(choices=[], value=None, interactive=False),
                gr.update(choices=[], value=None, interactive=False),
                json.dumps({"cnt_as_sender":0,"cnt_as_receiver":0}))


def update_counterparts_and_clear(selected_region: str):
    status, mode_update, rec_dd, snd_dd, counts = update_counterparts(selected_region)
    empty = pd.DataFrame()
    return (status, mode_update, rec_dd, snd_dd, counts, empty, empty, empty, empty)


def toggle_mode(mode: str, counts_json: str):
    try:
        counts = json.loads(counts_json or "{}")
        cnt_s = int(counts.get("cnt_as_sender", 0))
        cnt_r = int(counts.get("cnt_as_receiver", 0))
    except Exception:
        cnt_s = cnt_r = 0

    if mode == "Use as Sender":
        return (gr.update(interactive=True),  gr.update(interactive=False))
    elif mode == "Use as Receiver":
        return (gr.update(interactive=False), gr.update(interactive=True))
    else:
        if cnt_s>0 and cnt_r==0:
            return (gr.update(interactive=True),  gr.update(interactive=False))
        elif cnt_s==0 and cnt_r>0:
            return (gr.update(interactive=False), gr.update(interactive=True))
        else:
            return (gr.update(interactive=True),  gr.update(interactive=True))


def lookup_from_flex(selected_region: str, mode: str, chosen_receiver: str, chosen_sender: str):
    rid = (selected_region or "").strip()
    empty = pd.DataFrame()
    
    if rid == "":
        return (gr.update(value="領域を先に選択してください。"), empty, empty, empty, empty)

    m = (mode or "Auto").strip()
    if m == "Use as Sender":
        sender, receiver = rid, (chosen_receiver or "").strip()
    elif m == "Use as Receiver":
        sender, receiver = (chosen_sender or "").strip(), rid
    else:
        if chosen_receiver:
            sender, receiver = rid, chosen_receiver.strip()
        elif chosen_sender:
            sender, receiver = chosen_sender.strip(), rid
        else:
            return (gr.update(value="Please select counterpart candidates."), empty, empty, empty, empty)

    if sender == "" or receiver == "":
        return gr.update(value="Please specify both Sender and Receiver."), empty, empty, empty, empty

    try:
        flags = detect_flags()
        n = _pair_exists(sender, receiver)
        if n == 0:
            return gr.update(value=f"**未存在**：Connections に `{sender}` → `{receiver}` の行はありません。"), empty, empty, empty, empty

        df_conn, df_refs, df_evi, df_scores = _fetch_pair_details(sender, receiver, flags)
        msg = gr.update(value=f"**Found**: `{sender}` → `{receiver}` (connections: {len(df_conn)} rows)")
        return msg, df_conn, df_refs, df_evi, df_scores
    except Exception as e:
        print("lookup_from_flex error:", e)
        return gr.update(value=f"DB error: {e}"), empty, empty, empty, empty


def clear_results_only():
    empty = pd.DataFrame()
    return (gr.update(value="Cleared results."), empty, empty, empty, empty)


def refresh_candidates_and_clear(selected_region: str):
    return update_counterparts_and_clear(selected_region)


# ==============================
# UI
# ==============================

def build_ui():
    with gr.Blocks(title="WholeBIF-RDB – Query & Pair Tools (v3 Simplified)") as demo:
        gr.Markdown("# WholeBIF-RDB – Query & Pair Tools")

        # ===== Query Explorer =====
        with gr.Tab("Query Explorer"):
            with gr.Row():
                query = gr.Textbox(label="Keyword", placeholder="Type 2+ chars for suggestions", scale=4)
                limit = gr.Slider(label="Max results", minimum=5, maximum=100, value=20, step=5, scale=1)
                btn = gr.Button("Search", variant="primary", scale=1)
                btn_suggest = gr.Button("Suggest", scale=1)

            suggest = gr.Dropdown(label="Circuit ID candidates", choices=[], interactive=True, allow_custom_value=False)

            diag = gr.Code(label="Diagnostics", interactive=False)

            with gr.Accordion("A. Similar Circuits", open=True):
                df_circuits = gr.Dataframe(label="Matched Circuits", interactive=False, wrap=True)
                df_conn_c = gr.Dataframe(label="Connections from Circuit matches", interactive=False, wrap=True)
                df_refs_c = gr.Dataframe(label="References (distinct)", interactive=False, wrap=True)
                df_evi_c = gr.Dataframe(label="Evidence (outgoing)", interactive=False, wrap=True)
                df_score_c = gr.Dataframe(label="Scores (or proxy)", interactive=False, wrap=True)

            # B. Receiver に類似 セクション削除

            btn.click(
                fn=run_query,
                inputs=[query, limit],
                outputs=[df_circuits, df_conn_c, df_refs_c, df_evi_c, df_score_c, diag]
            )

            # live suggest on input + change + manual button
            query.input(fn=suggest_circuit_ids, inputs=[query], outputs=[suggest], queue=False)
            query.change(fn=suggest_circuit_ids, inputs=[query], outputs=[suggest])
            btn_suggest.click(fn=suggest_circuit_ids, inputs=[query], outputs=[suggest])

            evt = suggest.select(fn=apply_selection_to_text, inputs=[suggest], outputs=[query])
            evt.then(fn=run_query, inputs=[query, limit],
                     outputs=[df_circuits, df_conn_c, df_refs_c, df_evi_c, df_score_c, diag])

        # ===== Pair Lookup タブ削除 =====

        # ===== Flex Pair Finder =====
        with gr.Tab("Flex Pair Finder"):
            with gr.Row():
                region_text = gr.Textbox(label="Region (ID / Names / Receiver)", placeholder="Type 2+ chars for suggestions", scale=3)
                region_suggest = gr.Dropdown(label="Candidates (click to select)", choices=[], interactive=True, allow_custom_value=False, scale=2)
                mode = gr.Radio(label="Direction", choices=["Auto","Use as Sender","Use as Receiver"], value="Auto", scale=2)
                btn_suggest_region = gr.Button("Suggest", scale=1)

            receivers_dd = gr.Dropdown(label="Counterparts (you as Sender → Receivers)", choices=[], interactive=False, allow_custom_value=False)
            senders_dd   = gr.Dropdown(label="Counterparts (you as Receiver ← Senders)", choices=[], interactive=False, allow_custom_value=False)

            with gr.Row():
                btn_lookup   = gr.Button("Lookup Pair", variant="primary")
                btn_clear    = gr.Button("Clear Results")
                btn_refresh  = gr.Button("Refresh Candidates")

            status2 = gr.Markdown()
            df_pair_conn2   = gr.Dataframe(label="Connections (pair)", interactive=False, wrap=True)
            df_pair_refs2   = gr.Dataframe(label="References (distinct)", interactive=False, wrap=True)
            # Evidence削除
            df_pair_scores2 = gr.Dataframe(label="Scores (pair | proxy)", interactive=False, wrap=True)

            counts_state = gr.State(value=json.dumps({"cnt_as_sender":0,"cnt_as_receiver":0}))

            region_text.input(fn=suggest_any_region, inputs=[region_text], outputs=[region_suggest], queue=False)
            region_text.change(fn=suggest_any_region, inputs=[region_text], outputs=[region_suggest])
            btn_suggest_region.click(fn=suggest_any_region, inputs=[region_text], outputs=[region_suggest])

            evt = region_suggest.select(fn=apply_selection_to_text, inputs=[region_suggest], outputs=[region_text])
            evt.then(fn=update_counterparts_and_clear, inputs=[region_text],
                     outputs=[status2, mode, receivers_dd, senders_dd, counts_state, df_pair_conn2, df_pair_refs2, gr.Dataframe(), df_pair_scores2])

            mode.change(fn=toggle_mode, inputs=[mode, counts_state],
                        outputs=[receivers_dd, senders_dd])

            btn_lookup.click(fn=lookup_from_flex, inputs=[region_text, mode, receivers_dd, senders_dd],
                             outputs=[status2, df_pair_conn2, df_pair_refs2, gr.Dataframe(), df_pair_scores2])

            btn_clear.click(fn=clear_results_only, inputs=[], outputs=[status2, df_pair_conn2, df_pair_refs2, gr.Dataframe(), df_pair_scores2])
            btn_refresh.click(fn=refresh_candidates_and_clear, inputs=[region_text],
                              outputs=[status2, mode, receivers_dd, senders_dd, counts_state, df_pair_conn2, df_pair_refs2, gr.Dataframe(), df_pair_scores2])

            receivers_dd.select(fn=lookup_from_flex, inputs=[region_text, mode, receivers_dd, senders_dd],
                                outputs=[status2, df_pair_conn2, df_pair_refs2, gr.Dataframe(), df_pair_scores2])
            senders_dd.select(fn=lookup_from_flex, inputs=[region_text, mode, receivers_dd, senders_dd],
                              outputs=[status2, df_pair_conn2, df_pair_refs2, gr.Dataframe(), df_pair_scores2])

        # ヒントメッセージ削除

    return demo


# ==============================
# Public launcher (CLI)
# ==============================

def _parse_auth(auth_str: str):
    if not auth_str:
        return None
    items = [p.strip() for p in auth_str.split(",") if p.strip()]
    creds: List[Tuple[str, str]] = []
    for it in items:
        if ":" not in it:
            continue
        u, p = it.split(":", 1)
        creds.append((u, p))
    if not creds:
        return None
    if len(creds) == 1:
        return creds[0]
    return creds


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--share", action="store_true", help="Create a public gradio.live link (ephemeral)")
    parser.add_argument("--host", default=os.getenv("GRADIO_HOST", "127.0.0.1"), help="Bind address (e.g., 0.0.0.0)")
    parser.add_argument("--port", type=int, default=int(os.getenv("GRADIO_PORT", os.getenv("PORT", "7860"))), help="Port")
    parser.add_argument("--auth", default=os.getenv("GRADIO_AUTH", ""), help="Basic auth (user:pass or 'u1:p1,u2:p2')")
    args = parser.parse_args()

    share_env = os.getenv("GRADIO_SHARE", "")
    share_flag = args.share or share_env.lower() in ("1", "true", "yes", "y")

    auth = _parse_auth(args.auth)

    load_dotenv()
    app = build_ui()
    app.queue()
    app.launch(
        share=share_flag,
        server_name=args.host,
        server_port=args.port,
        auth=auth,
        show_api=False,
    )


if __name__ == "__main__":
    # quick sanity: ensure functions are defined
    for _name in ("run_query","suggest_circuit_ids","suggest_any_region"):
        assert callable(globals().get(_name)), f"{_name} is not defined"
    main()