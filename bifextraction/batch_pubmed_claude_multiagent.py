#!/usr/bin/env python3
# batch_pubmed_claude_multiagent.py
# Multi-agent system using Claude API for neuroscience projection extraction

import os, time, csv, argparse, json, random
from typing import List, Dict, Any, Tuple, Set
import requests
import anthropic

from prompts_claude_multiagent import (
    REGION_EXTRACTOR_SYSTEM, REGION_EXTRACTOR_USER, REGION_EXTRACTOR_SCHEMA,
    PROJECTION_EXTRACTOR_SYSTEM, PROJECTION_EXTRACTOR_USER, PROJECTION_EXTRACTOR_SCHEMA,
    METHOD_CLASSIFIER_SYSTEM, METHOD_CLASSIFIER_USER, METHOD_CLASSIFIER_SCHEMA
)

APP_NAME = "np-loop-claude"
HEADERS_JSON = {"Accept": "application/json"}

# ============================================================================
# HTTP Utilities (unchanged from original)
# ============================================================================

def backoff_sleep(base=0.6, factor=1.8, jitter=0.25, attempt=0):
    import random, time
    t = base * (factor ** attempt) + random.uniform(0, jitter)
    time.sleep(min(t, 8.0))

def eutils_get(url: str, params: dict, max_attempts: int = 6):
    for att in range(max_attempts):
        try:
            r = requests.get(url, params=params, headers=HEADERS_JSON, timeout=40)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                backoff_sleep(attempt=att)
                continue
            r.raise_for_status()
            return r
        except Exception:
            backoff_sleep(attempt=att)
    return None

def safe_json(url: str, params: dict, attempts: int = 5, sleep_base: float = 0.6):
    for att in range(attempts):
        r = eutils_get(url, params)
        if r is None:
            time.sleep(sleep_base * (att + 1))
            continue
        ct = (r.headers.get("content-type") or "").lower()
        try:
            return r.json()
        except Exception:
            snippet = (r.text or "")[:180].replace("\n", "\\n")
            print(f"[safe_json] Non-JSON response (ct={ct}). retry {att+1}/{attempts}. head={snippet}")
            time.sleep(sleep_base * (att + 1))
            continue
    return {}

# ============================================================================
# PubMed Utilities (unchanged)
# ============================================================================

def esearch_count(query: str, email: str, api_key: str, mindate=None, maxdate=None, datetype="pdat") -> int:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {"db": "pubmed", "term": query, "retmode": "json", "retmax": 0, "email": email, "tool": APP_NAME}
    if api_key:
        params["api_key"] = api_key
    if mindate and maxdate:
        params.update({"mindate": mindate, "maxdate": maxdate, "datetype": datetype})
    j = safe_json(url, params)
    return int(j.get("esearchresult", {}).get("count", "0"))

def esearch_ids(query: str, email: str, api_key: str, retstart: int, retmax: int,
                mindate=None, maxdate=None, datetype="pdat") -> List[str]:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {"db": "pubmed", "term": query, "retmode": "json", "retmax": retmax, 
              "retstart": retstart, "email": email, "tool": APP_NAME}
    if api_key:
        params["api_key"] = api_key
    if mindate and maxdate:
        params.update({"mindate": mindate, "maxdate": maxdate, "datetype": datetype})
    data = safe_json(url, params)
    return data.get("esearchresult", {}).get("idlist", [])

def esummary_details(ids: List[str], email: str, api_key: str) -> List[Dict[str, str]]:
    if not ids:
        return []
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    out = []
    for i in range(0, len(ids), 200):
        chunk = ",".join(ids[i:i+200])
        params = {"db": "pubmed", "id": chunk, "retmode": "json", "email": email, "tool": APP_NAME}
        if api_key:
            params["api_key"] = api_key
        res = safe_json(url, params).get("result", {})
        for k, v in res.items():
            if k == "uids":
                continue
            out.append({
                "pmid": k,
                "title": v.get("title", ""),
                "journal": v.get("fulljournalname", ""),
                "year": (v.get("pubdate", "") or "").split(" ")[0],
                "doi": "",
            })
    return out

def europepmc_links(pmid: str) -> Dict[str, str]:
    base = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    params = {"query": f"EXT_ID:{pmid} AND SRC:MED", "format": "json"}
    try:
        r = requests.get(base, params=params, timeout=20)
        if r.status_code != 200:
            return {}
        links = {}
        for hit in r.json().get("resultList", {}).get("result", []):
            if "pmcid" in hit:
                links["pmcid"] = hit["pmcid"]
            if "doi" in hit:
                links["doi"] = hit["doi"]
            if "fullTextUrlList" in hit:
                for u in hit["fullTextUrlList"]["fullTextUrl"]:
                    url = u.get("url", "")
                    style = u.get("documentStyle", "")
                    avail = u.get("availability", "")
                    if url.endswith(".pdf") or "pdf" in (style+avail).lower():
                        links["pdf"] = url
                    elif url:
                        links["html"] = url
        return links
    except Exception:
        return {}

def pdf_bytes_to_text(pdf_bytes: bytes) -> str:
    try:
        from io import BytesIO
        from pdfminer.high_level import extract_text
        from pdfminer.layout import LAParams
        laparams = LAParams(line_margin=0.2, word_margin=0.1, char_margin=2.0)
        return extract_text(BytesIO(pdf_bytes), laparams=laparams) or ""
    except Exception:
        return ""

def html_to_text(html: str) -> str:
    import re
    from html import unescape
    html = re.sub(r'<(script|style)[\s\S]*?</\1>', ' ', html, flags=re.I)
    html = re.sub(r'</?(br|p|div|li|tr|h\d)>', '\n', html, flags=re.I)
    text = re.sub(r'<[^>]+>', ' ', html)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_pmc_pdf_text(pmcid: str) -> str:
    try:
        url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf"
        r = requests.get(url, timeout=25)
        if r.status_code == 200 and r.headers.get("content-type", "").lower().startswith("application/pdf"):
            return pdf_bytes_to_text(r.content)
    except Exception:
        pass
    return ""

def fetch_url_text(url: str) -> str:
    try:
        r = requests.get(url, timeout=25)
        ct = r.headers.get("content-type", "").lower()
        if "pdf" in ct:
            return pdf_bytes_to_text(r.content)
        if "html" in ct or "xml" in ct or url.endswith(".html"):
            return html_to_text(r.text)
    except Exception:
        return ""
    return ""

def resolve_text_for_pmid(pmid: str, email: str, api_key: str) -> Dict[str, str]:
    info = {"text": "", "doi": "", "pmcid": ""}
    links = europepmc_links(pmid)
    if "pmcid" in links:
        t = get_pmc_pdf_text(links["pmcid"])
        if t:
            info.update({"text": t, "pmcid": links["pmcid"], "doi": links.get("doi", "")})
            return info
    for k in ["pdf", "html"]:
        if links.get(k):
            t = fetch_url_text(links[k])
            if t:
                info.update({"text": t, "pmcid": links.get("pmcid", ""), "doi": links.get("doi", "")})
                return info
    # Fallback: abstract only
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {"db": "pubmed", "id": pmid, "retmode": "text", "rettype": "abstract", 
              "email": email, "tool": APP_NAME}
    if api_key:
        params["api_key"] = api_key
    r = eutils_get(base, params)
    info["text"] = r.text if (r and r.status_code == 200) else ""
    return info

# ============================================================================
# Shard Planner (unchanged)
# ============================================================================

def plan_shards(query: str, email: str, api_key: str, start_year: int, end_year: int,
                cap: int = 9000) -> List[Tuple[int, int, int]]:
    """Recursively split [start_year, end_year] into shards with <= cap hits each."""
    def count_range(y0, y1):
        return esearch_count(query, email, api_key, mindate=str(y0), maxdate=str(y1), datetype="pdat")
    
    def split(y0, y1):
        c = count_range(y0, y1)
        if c <= cap:
            return [(y0, y1, c)]
        mid = (y0 + y1) // 2
        if mid == y0:
            return [(y0, y1, c)]
        return split(y0, mid) + split(mid+1, y1)
    
    return split(start_year, end_year)

# ============================================================================
# Claude API Multi-Agent Extraction
# ============================================================================

def claude_extract_multiagent(
    text: str, 
    meta: Dict[str, str], 
    region_hints: str,
    model: str = "claude-sonnet-4-20250514",
    temperature: float = 0.0,
    api_key: str = None
) -> List[Dict[str, Any]]:
    """
    Three-stage Claude multi-agent extraction:
    1. Region Extractor Agent
    2. Projection Extractor Agent  
    3. Method & Taxon Classifier Agent
    
    Returns final list of classified projections.
    """
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")
    
    client = anthropic.Anthropic(api_key=api_key)
    
    # Truncate text to fit context window (Claude can handle more, but be conservative)
    text_truncated = text[:80000]  # Claude Sonnet 4 has 200k context, use ~80k for safety
    
    print(f"  [Agent 1/3] Extracting brain regions...")
    
    # ========== AGENT 1: Region Extraction ==========
    try:
        region_user = REGION_EXTRACTOR_USER.format(
            title=meta.get("title", ""),
            journal=meta.get("journal", ""),
            year=meta.get("year", ""),
            pmid=meta.get("pmid", ""),
            region_hints=region_hints,
            text=text_truncated
        )
        
        region_response = client.messages.create(
            model=model,
            max_tokens=8000,
            temperature=temperature,
            system=REGION_EXTRACTOR_SYSTEM,
            messages=[{"role": "user", "content": region_user}],
            tools=[{
                "name": "RegionExtraction",
                "description": "Extract brain regions from text",
                "input_schema": REGION_EXTRACTOR_SCHEMA["input_schema"]
            }],
            tool_choice={"type": "tool", "name": "RegionExtraction"}
        )
        
        # Parse tool use response
        regions_data = None
        for block in region_response.content:
            if block.type == "tool_use" and block.name == "RegionExtraction":
                regions_data = block.input
                break
        
        if not regions_data or "regions" not in regions_data:
            print(f"  [Agent 1] No regions extracted, skipping paper.")
            return []
        
        regions = regions_data["regions"]
        print(f"  [Agent 1] Extracted {len(regions)} regions")
        
    except Exception as e:
        print(f"  [Agent 1] Region extraction failed: {e}")
        return []
    
    # ========== AGENT 2: Projection Extraction ==========
    print(f"  [Agent 2/3] Extracting projections...")
    
    try:
        projection_user = PROJECTION_EXTRACTOR_USER.format(
            title=meta.get("title", ""),
            pmid=meta.get("pmid", ""),
            regions_json=json.dumps(regions, indent=2, ensure_ascii=False),
            text=text_truncated
        )
        
        projection_response = client.messages.create(
            model=model,
            max_tokens=16000,
            temperature=temperature,
            system=PROJECTION_EXTRACTOR_SYSTEM,
            messages=[{"role": "user", "content": projection_user}],
            tools=[{
                "name": "ProjectionExtraction",
                "description": "Extract neural projections from text",
                "input_schema": PROJECTION_EXTRACTOR_SCHEMA["input_schema"]
            }],
            tool_choice={"type": "tool", "name": "ProjectionExtraction"}
        )
        
        projections_data = None
        for block in projection_response.content:
            if block.type == "tool_use" and block.name == "ProjectionExtraction":
                projections_data = block.input
                break
        
        if not projections_data or "projections" not in projections_data:
            print(f"  [Agent 2] No projections extracted.")
            return []
        
        projections = projections_data["projections"]
        print(f"  [Agent 2] Extracted {len(projections)} projections")
        
    except Exception as e:
        print(f"  [Agent 2] Projection extraction failed: {e}")
        return []
    
    # ========== AGENT 3: Method & Taxon Classification ==========
    print(f"  [Agent 3/3] Classifying methods and taxa...")
    
    try:
        classifier_user = METHOD_CLASSIFIER_USER.format(
            title=meta.get("title", ""),
            pmid=meta.get("pmid", ""),
            projections_json=json.dumps(projections, indent=2, ensure_ascii=False),
            text=text_truncated
        )
        
        classifier_response = client.messages.create(
            model=model,
            max_tokens=16000,
            temperature=temperature,
            system=METHOD_CLASSIFIER_SYSTEM,
            messages=[{"role": "user", "content": classifier_user}],
            tools=[{
                "name": "MethodTaxonClassification",
                "description": "Classify experimental methods and species",
                "input_schema": METHOD_CLASSIFIER_SCHEMA["input_schema"]
            }],
            tool_choice={"type": "tool", "name": "MethodTaxonClassification"}
        )
        
        classified_data = None
        for block in classifier_response.content:
            if block.type == "tool_use" and block.name == "MethodTaxonClassification":
                classified_data = block.input
                break
        
        if not classified_data or "classified_projections" not in classified_data:
            print(f"  [Agent 3] Classification failed, returning unclassified projections.")
            # Return projections without classification
            for p in projections:
                p["method"] = "Unspecified"
                p["method_confidence"] = 0.0
                p["taxon"] = "Unspecified"
                p["taxon_confidence"] = 0.0
            return projections
        
        classified_projections = classified_data["classified_projections"]
        print(f"  [Agent 3] Classified {len(classified_projections)} projections")
        
        return classified_projections
        
    except Exception as e:
        print(f"  [Agent 3] Method classification failed: {e}")
        # Return projections without classification
        for p in projections:
            p["method"] = "Unspecified"
            p["method_confidence"] = 0.0
            p["taxon"] = "Unspecified"
            p["taxon_confidence"] = 0.0
        return projections

# ============================================================================
# CSV Helpers (unchanged)
# ============================================================================

def ensure_header(path: str, header: List[str]):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=header).writeheader()

def count_rows(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)

# ============================================================================
# Main Loop
# ============================================================================

def main():
    ap = argparse.ArgumentParser(
        description="Claude multi-agent neuroscience projection extractor"
    )
    ap.add_argument("--email", default=os.environ.get("NCBI_EMAIL", ""), 
                    help="NCBI-required email")
    ap.add_argument("--ncbi_api_key", default=os.environ.get("NCBI_API_KEY", ""))
    ap.add_argument("--anthropic_api_key", default=os.environ.get("ANTHROPIC_API_KEY", ""))
    ap.add_argument("--query", required=True, help="PubMed search query")
    ap.add_argument("--out_csv", default="/content/out_pubmed_claude.csv")
    ap.add_argument("--model", default="claude-sonnet-4-20250514",
                    help="Claude model (claude-sonnet-4-20250514 or claude-sonnet-4-5-20250929)")
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--region_hints", 
                    default="M1,V1,CPu,MD,CA1,Thalamus,Cerebellum,STN,SNc,GPe,GPi,PPN,SMA,pre-SMA,PFC,Hippocampus,Putamen,Caudate")
    ap.add_argument("--target_rows", type=int, default=60100)
    ap.add_argument("--chunk_size", type=int, default=50)
    ap.add_argument("--state_json", default="/content/loop_state_claude.json")
    ap.add_argument("--sleep_base", type=float, default=1.0)
    ap.add_argument("--year_start", type=int, default=1950)
    ap.add_argument("--year_end", type=int, default=2025)
    args = ap.parse_args()

    assert args.email, "NCBI email is required"
    assert args.anthropic_api_key, "ANTHROPIC_API_KEY is required"

    header = ["sender", "receiver", "connection_flag", "reference", "journal", "DOI", 
              "Taxon", "Method", "Pointer", "Figure", "Section", "Confidence", 
              "Method_Confidence", "Neurotransmitter"]
    ensure_header(args.out_csv, header)

    # State management
    state = {"current_shard": 0, "retstart": 0, "shards": [], "processed_pmids": []}
    if os.path.exists(args.state_json):
        try:
            state.update(json.load(open(args.state_json, "r", encoding="utf-8")))
        except Exception:
            pass

    if not state.get("shards"):
        shards = plan_shards(args.query, args.email, args.ncbi_api_key, 
                           args.year_start, args.year_end, cap=9000)
        state["shards"] = [s for s in shards if s[2] > 0]
        state["current_shard"] = 0
        state["retstart"] = 0
        with open(args.state_json, "w", encoding="utf-8") as sf:
            json.dump(state, sf, ensure_ascii=False, indent=2)
        print(f"[Plan] {len(state['shards'])} shards planned.")

    current_rows = count_rows(args.out_csv)
    print(f"[Init] CSV rows: {current_rows} / target {args.target_rows}")

    seen_keys: Set[Tuple[str, str, str]] = set()

    while current_rows < args.target_rows and state["current_shard"] < len(state["shards"]):
        mindate, maxdate, shard_count = state["shards"][state["current_shard"]]
        print(f"\n[Shard {state['current_shard']+1}/{len(state['shards'])}] "
              f"range={mindate}..{maxdate} (count≈{shard_count}), retstart={state['retstart']}")

        ids = esearch_ids(args.query, args.email, args.ncbi_api_key, state["retstart"], 
                         args.chunk_size, mindate=mindate, maxdate=maxdate, datetype="pdat")
        
        if not ids:
            state["current_shard"] += 1
            state["retstart"] = 0
            with open(args.state_json, "w", encoding="utf-8") as sf:
                json.dump(state, sf, ensure_ascii=False, indent=2)
            continue

        summaries = esummary_details(ids, args.email, args.ncbi_api_key)
        
        with open(args.out_csv, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            
            for meta in summaries:
                pmid = meta.get("pmid", "")
                if pmid in state.get("processed_pmids", []):
                    continue

                print(f"\n[PMID {pmid}] Processing...")
                
                info = resolve_text_for_pmid(pmid, args.email, args.ncbi_api_key)
                text = info.get("text", "")
                
                if not text:
                    print(f"[PMID {pmid}] No text available, skipping.")
                    state["processed_pmids"].append(pmid)
                    time.sleep(args.sleep_base)
                    continue

                meta["doi"] = info.get("doi", "") or meta.get("doi", "") or ""
                meta["pmcid"] = info.get("pmcid", "")

                try:
                    classified_projections = claude_extract_multiagent(
                        text=text,
                        meta=meta,
                        region_hints=args.region_hints,
                        model=args.model,
                        temperature=args.temperature,
                        api_key=args.anthropic_api_key
                    )
                except Exception as e:
                    print(f"[PMID {pmid}] Claude extraction failed: {e}")
                    state["processed_pmids"].append(pmid)
                    time.sleep(args.sleep_base * 2)
                    continue

                ref = meta.get("title", "").strip()
                
                for proj in classified_projections:
                    sender = proj.get("sender", "")
                    receiver = proj.get("receiver", "")
                    pointer = proj.get("quote", "")
                    
                    key = (pmid, sender, receiver)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    w.writerow({
                        "sender": sender,
                        "receiver": receiver,
                        "connection_flag": proj.get("connection_flag", 1),
                        "reference": ref,
                        "journal": meta.get("journal", ""),
                        "DOI": meta.get("doi", ""),
                        "Taxon": proj.get("taxon", "Unspecified"),
                        "Method": proj.get("method", "Unspecified"),
                        "Pointer": pointer[:240],
                        "Figure": ", ".join(proj.get("figure_ids", [])),
                        "Section": proj.get("section", "Other"),
                        "Confidence": f"{proj.get('confidence', 0.0):.2f}",
                        "Method_Confidence": f"{proj.get('method_confidence', 0.0):.2f}",
                        "Neurotransmitter": proj.get("neurotransmitter", "")
                    })
                    current_rows += 1
                    
                    if current_rows >= args.target_rows:
                        print(f"\n[Reached] {args.target_rows} rows target.")
                        with open(args.state_json, "w", encoding="utf-8") as sf:
                            json.dump(state, sf, ensure_ascii=False, indent=2)
                        return

                state["processed_pmids"].append(pmid)
                time.sleep(args.sleep_base)

        state["retstart"] += args.chunk_size
        with open(args.state_json, "w", encoding="utf-8") as sf:
            json.dump(state, sf, ensure_ascii=False, indent=2)

    print(f"\n[Complete] CSV at {args.out_csv}, state at {args.state_json}")

if __name__ == "__main__":
    main()
