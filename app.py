from __future__ import annotations
import os
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import pandas as pd

from api.fetch_football_data import build_merged_dataset
from api.smarkets_api import (
    fetch_events,
    enrich_events_with_competitors,
    fetch_event_detail,
    fetch_event_states,
    fetch_event_markets,
    fetch_market_contracts,
    fetch_last_executed_prices,
    fetch_quotes,
)
from api.analytics import (
    load_dataset,
    build_match_insights,
)

load_dotenv()  # Load .env in local dev
APP = FastAPI(title="MatchScreener API")

# Cache TTL settings (seconds) - overridable via environment variables
_EVENTS_TTL_SECS: int = int(os.environ.get("EVENTS_TTL_SECS", "60"))
_STATES_TTL_SECS: int = int(os.environ.get("STATES_TTL_SECS", "15"))
_ODDS_TTL_SECS: int = int(os.environ.get("ODDS_TTL_SECS", "5"))
_QUOTES_TTL_SECS: int = int(os.environ.get("QUOTES_TTL_SECS", "2"))
_ANALYTICS_TTL_SECS: int = int(os.environ.get("ANALYTICS_TTL_SECS", "300"))

DATA_PATH = os.environ.get("DATA_PATH", "data/matches_v1.parquet")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN", "")

def _check_authorization(authorization: Optional[str]):
    # If a static refresh token is set, validate equality against Bearer header
    if REFRESH_TOKEN:
        expected = f"Bearer {REFRESH_TOKEN}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return
    # No protection configured: allow
    return

@APP.post("/refresh")
def refresh(authorization: Optional[str] = Header(default=None)):
    _check_authorization(authorization)
    # Build dataset (current + last season with latest overlay)
    df = build_merged_dataset()
    # Persist to Parquet for quick reads; ensure directory exists
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    df.to_parquet(DATA_PATH, index=False)
    return {"rows": int(len(df)), "path": DATA_PATH}

# In-memory cache for events list + enrichment
_EVENTS_CACHE: Dict[str, Dict[str, Any]] = {}

@APP.get("/api/events")
def api_events(day: Optional[str] = None):
    try:
        # Cache key based on query params
        key = f"day:{day or '*'}"
        now = datetime.utcnow().timestamp()
        cached = _EVENTS_CACHE.get(key)
        if cached and (now - cached.get("ts", 0)) < _EVENTS_TTL_SECS:
            age = int(now - cached.get("ts", 0))
            return JSONResponse(
                content=cached["data"],
                headers={
                    "Cache-Control": f"public, max-age={_EVENTS_TTL_SECS}",
                    "X-Cache-TTL": str(_EVENTS_TTL_SECS),
                    "Age": str(age),
                    "X-Cache": "HIT",
                },
            )
        result = fetch_events(day=day, limit=300)
        events = result.get("events", [])
        events = enrich_events_with_competitors(events)
        # Fetch market IDs for Winner 3-way and Correct Score
        ids = [str(e.get("id")) for e in events if e.get("id")]
        try:
            market_map = fetch_event_markets(ids)
        except Exception:
            market_map = {}
        enriched_markets = []
        for e in events:
            eid = str(e.get("id")) if e.get("id") else None
            mm = market_map.get(eid, {}) if eid else {}
            if mm:
                e = {**e, **mm}
            enriched_markets.append(e)
        events = enriched_markets

        # Require both Winner 3-way and Correct Score markets
        events = [e for e in events if e.get("winner_market_id") and e.get("correct_score_market_id")]

        # Collect market IDs to fetch contracts (WINNER_3_WAY + CORRECT_SCORE + OVER_UNDER 4.5)
        market_ids: list[str] = []
        for e in events:
            wm = e.get("winner_market_id")
            cs = e.get("correct_score_market_id")
            over_45_market_id = e.get("over_under_45_market_id")
            if wm:
                market_ids.append(str(wm))
            if cs:
                market_ids.append(str(cs))
            if over_45_market_id:
                market_ids.append(str(over_45_market_id))
        contract_map: Dict[str, Any] = {}
        if market_ids:
            try:
                contract_map = fetch_market_contracts(market_ids, include_hidden=True)
            except Exception:
                contract_map = {}

        # Identify required contracts per event
        enriched_contracts = []
        for e in events:
            home_name = (e.get("home_name") or e.get("home") or "").strip()
            away_name = (e.get("away_name") or e.get("away") or "").strip()
            # Winner 3-way contracts: use contract_type.name for robust mapping
            home_id = None
            draw_id = None
            away_id = None
            wm = e.get("winner_market_id")
            wm_contracts = contract_map.get(str(wm), []) if wm else []
            for c in wm_contracts:
                ctype = (c.get("contract_type") or {}).get("name")
                cid = str(c.get("id")) if c.get("id") is not None else None
                if not cid or not ctype:
                    continue
                ctype = ctype.upper()
                if ctype == "HOME" and home_id is None:
                    home_id = cid
                elif ctype == "DRAW" and draw_id is None:
                    draw_id = cid
                elif ctype == "AWAY" and away_id is None:
                    away_id = cid
                if home_id and draw_id and away_id:
                    break

            # Correct score: any-other outcomes
            any_other_home_id = None
            any_other_away_id = None
            any_other_draw_id = None
            cs = e.get("correct_score_market_id")
            cs_contracts = contract_map.get(str(cs), []) if cs else []
            for c in cs_contracts:
                cid = str(c.get("id")) if c.get("id") is not None else None
                if not cid:
                    continue
                ctype = (c.get("contract_type") or {}).get("name") or ""
                slug = (c.get("slug") or "").lower().strip()
                name = (c.get("name") or "").lower().strip()
                t = ctype.upper().strip()
                if t == "ANY_OTHER_HOME_WIN" or slug == "any-other-home-win" or "any other home win" in name:
                    any_other_home_id = cid
                elif t == "ANY_OTHER_AWAY_WIN" or slug == "any-other-away-win" or "any other away win" in name:
                    any_other_away_id = cid
                elif t == "ANY_OTHER_DRAW" or slug == "any-other-draw" or "any other draw" in name:
                    any_other_draw_id = cid

            # Over/Under 4.5
            over_45_contract_id = None
            over_45_market_id = e.get("over_under_45_market_id")
            over_45_contracts = contract_map.get(str(over_45_market_id), []) if over_45_market_id else []
            for c in over_45_contracts:
                cid = str(c.get("id")) if c.get("id") is not None else None
                if not cid:
                    continue
                ctype = (c.get("contract_type") or {}).get("name") or ""
                t = ctype.upper().strip()
                if t == "OVER":
                    over_45_contract_id = cid

            enriched_contracts.append({
                **e,
                "winner_contract_home_id": home_id,
                "winner_contract_draw_id": draw_id,
                "winner_contract_away_id": away_id,
                "correct_score_any_other_home_win_contract_id": any_other_home_id,
                "correct_score_any_other_away_win_contract_id": any_other_away_id,
                "correct_score_any_other_draw_contract_id": any_other_draw_id,
                "over_45_contract_id": over_45_contract_id,
            })
        events = enriched_contracts
        data = {"count": len(events), "events": events}
        _EVENTS_CACHE[key] = {"ts": now, "data": data}
        return JSONResponse(
            content=data,
            headers={
                "Cache-Control": f"public, max-age={_EVENTS_TTL_SECS}",
                "X-Cache-TTL": str(_EVENTS_TTL_SECS),
                "Age": "0",
                "X-Cache": "MISS",
            },
        )
    except Exception:
        raise HTTPException(status_code=502, detail="Failed to fetch Smarkets events")


# Simple in-memory cache for states with TTL
_STATES_CACHE: Dict[str, Dict[str, Any]] = {}


@APP.get("/api/states")
def api_states(ids: str):
    if not ids:
        raise HTTPException(status_code=422, detail="ids parameter is required, e.g. ids=1,2,3")
    id_list = [s.strip() for s in ids.split(",") if s.strip()]
    if not id_list:
        raise HTTPException(status_code=422, detail="No valid event IDs provided")

    key = ",".join(sorted(id_list))
    now = datetime.utcnow().timestamp()
    cached = _STATES_CACHE.get(key)
    if cached and (now - cached.get("ts", 0)) < _STATES_TTL_SECS:
        age = int(now - cached.get("ts", 0))
        return JSONResponse(
            content=cached["data"],
            headers={
                "Cache-Control": f"public, max-age={_STATES_TTL_SECS}",
                "X-Cache-TTL": str(_STATES_TTL_SECS),
                "Age": str(age),
                "X-Cache": "HIT",
            },
        )

    try:
        data = fetch_event_states(id_list)
    except Exception:
        raise HTTPException(status_code=502, detail="Failed to fetch Smarkets states")

    _STATES_CACHE[key] = {"ts": now, "data": data}
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": f"public, max-age={_STATES_TTL_SECS}",
            "X-Cache-TTL": str(_STATES_TTL_SECS),
            "Age": "0",
            "X-Cache": "MISS",
        },
    )

# Simple in-memory cache for odds with short TTL
_ODDS_CACHE: Dict[str, Dict[str, Any]] = {}
_QUOTES_CACHE: Dict[str, Dict[str, Any]] = {}


@APP.get("/api/odds")
def api_odds(market_ids: str, contract_ids: Optional[str] = None):
    """
    Returns last executed prices for the provided market IDs (≤100 per batch).
    Optionally filters to the provided contract IDs.

    - market_ids: comma-separated list of market IDs
    - contract_ids: comma-separated list of contract IDs to include (optional)
    """
    if not market_ids:
        raise HTTPException(status_code=422, detail="market_ids parameter is required, e.g. market_ids=1,2,3")
    mids = [s.strip() for s in market_ids.split(",") if s.strip()]
    if not mids:
        raise HTTPException(status_code=422, detail="No valid market IDs provided")

    cids_set = None
    if contract_ids:
        cids = [s.strip() for s in contract_ids.split(",") if s.strip()]
        if cids:
            cids_set = set(cids)

    # Cache key includes filters
    cache_key = f"m:{','.join(sorted(mids))}|c:{','.join(sorted(cids_set)) if cids_set else '*'}"
    now = datetime.utcnow().timestamp()
    cached = _ODDS_CACHE.get(cache_key)
    if cached and (now - cached.get("ts", 0)) < _ODDS_TTL_SECS:
        age = int(now - cached.get("ts", 0))
        return JSONResponse(
            content=cached["data"],
            headers={
                "Cache-Control": f"public, max-age={_ODDS_TTL_SECS}",
                "X-Cache-TTL": str(_ODDS_TTL_SECS),
                "Age": str(age),
                "X-Cache": "HIT",
            },
        )

    try:
        price_map = fetch_last_executed_prices(mids)
    except Exception:
        raise HTTPException(status_code=502, detail="Failed to fetch last executed prices")

    # Filter to desired contracts, preserving raw price entries
    filtered: Dict[str, Dict[str, Any]] = {}
    for mid, by_contract in (price_map or {}).items():
        if not isinstance(by_contract, dict):
            continue
        if cids_set:
            sub = {cid: obj for cid, obj in by_contract.items() if cid in cids_set}
        else:
            sub = by_contract
        # Normalize last price: 100 / value (percent) to decimal odds, 2dp; 0 -> empty
        norm_sub: Dict[str, Any] = {}
        for cid, obj in (sub or {}).items():
            val = obj.get("last_executed_price")
            norm_val = None
            try:
                f = float(val) if val is not None else None
                if f and f > 0:
                    norm_val = round(100.0 / f, 2)
            except Exception:
                norm_val = None
            norm_sub[cid] = {
                "last_decimal": norm_val,
                "last_executed_price": val,
                "raw": obj,
            }
        filtered[mid] = norm_sub

    data = {"count": len(filtered), "prices": filtered}
    _ODDS_CACHE[cache_key] = {"ts": now, "data": data}
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": f"public, max-age={_ODDS_TTL_SECS}",
            "X-Cache-TTL": str(_ODDS_TTL_SECS),
            "Age": "0",
            "X-Cache": "MISS",
        },
    )


@APP.get("/api/quotes")
def api_quotes(market_ids: str, contract_ids: str):
    """
    Returns live quotes (best offer and smallest bid) for provided market IDs.
    Accepts up to 200 market IDs per batch; requires contract IDs to filter.
    """
    if not market_ids:
        raise HTTPException(status_code=422, detail="market_ids parameter is required, e.g. market_ids=1,2,3")
    mids = [s.strip() for s in market_ids.split(",") if s.strip()]
    if not mids:
        raise HTTPException(status_code=422, detail="No valid market IDs provided")

    if not contract_ids:
        raise HTTPException(status_code=422, detail="contract_ids parameter is required, e.g. contract_ids=10,11,12")
    cids = [s.strip() for s in contract_ids.split(",") if s.strip()]
    if not cids:
        raise HTTPException(status_code=422, detail="No valid contract IDs provided")
    cids_set = set(cids)

    cache_key = f"quotes:m:{','.join(sorted(mids))}|c:{','.join(sorted(cids_set))}"
    now = datetime.utcnow().timestamp()
    cached = _QUOTES_CACHE.get(cache_key)
    if cached and (now - cached.get("ts", 0)) < _QUOTES_TTL_SECS:
        age = int(now - cached.get("ts", 0))
        return JSONResponse(
            content=cached["data"],
            headers={
                "Cache-Control": f"public, max-age={_QUOTES_TTL_SECS}",
                "X-Cache-TTL": str(_QUOTES_TTL_SECS),
                "Age": str(age),
                "X-Cache": "HIT",
            },
        )

    try:
        quote_by_contract = fetch_quotes(mids)
    except Exception:
        raise HTTPException(status_code=502, detail="Failed to fetch quotes")

    # Filter strictly to desired contract IDs; return mapping keyed by contract_id
    filtered = {cid: obj for cid, obj in (quote_by_contract or {}).items() if cid in cids_set}

    data = {"count": len(filtered), "quotes": filtered}
    _QUOTES_CACHE[cache_key] = {"ts": now, "data": data}
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": f"public, max-age={_QUOTES_TTL_SECS}",
            "X-Cache-TTL": str(_QUOTES_TTL_SECS),
            "Age": "0",
            "X-Cache": "MISS",
        },
    )

 

# resolve-teams endpoint removed; normalization handled within analytics endpoints.

# Analytics: per-match insights (teams + h2h)
_INSIGHTS_CACHE: Dict[str, Dict[str, Any]] = {}

@APP.get("/api/analytics/match-insights")
def api_match_insights(ids: str, debug_examples: bool = False):
    if not ids:
        raise HTTPException(status_code=422, detail="ids parameter is required, e.g. ids=1,2")
    id_list = [s.strip() for s in ids.split(",") if s.strip()]
    if not id_list:
        raise HTTPException(status_code=422, detail="No valid event IDs provided")

    key = f"ids:{','.join(sorted(id_list))}"
    now = datetime.utcnow().timestamp()
    cached = _INSIGHTS_CACHE.get(key)
    if cached and (now - cached.get("ts", 0)) < _ANALYTICS_TTL_SECS:
        age = int(now - cached.get("ts", 0))
        return JSONResponse(
            content=cached["data"],
            headers={
                "Cache-Control": f"public, max-age={_ANALYTICS_TTL_SECS}",
                "X-Cache-TTL": str(_ANALYTICS_TTL_SECS),
                "Age": str(age),
                "X-Cache": "HIT",
            },
        )

    # Resolve teams + details
    stub = [{"id": i} for i in id_list]
    try:
        enriched = enrich_events_with_competitors(stub)
    except Exception:
        enriched = stub
    details_map: Dict[str, Dict[str, Any]] = {}
    for eid in id_list:
        try:
            details_map[str(eid)] = fetch_event_detail(eid)
        except Exception:
            details_map[str(eid)] = {"id": eid}

    df = load_dataset(DATA_PATH)
    # Optional per-request debug toggle for 'Others' examples
    prev_debug = os.environ.get("INSIGHTS_DEBUG_EXAMPLES")
    if debug_examples:
        os.environ["INSIGHTS_DEBUG_EXAMPLES"] = "1"
    out = []
    try:
        for e in enriched:
            eid = str(e.get("id"))
            det = details_map.get(eid, {})
            full_slug = det.get("full_slug")
            home = e.get("home_name") or e.get("home")
            away = e.get("away_name") or e.get("away")
            if not home or not away or df.empty:
                out.append({"event_id": eid, "insights": None, "note": "missing teams or dataset"})
                continue
            try:
                from api.fetch_football_data import normalize_team_name
                home_norm = normalize_team_name(str(home))
                away_norm = normalize_team_name(str(away))
            except Exception:
                home_norm = str(home).lower().strip()
                away_norm = str(away).lower().strip()
            # Use full merged dataset (current + last season); H2H ignores league
            insights = build_match_insights(df, home_norm, away_norm, full_slug, max_matches=None, h2h_max=None)
            # Compute score via Poisson-based 0-0 probability from blended expected goals
            score = None
            zero_zero_prob_pct = None
            try:
                import math
                home_blk = insights.get("home") or {}
                away_blk = insights.get("away") or {}
                h2h_blk = insights.get("h2h") or {}
                league_blocks = insights.get("league_scope") or []
                def _clamp_goals(v):
                    try:
                        f = float(v)
                        if not pd.isna(f):
                            return max(0.2, min(f, 3.0))
                    except Exception:
                        pass
                    return None
                home_g = _clamp_goals(home_blk.get("avg_goals_scored"))
                away_g = _clamp_goals(away_blk.get("avg_goals_scored"))
                league_avg = None
                if isinstance(league_blocks, list) and len(league_blocks) > 0:
                    first = league_blocks[0] or {}
                    league_avg = first.get("avg_total_goals")
                    try:
                        league_avg = float(league_avg)
                    except Exception:
                        league_avg = None
                h2h_avg = h2h_blk.get("avg_total_goals")
                try:
                    h2h_avg = float(h2h_avg)
                except Exception:
                    h2h_avg = None
                # Blend expected total goals (lambda)
                comp_sum = None
                if home_g is not None and away_g is not None:
                    comp_sum = home_g + away_g
                # Fallbacks if components missing
                if comp_sum is None and league_avg is not None:
                    comp_sum = league_avg
                if comp_sum is None:
                    comp_sum = None
                ctx_avg = h2h_avg if h2h_avg is not None else (league_avg if league_avg is not None else None)
                if comp_sum is not None:
                    lam = 0.6 * comp_sum + 0.2 * (league_avg if league_avg is not None else comp_sum) + 0.2 * (ctx_avg if ctx_avg is not None else (league_avg if league_avg is not None else comp_sum))
                    lam = max(0.2, min(lam, 5.0))
                    p0 = math.exp(-lam)
                    zero_zero_prob_pct = round(100.0 * p0, 1)
                    score = int(round(100.0 - 200.0 * p0))
                    score = max(0, min(score, 100))
            except Exception:
                score = None
                zero_zero_prob_pct = None
            # Always append result (even if score calc failed) with h2h matches and team codes
            out.append({
                "event_id": eid,
                "name": det.get("name"),
                "start_datetime": det.get("start_datetime"),
                "home_name": home,
                "away_name": away,
                "home_code": e.get("home_code"),
                "away_code": e.get("away_code"),
                "league_div": insights.get("league_div"),
                "status": insights.get("status"),
                "home": insights.get("home"),
                "away": insights.get("away"),
                "h2h": insights.get("h2h"),
                "h2h_matches": insights.get("h2h_matches"),
                "league_scope": insights.get("league_scope"),
                "score": score,
                "zero_zero_prob_pct": zero_zero_prob_pct,
            })
    finally:
        # Restore previous debug setting to avoid leaking across requests
        if prev_debug is None:
            os.environ.pop("INSIGHTS_DEBUG_EXAMPLES", None)
        else:
            os.environ["INSIGHTS_DEBUG_EXAMPLES"] = prev_debug

    data = {"count": len(out), "results": out}
    _INSIGHTS_CACHE[key] = {"ts": now, "data": data}
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": f"public, max-age={_ANALYTICS_TTL_SECS}",
            "X-Cache-TTL": str(_ANALYTICS_TTL_SECS),
            "Age": "0",
            "X-Cache": "MISS",
        },
    )


# odds-highlevel analytics removed per request


# league-mapping removed; internal-only.

# Admin: export dataset as CSV for offline analysis
@APP.get("/api/admin/export")
def api_admin_export(format: str = "csv", div: Optional[str] = None, authorization: Optional[str] = Header(default=None)):
    _check_authorization(authorization)
    df = load_dataset(DATA_PATH)
    if df.empty:
        raise HTTPException(status_code=503, detail="Dataset not available. Run /refresh first.")
    sub = df.copy()
    if div:
        try:
            sub = sub[sub["Div"].astype(str) == str(div)]
        except Exception:
            pass
    # Always return full dataset (optionally filtered by div)

    fmt = (format or "csv").strip().lower()
    if fmt == "csv":
        import io
        buf = io.StringIO()
        try:
            sub.to_csv(buf, index=False)
        except Exception:
            # Fallback: select common columns if complex dtypes fail
            cols = [c for c in sub.columns if c not in {"raw"}]
            sub[cols].to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers={
            "Content-Disposition": "attachment; filename=matches.csv"
        })
    elif fmt in {"xlsx", "excel"}:
        # Excel requires openpyxl; attempt and error if missing
        try:
            import io
            import pandas as pd  # ensure pandas is available
            buf = io.BytesIO()
            sub.to_excel(buf, index=False)
            buf.seek(0)
            return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
                "Content-Disposition": "attachment; filename=matches.xlsx"
            })
        except Exception:
            raise HTTPException(status_code=501, detail="Excel export not available (openpyxl not installed). Use format=csv.")
    else:
        raise HTTPException(status_code=400, detail="Unsupported format. Use format=csv or format=xlsx")


@APP.get("/api/verify-data")
def verify_data():
    """
    Verification endpoint to check if the data file is accessible.
    Returns detailed diagnostics for troubleshooting.
    """
    import os
    import sys
    
    response = {
        "data_path_env": DATA_PATH,
        "file_exists": os.path.exists(DATA_PATH),
        "file_size_bytes": None,
        "sample_rows": [],
        "total_rows": 0,
        "columns": [],
        "python_version": sys.version,
        "pandas_version": pd.__version__,
    }
    
    # Try to get pyarrow version if available
    try:
        import pyarrow as pa
        response["pyarrow_version"] = pa.__version__
    except ImportError:
        response["pyarrow_version"] = "not installed"
    
    if os.path.exists(DATA_PATH):
        try:
            response["file_size_bytes"] = os.path.getsize(DATA_PATH)
            response["file_size_kb"] = round(response["file_size_bytes"] / 1024, 1)
        except Exception as e:
            response["file_size_error"] = str(e)
        
        # Detect file type
        if DATA_PATH.endswith('.csv.gz'):
            response["file_type"] = "CSV (gzip compressed)"
        elif DATA_PATH.endswith('.csv'):
            response["file_type"] = "CSV"
        elif DATA_PATH.endswith('.parquet'):
            response["file_type"] = "Parquet"
        else:
            response["file_type"] = "Unknown"
        
        # Try loading with load_dataset
        try:
            df = load_dataset(DATA_PATH)
            response["total_rows"] = len(df)
            response["columns"] = list(df.columns)
            response["read_success"] = True
            
            if len(df) > 0:
                # Get first 3 rows as sample
                sample_df = df.head(3)
                response["sample_rows"] = sample_df.to_dict(orient="records")
                
                # Add some data quality checks
                response["data_quality"] = {
                    "has_date_column": "date" in df.columns,
                    "has_team_columns": "home_norm" in df.columns and "away_norm" in df.columns,
                    "date_range": None,
                }
                
                if "date" in df.columns:
                    try:
                        response["data_quality"]["date_range"] = {
                            "min": str(df["date"].min()),
                            "max": str(df["date"].max()),
                        }
                    except Exception:
                        pass
            else:
                response["warning"] = "File loaded but contains 0 rows"
                
        except Exception as e:
            response["read_error"] = str(e)
            response["read_success"] = False
            import traceback
            response["traceback"] = traceback.format_exc()
    else:
        response["error"] = f"File not found at {DATA_PATH}"
        # List available data files
        try:
            if os.path.exists("data"):
                files = os.listdir("data")
                response["available_data_files"] = [f for f in files if f.endswith(('.parquet', '.csv', '.csv.gz'))]
        except Exception:
            pass
    
    return response


# Mount static last to avoid intercepting API routes
APP.mount("/", StaticFiles(directory="web", html=True), name="static")
