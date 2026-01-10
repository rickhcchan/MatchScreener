from __future__ import annotations
from datetime import datetime, timedelta
import os
from typing import Optional, Dict, Any, List
import requests

BASE_URL = "https://api.smarkets.com/v3/events/"
EVENT_DETAIL_URL_TMPL = "https://api.smarkets.com/v3/events/{event_id}/"
COMPETITORS_URL_TMPL = "https://api.smarkets.com/v3/events/{event_id}/competitors/"
COMPETITORS_URL_BATCH_TMPL = "https://api.smarkets.com/v3/events/{event_ids}/competitors/"
STATES_URL_BATCH_TMPL = "https://api.smarkets.com/v3/events/{event_ids}/states/"
MARKETS_URL_BATCH_TMPL = "https://api.smarkets.com/v3/events/{event_ids}/markets/"
CONTRACTS_URL_BATCH_TMPL = "https://api.smarkets.com/v3/markets/{market_ids}/contracts/"
LAST_PRICES_URL_BATCH_TMPL = "https://api.smarkets.com/v3/markets/{market_ids}/last_executed_prices/"
QUOTES_URL_BATCH_TMPL = "https://api.smarkets.com/v3/markets/{market_ids}/quotes/"


def _iso_date_start_end(day: Optional[str] = None) -> tuple[str, str]:
    if day:
        d = datetime.strptime(day, "%Y-%m-%d")
    else:
        d = datetime.utcnow()
    start = datetime(d.year, d.month, d.day, 0, 0, 0)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def fetch_events(day: Optional[str] = None, limit: int = 300) -> Dict[str, Any]:
    start_min, start_max = _iso_date_start_end(day)
    params = {
        "inplay_enabled": "true",
        "state": ["new", "upcoming", "live"],
        "type": "football_match",
        "type_domain": "football",
        "type_scope": "single_event",
        "with_new_type": "true",
        "start_datetime_min": start_min,
        "start_datetime_max": start_max,
        "sort": "start_datetime,id",
        "limit": str(limit),
        "include_hidden": "false",
    }
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    events = data.get("events", [])
    normalized: List[Dict[str, Any]] = []
    for e in events:
        name = e.get("name", "")
        home, away = None, None
        if " vs " in name:
            parts = name.split(" vs ", 1)
            if len(parts) == 2:
                home, away = parts[0], parts[1]
        full_slug = e.get("full_slug")
        eid = e.get("id")
        event_url = None
        if eid and full_slug:
            # full_slug starts with /sport/... so we can append directly
            event_url = f"https://smarkets.com/event/{eid}{full_slug}"
        normalized.append({
            "id": eid,
            "name": name,
            "home": home,
            "away": away,
            "start_datetime": e.get("start_datetime"),
            "state": e.get("state"),
            "type": e.get("type"),
            "full_slug": full_slug,
            "event_url": event_url,
            "raw": e,
        })
    return {"count": len(normalized), "events": normalized}


def fetch_event_detail(event_id: str) -> Dict[str, Any]:
    """
    Fetch a single event's details, including name, start time, state, type, and full_slug.
    """
    url = EVENT_DETAIL_URL_TMPL.format(event_id=str(event_id))
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    e = data.get("event") or data  # API may return { event: { ... } } or flat
    # Normalize common fields
    eid = e.get("id")
    name = e.get("name")
    full_slug = e.get("full_slug")
    start_datetime = e.get("start_datetime")
    state = e.get("state")
    etype = e.get("type")
    event_url = None
    if eid and full_slug:
        event_url = f"https://smarkets.com/event/{eid}{full_slug}"
    return {
        "id": eid,
        "name": name,
        "start_datetime": start_datetime,
        "state": state,
        "type": etype,
        "full_slug": full_slug,
        "event_url": event_url,
        "raw": e,
    }


def fetch_competitors(event_id: str) -> Dict[str, Any]:
    url = COMPETITORS_URL_TMPL.format(event_id=event_id)
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    comps = data.get("competitors", [])
    result: Dict[str, Any] = {
        "home_id": None,
        "home_name": None,
        "home_short": None,
        "home_code": None,
        "away_id": None,
        "away_name": None,
        "away_short": None,
        "away_code": None,
    }
    for c in comps:
        t = c.get("type")
        if t == "home":
            result["home_id"] = c.get("id")
            result["home_name"] = c.get("name")
            result["home_short"] = c.get("short_name")
            result["home_code"] = c.get("short_code")
        elif t == "away":
            result["away_id"] = c.get("id")
            result["away_name"] = c.get("name")
            result["away_short"] = c.get("short_name")
            result["away_code"] = c.get("short_code")
    return result

def enrich_events_with_competitors(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Batch competitor lookups up to 300 IDs per call
    def _chunk(lst: List[str], size: int) -> List[List[str]]:
        return [lst[i:i+size] for i in range(0, len(lst), size)]

    ids: List[str] = [str(e.get("id")) for e in events if e.get("id")]
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    # Map event_id -> competitor info
    comp_map: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunk(ids, 300):
        if not chunk:
            continue
        try:
            url = COMPETITORS_URL_BATCH_TMPL.format(event_ids=",".join(chunk))
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            comps = data.get("competitors", [])
            for c in comps:
                ev_id = str(c.get("event_id")) if c.get("event_id") is not None else None
                if not ev_id:
                    continue
                entry = comp_map.get(ev_id) or {
                    "home_id": None,
                    "home_name": None,
                    "home_short": None,
                    "home_code": None,
                    "away_id": None,
                    "away_name": None,
                    "away_short": None,
                    "away_code": None,
                }
                t = c.get("type")
                if t == "home":
                    entry["home_id"] = c.get("id")
                    entry["home_name"] = c.get("name")
                    entry["home_short"] = c.get("short_name")
                    entry["home_code"] = c.get("short_code")
                elif t == "away":
                    entry["away_id"] = c.get("id")
                    entry["away_name"] = c.get("name")
                    entry["away_short"] = c.get("short_name")
                    entry["away_code"] = c.get("short_code")
                comp_map[ev_id] = entry
        except Exception:
            # If batch fetch fails, fall back to per-event for this chunk
            for ev_id in chunk:
                try:
                    comp_map[ev_id] = fetch_competitors(ev_id)
                except Exception:
                    pass

    enriched: List[Dict[str, Any]] = []
    for e in events:
        eid = str(e.get("id")) if e.get("id") else None
        if eid and eid in comp_map:
            comp = comp_map[eid]
            merged = {**e, **comp}
            if comp.get("home_name"):
                merged["home"] = comp["home_name"]
            if comp.get("away_name"):
                merged["away"] = comp["away_name"]
            enriched.append(merged)
        else:
            enriched.append(e)
    return enriched


def fetch_event_markets(event_ids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    ids = [str(i) for i in event_ids if i]
    if not ids:
        return {}
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    params = {
        "sort": "event_id,display_order",
        "popular": "false",
        "market_types": "WINNER_3_WAY,CORRECT_SCORE,OVER_UNDER",
        "include_hidden": "false",
    }

    def _chunk(lst: List[str], size: int) -> List[List[str]]:
        return [lst[i:i+size] for i in range(0, len(lst), size)]

    out: Dict[str, Dict[str, Optional[str]]] = {}
    for chunk in _chunk(ids, 50):
        if not chunk:
            continue
        try:
            url = MARKETS_URL_BATCH_TMPL.format(event_ids=",".join(chunk))
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            markets = data.get("markets", []) or []
            for m in markets:
                ev_id = str(m.get("event_id")) if m.get("event_id") is not None else None
                if not ev_id:
                    continue
                mt_obj = m.get("market_type") or {}
                mt_name = (mt_obj.get("name") or "").upper()
                mt_param = str(mt_obj.get("param") or "")
                entry = out.get(ev_id) or {"winner_market_id": None, "correct_score_market_id": None, "over_under_25_market_id": None, "over_under_35_market_id": None, "over_under_45_market_id": None, "over_under_55_market_id": None, "over_under_65_market_id": None}
                if mt_name == "WINNER_3_WAY" and not entry["winner_market_id"]:
                    entry["winner_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                elif mt_name == "CORRECT_SCORE" and not entry["correct_score_market_id"]:
                    entry["correct_score_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                elif mt_name == "OVER_UNDER" and mt_param == "2.5" and not entry.get("over_under_25_market_id"):
                    entry["over_under_25_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                elif mt_name == "OVER_UNDER" and mt_param == "3.5" and not entry.get("over_under_35_market_id"):
                    entry["over_under_35_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                elif mt_name == "OVER_UNDER" and mt_param == "4.5" and not entry.get("over_under_45_market_id"):
                    entry["over_under_45_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                elif mt_name == "OVER_UNDER" and mt_param == "5.5" and not entry.get("over_under_55_market_id"):
                    entry["over_under_55_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                elif mt_name == "OVER_UNDER" and mt_param == "6.5" and not entry.get("over_under_65_market_id"):
                    entry["over_under_65_market_id"] = str(m.get("id")) if m.get("id") is not None else None
                out[ev_id] = entry
        except Exception:
            # Skip failing chunk but keep any results collected so far
            continue
    return out


def fetch_market_contracts(market_ids: List[str], include_hidden: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    ids = [str(i) for i in market_ids if i]
    if not ids:
        return {}
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    params = {
        "include_hidden": "true" if include_hidden else "false",
    }

    def _chunk(lst: List[str], size: int) -> List[List[str]]:
        return [lst[i:i+size] for i in range(0, len(lst), size)]

    out: Dict[str, List[Dict[str, Any]]] = {}
    for chunk in _chunk(ids, 100):
        if not chunk:
            continue
        try:
            url = CONTRACTS_URL_BATCH_TMPL.format(market_ids=",".join(chunk))
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            contracts = data.get("contracts", []) or []
            for c in contracts:
                mid = str(c.get("market_id")) if c.get("market_id") is not None else None
                if not mid:
                    continue
                lst = out.get(mid) or []
                lst.append(c)
                out[mid] = lst
        except Exception:
            continue
    return out


def fetch_last_executed_prices(market_ids: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Fetch last executed prices for up to 100 market IDs in each batch.
    Returns a nested mapping: market_id -> contract_id -> price_entry (raw object).

    This function does not interpret price fields; it passes through the raw
    entry to allow flexible client-side handling, since the API schema can vary.
    """
    ids = [str(i) for i in market_ids if i]
    if not ids:
        return {}
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    def _chunk(lst: List[str], size: int) -> List[List[str]]:
        return [lst[i:i+size] for i in range(0, len(lst), size)]

    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for chunk in _chunk(ids, 100):
        if not chunk:
            continue
        try:
            url = LAST_PRICES_URL_BATCH_TMPL.format(market_ids=",".join(chunk))
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            lep = data.get("last_executed_prices")
            if isinstance(lep, dict):
                # Shape: { market_id: [ { contract_id, last_executed_price, timestamp }, ... ] }
                for mid, arr in lep.items():
                    mid_str = str(mid)
                    if not isinstance(arr, list):
                        continue
                    m = out.get(mid_str) or {}
                    for item in arr:
                        cid = str(item.get("contract_id")) if item.get("contract_id") is not None else None
                        if not cid:
                            continue
                        m[cid] = item
                    out[mid_str] = m
            else:
                # Fallback: flat list of items with market_id/contract_id
                items = lep or data.get("prices") or []
                if isinstance(items, list):
                    for item in items:
                        mid = str(item.get("market_id")) if item.get("market_id") is not None else None
                        cid = str(item.get("contract_id")) if item.get("contract_id") is not None else None
                        if not mid or not cid:
                            continue
                        m = out.get(mid) or {}
                        m[cid] = item
                        out[mid] = m
        except Exception:
            continue
    return out


def fetch_quotes(market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch live quotes (order book summary) for up to 200 market IDs per batch.
    Returns nested mapping: market_id -> contract_id -> {
        best_offer_bps, best_offer_decimal, best_bid_bps, best_bid_decimal, raw
    }

    - Prices are basis points (e.g., 5000 = 50%). Decimal odds = 10000 / bps.
    - We compute:
      * best_offer_bps: largest offer price (highest odds to back)
      * best_bid_bps: smallest bid price (smallest odds to lay) per user request
    """
    ids = [str(i) for i in market_ids if i]
    if not ids:
        return {}
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    def _chunk(lst: List[str], size: int) -> List[List[str]]:
        return [lst[i:i+size] for i in range(0, len(lst), size)]

    def _bps_to_decimal(bps: Optional[int]) -> Optional[float]:
        try:
            if bps and bps > 0:
                return 10000.0 / float(bps)
        except Exception:
            return None
        return None

    # Return mapping keyed by contract_id
    out: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunk(ids, 200):
        if not chunk:
            continue
        try:
            url = QUOTES_URL_BATCH_TMPL.format(market_ids=",".join(chunk))
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            # Some responses use top-level dict keyed by contract_id without a 'quotes' key
            q = data.get("quotes") if isinstance(data, dict) and "quotes" in data else data
            # The quotes payload is typically a dict keyed by contract_id
            if isinstance(q, dict):
                items = list(q.items())  # (contract_id, { bids, offers })
            elif isinstance(q, list):
                # Fallback shape: list of objects containing contract_id
                items = [(str(it.get("contract_id")), it) for it in q]
            else:
                items = []
            for cid, item in items:
                cid_str = str(cid) if cid else None
                if not cid_str or not isinstance(item, dict):
                    continue
                bids = item.get("bids") or []
                offers = item.get("offers") or []
                # Extract price integers from bid/offer arrays; field name may be 'price'
                bid_prices = []
                offer_prices = []
                for b in bids:
                    p = b.get("price")
                    if isinstance(p, int):
                        bid_prices.append(p)
                    elif isinstance(p, str):
                        try:
                            bid_prices.append(int(p))
                        except Exception:
                            pass
                for o in offers:
                    p = o.get("price")
                    if isinstance(p, int):
                        offer_prices.append(p)
                    elif isinstance(p, str):
                        try:
                            offer_prices.append(int(p))
                        except Exception:
                            pass

                # Best ask (back) is the lowest offer price; best bid (lay) is the highest bid price
                best_offer_bps = min(offer_prices) if offer_prices else None
                best_bid_bps = max(bid_prices) if bid_prices else None
                entry = {
                    "best_offer_bps": best_offer_bps,
                    "best_offer_decimal": _bps_to_decimal(best_offer_bps),
                    "best_bid_bps": best_bid_bps,
                    "best_bid_decimal": _bps_to_decimal(best_bid_bps),
                    "raw": item,
                }
                out[cid_str] = entry
        except Exception:
            continue
    return out


def fetch_event_states(event_ids: List[str]) -> Dict[str, Any]:
    ids = [str(i) for i in event_ids if i]
    if not ids:
        return {"count": 0, "states": []}
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("SMARKETS_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    url = STATES_URL_BATCH_TMPL.format(event_ids=",".join(ids))
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    src_states = data.get("event_states", []) or []
    def _format_clock(period: Optional[str], match_time: Optional[str], stoppage_time: Optional[str], stoppage_time_announced: Optional[Any]) -> Dict[str, Any]:
        p = (period or "").lower()
        if p == "half_time":
            return {"clock_minute": None, "clock_text": "HT"}
        if p == "extra_time_half_time":
            return {"clock_minute": None, "clock_text": "ET HT"}
        if p == "full_time":
            return {"clock_minute": None, "clock_text": "FT"}
        if p == "penalty_shootout":
            return {"clock_minute": None, "clock_text": "PEN"}
        mt = match_time or ""
        minute: Optional[int] = None
        if ":" in mt:
            parts = mt.split(":")
            try:
                nums = [int(x) for x in parts]
                if len(nums) == 3:
                    minute = nums[0] * 60 + nums[1]
                elif len(nums) == 2:
                    minute = nums[0]
                elif len(nums) == 1:
                    minute = nums[0]
            except Exception:
                minute = None
        # Parse announced stoppage time (e.g., "00:07:00")
        def _to_bool(v: Any) -> bool:
            if isinstance(v, bool):
                return v
            if isinstance(v, str):
                return v.strip().lower() in {"true", "1", "yes"}
            return False
        announced = _to_bool(stoppage_time_announced)
        stoppage_minutes: Optional[int] = None
        stp = stoppage_time or ""
        if ":" in stp:
            try:
                parts = [int(x) for x in stp.split(":")]
                if len(parts) == 3:
                    stoppage_minutes = parts[0] * 60 + parts[1]  # ignore seconds for display
                elif len(parts) == 2:
                    stoppage_minutes = parts[0]
                elif len(parts) == 1:
                    stoppage_minutes = parts[0]
            except Exception:
                stoppage_minutes = None
        text: Optional[str] = None
        if minute is not None:
            if announced and (stoppage_minutes or 0) > 0:
                text = f"{minute}' (+{stoppage_minutes}')"
            else:
                text = f"{minute}'"
        return {"clock_minute": minute, "clock_text": text, "stoppage_minutes": stoppage_minutes}

    out: List[Dict[str, Any]] = []
    for s in src_states:
        st = (s.get("state") or "").lower()
        # Exclude cancelled/settled, include 'ended' so UI can show ENDED
        if st in {"cancelled", "settled"}:
            continue
        clock = s.get("clock") or {}
        scores = s.get("scores") or {}
        current = scores.get("current") if isinstance(scores.get("current"), list) else None
        clock_fmt = _format_clock(
            s.get("match_period"),
            clock.get("match_time"),
            clock.get("stoppage_time"),
            clock.get("stoppage_time_announced"),
        )
        out.append({
            "id": s.get("id"),
            "state": s.get("state"),
            "match_period": s.get("match_period"),
            "match_time": clock.get("match_time"),
            "stoppage_time": clock.get("stoppage_time"),
            "stoppage_time_announced": clock.get("stoppage_time_announced"),
            "stopped": clock.get("stopped"),
            "scores_current": current,
            "clock_minute": clock_fmt.get("clock_minute"),
            "clock_text": clock_fmt.get("clock_text"),
            "stoppage_minutes": clock_fmt.get("stoppage_minutes"),
            "actual_end_datetime": s.get("actual_end_datetime"),
            "raw": s,
        })
    return {"count": len(out), "states": out}
