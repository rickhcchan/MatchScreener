from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
import os
import math
import pandas as pd

from .league_map import resolve_div_from_slug, friendly_name_for_div


@dataclass
class DatasetCache:
    df: Optional[pd.DataFrame] = None
    mtime: Optional[float] = None


_DATASET: DatasetCache = DatasetCache()


def load_dataset(data_path: str) -> pd.DataFrame:
    """
    Load cached dataset if unchanged, otherwise read from Parquet. If not found, return empty DataFrame.
    """
    try:
        st = os.stat(data_path)
    except FileNotFoundError:
        return pd.DataFrame()
    if _DATASET.df is not None and _DATASET.mtime == st.st_mtime:
        return _DATASET.df
    try:
        df = pd.read_parquet(data_path)
    except Exception:
        return pd.DataFrame()
    _DATASET.df = df
    _DATASET.mtime = st.st_mtime
    return df


def _team_matches(df: pd.DataFrame, team_norm: str, div_code: Optional[str]) -> pd.DataFrame:
    sub = df[(df["home_norm"] == team_norm) | (df["away_norm"] == team_norm)].copy()
    if div_code:
        sub = sub[sub["Div"].astype(str) == div_code]
    # Deduplicate exact duplicate rows from merged datasets (same match appearing twice)
    # Include final scores so legitimate same-day rematches are preserved.
    try:
        sub = sub.drop_duplicates(
            subset=["Div", "date", "home_norm", "away_norm", "FTHG", "FTAG"],
            keep="first",
        )
    except Exception:
        pass
    return sub.sort_values("date", ascending=False)


def _last_n(sub: pd.DataFrame, n: Optional[int] = 80) -> pd.DataFrame:
    if sub.empty:
        return sub
    if n is None or (isinstance(n, int) and n <= 0):
        return sub
    return sub.head(int(n))


def _safe_num(x) -> float:
    try:
        return float(x)
    except Exception:
        return math.nan


def _pair_rows(df: pd.DataFrame, home_norm: str, away_norm: str) -> pd.DataFrame:
    """Return only rows where the two given teams faced each other (either home/away)."""
    return df[
        ((df["home_norm"] == home_norm) & (df["away_norm"] == away_norm)) |
        ((df["home_norm"] == away_norm) & (df["away_norm"] == home_norm))
    ]


def compute_team_stats(df: pd.DataFrame, team_norm: str, div_code: Optional[str], max_matches: Optional[int] = None) -> Dict[str, Any]:
    sub = _last_n(_team_matches(df, team_norm, div_code), max_matches)
    if sub.empty:
        return {
            "team_norm": team_norm,
            "n": 0,
            "league_div": div_code,
            "league_name": friendly_name_for_div(div_code),
            # New fields defaulted when empty
            "avg_goals_scored": None,
            "avg_goals_conceded": None,
            "wins_count": 0, "wins_pct": None, "wins_others_count": 0, "wins_others_pct": None,
            "draws_count": 0, "draws_pct": None, "draws_others_count": 0, "draws_others_pct": None,
            "losses_count": 0, "losses_pct": None, "losses_others_count": 0, "losses_others_pct": None,
            # 1st half 2+ goals by venue
            "home_ht_2plus_count": 0, "home_ht_2plus_pct": None,
            "away_ht_2plus_count": 0, "away_ht_2plus_pct": None,
        }
    # Per-row: goals for / against from team perspective, plus half splits where available
    is_home = sub["home_norm"] == team_norm
    gf = (sub["FTHG"].where(is_home, sub["FTAG"]).astype(float))
    ga = (sub["FTAG"].where(is_home, sub["FTHG"]).astype(float))
    hthg = sub.get("HTHG")
    htag = sub.get("HTAG")
    if hthg is not None and htag is not None:
        hthg = hthg.astype(float)
        htag = htag.astype(float)
        gf_ht = (hthg.where(is_home, htag))
        ga_ht = (htag.where(is_home, hthg))
        gf_2h = (gf - gf_ht).clip(lower=0)
        total_gf = gf.sum()
        share_gf_2h = float(gf_2h.sum() / total_gf) if total_gf > 0 else None
    else:
        gf_ht = None
        ga_ht = None
        share_gf_2h = None

    total_goals = (sub["FTHG"].astype(float) + sub["FTAG"].astype(float))

    # Outcome masks from team perspective
    wins_mask = gf > ga
    draws_mask = gf == ga
    losses_mask = gf < ga
    n = float(len(sub)) if len(sub) > 0 else 0.0
    def _pct(cnt: int) -> Optional[float]:
        return float(cnt) / n if n > 0 else None

    wins_count = int(wins_mask.sum())
    draws_count = int(draws_mask.sum())
    losses_count = int(losses_mask.sum())

    # Others definitions
    wins_others_mask = wins_mask & (gf >= 4)
    draws_others_mask = draws_mask & (gf >= 4) & (ga >= 4)
    losses_others_mask = losses_mask & (ga >= 4)

    wins_others_count = int(wins_others_mask.sum())
    draws_others_count = int(draws_others_mask.sum())
    losses_others_count = int(losses_others_mask.sum())

    # Optional debug: include concrete examples when enabled
    debug_examples = os.environ.get("INSIGHTS_DEBUG_EXAMPLES", "0") == "1"
    wins_others_examples: Optional[List[Dict[str, Any]]] = None
    draws_others_examples: Optional[List[Dict[str, Any]]] = None
    losses_others_examples: Optional[List[Dict[str, Any]]] = None
    if debug_examples:
        def _mk_examples(mask) -> List[Dict[str, Any]]:
            try:
                rows = sub[mask].copy()
                out: List[Dict[str, Any]] = []
                for _, r in rows.iterrows():
                    out.append({
                        "date": str(r.get("date")),
                        "div": str(r.get("Div")),
                        "home": str(r.get("home_norm")),
                        "away": str(r.get("away_norm")),
                        "FTHG": float(r.get("FTHG")) if pd.notna(r.get("FTHG")) else None,
                        "FTAG": float(r.get("FTAG")) if pd.notna(r.get("FTAG")) else None,
                    })
                return out
            except Exception:
                return []
        wins_others_examples = _mk_examples(wins_others_mask)
        draws_others_examples = _mk_examples(draws_others_mask)
        losses_others_examples = _mk_examples(losses_others_mask)

    return {
        "team_norm": team_norm,
        "n": int(len(sub)),
        "avg_goals_scored": float(gf.mean()),
        "avg_goals_conceded": float(ga.mean()),
        # Keep legacy keys for compatibility, though UI may ignore them
        "over_0_5_rate": float((total_goals >= 1).mean()),
        "clean_sheet_rate": float((ga == 0).mean()),
        "goals_share_second_half": share_gf_2h,
        "league_div": div_code,
        "league_name": friendly_name_for_div(div_code),
        # New outcome summary fields
        "wins_count": wins_count, "wins_pct": _pct(wins_count), "wins_others_count": wins_others_count, "wins_others_pct": _pct(wins_others_count),
        "draws_count": draws_count, "draws_pct": _pct(draws_count), "draws_others_count": draws_others_count, "draws_others_pct": _pct(draws_others_count),
        "losses_count": losses_count, "losses_pct": _pct(losses_count), "losses_others_count": losses_others_count, "losses_others_pct": _pct(losses_others_count),
        # 1st half 2+ goals scored by venue
        "home_ht_2plus_count": int(((is_home) & (gf_ht >= 2)).sum()) if gf_ht is not None else 0,
        "home_ht_2plus_pct": (float(((is_home) & (gf_ht >= 2)).sum()) / float(int(is_home.sum())) if gf_ht is not None and int(is_home.sum()) > 0 else None),
        "away_ht_2plus_count": int(((~is_home) & (gf_ht >= 2)).sum()) if gf_ht is not None else 0,
        "away_ht_2plus_pct": (float(((~is_home) & (gf_ht >= 2)).sum()) / float(int((~is_home).sum())) if gf_ht is not None and int((~is_home).sum()) > 0 else None),
        # 1st half 2+ goals conceded by venue
        "home_ht_2plus_conceded_count": int(((is_home) & (ga_ht >= 2)).sum()) if ga_ht is not None else 0,
        "home_ht_2plus_conceded_pct": (float(((is_home) & (ga_ht >= 2)).sum()) / float(int(is_home.sum())) if ga_ht is not None and int(is_home.sum()) > 0 else None),
        "away_ht_2plus_conceded_count": int(((~is_home) & (ga_ht >= 2)).sum()) if ga_ht is not None else 0,
        "away_ht_2plus_conceded_pct": (float(((~is_home) & (ga_ht >= 2)).sum()) / float(int((~is_home).sum())) if ga_ht is not None and int((~is_home).sum()) > 0 else None),
        # Conditional: HT 2+ scored → Win Others, HT 2+ conceded → Lost Others
        "ht_2plus_to_win_others_pct": (float(((gf_ht >= 2) & wins_others_mask).sum()) / float(int((gf_ht >= 2).sum())) if gf_ht is not None and int((gf_ht >= 2).sum()) > 0 else None),
        "ht_2plus_conceded_to_lost_others_pct": (float(((ga_ht >= 2) & losses_others_mask).sum()) / float(int((ga_ht >= 2).sum())) if ga_ht is not None and int((ga_ht >= 2).sum()) > 0 else None),
        # Conditional by venue: HT 2+ conceded → Lost Others
        "home_ht_2plus_conceded_to_lost_others_pct": (float(((is_home) & (ga_ht >= 2) & losses_others_mask).sum()) / float(int(((is_home) & (ga_ht >= 2)).sum())) if ga_ht is not None and int(((is_home) & (ga_ht >= 2)).sum()) > 0 else None),
        "away_ht_2plus_conceded_to_lost_others_pct": (float(((~is_home) & (ga_ht >= 2) & losses_others_mask).sum()) / float(int(((~is_home) & (ga_ht >= 2)).sum())) if ga_ht is not None and int(((~is_home) & (ga_ht >= 2)).sum()) > 0 else None),
        # Optional debug examples
        **({
            "wins_others_examples": wins_others_examples,
            "draws_others_examples": draws_others_examples,
            "losses_others_examples": losses_others_examples,
        } if debug_examples else {}),
    }


def compute_h2h(df: pd.DataFrame, home_norm: str, away_norm: str, max_matches: Optional[int] = None) -> Dict[str, Any]:
    sub = _pair_rows(df, home_norm, away_norm)
    sub = sub.sort_values("date", ascending=False)
    if isinstance(max_matches, int) and max_matches > 0:
        sub = sub.head(max_matches)
    if sub.empty:
        return {"n": 0}
    total_goals = (sub["FTHG"].astype(float) + sub["FTAG"].astype(float))
    zero_zero = (total_goals == 0).sum()
    return {
        "n": int(len(sub)),
        "zero_zero_rate": float(zero_zero / len(sub)),
        "over_0_5_rate": float(1.0 - (zero_zero / len(sub))),
        "avg_total_goals": float(total_goals.mean()),
    }


def compute_h2h_matches(
    df: pd.DataFrame,
    home_norm: str,
    away_norm: str,
    max_matches: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return raw H2H fixtures list in descending date order.
    Each item includes date, home_norm, away_norm, FTHG, FTAG.
    """
    sub = _pair_rows(df, home_norm, away_norm)
    sub = sub.sort_values("date", ascending=False)
    if isinstance(max_matches, int) and max_matches > 0:
        sub = sub.head(max_matches)
    out: List[Dict[str, Any]] = []
    for _, r in sub.iterrows():
        try:
            out.append({
                "date": str(r.get("date")),
                "home_norm": str(r.get("home_norm")),
                "away_norm": str(r.get("away_norm")),
                "FTHG": float(r.get("FTHG")) if pd.notna(r.get("FTHG")) else None,
                "FTAG": float(r.get("FTAG")) if pd.notna(r.get("FTAG")) else None,
            })
        except Exception:
            continue
    return out


def compute_h2h_for_div(
    df: pd.DataFrame,
    home_norm: str,
    away_norm: str,
    div_code: Optional[str],
    max_matches: Optional[int] = None,
) -> Dict[str, Any]:
    if not div_code:
        return {"n": 0}
    sub = _pair_rows(df, home_norm, away_norm)
    sub = sub[sub["Div"].astype(str) == str(div_code)]
    sub = sub.sort_values("date", ascending=False)
    if isinstance(max_matches, int) and max_matches > 0:
        sub = sub.head(max_matches)
    if sub.empty:
        return {"n": 0}
    total_goals = (sub["FTHG"].astype(float) + sub["FTAG"].astype(float))
    zero_zero = (total_goals == 0).sum()
    return {
        "n": int(len(sub)),
        "avg_total_goals": float(total_goals.mean()),
        "over_0_5_rate": float(1.0 - (zero_zero / len(sub))),
    }


def compute_league_overview(
    df: pd.DataFrame,
    div_code: Optional[str],
    max_matches: Optional[int] = None,
) -> Dict[str, Any]:
    """Aggregate league-wide stats for the provided Div across the dataset.
    Returns number of matches, average total goals, and total over 0.5 rate.
    Uses all rows in the merged dataset (unless max_matches is provided)."""
    if not div_code:
        return {"n": 0}
    sub = df[df["Div"].astype(str) == str(div_code)].copy()
    sub = sub.sort_values("date", ascending=False)
    if isinstance(max_matches, int) and max_matches > 0:
        sub = sub.head(max_matches)
    if sub.empty:
        return {"n": 0}
    FTHG = sub["FTHG"].astype(float)
    FTAG = sub["FTAG"].astype(float)
    total_goals = (FTHG + FTAG)
    zero_zero = (total_goals == 0).sum()

    # Half-time goals
    hthg = sub.get("HTHG")
    htag = sub.get("HTAG")
    if hthg is not None and htag is not None:
        HTHG = hthg.astype(float)
        HTAG = htag.astype(float)
        home_ht_2plus_pct = float((HTHG >= 2).mean()) if len(sub) > 0 else None
        away_ht_2plus_pct = float((HTAG >= 2).mean()) if len(sub) > 0 else None
    else:
        home_ht_2plus_pct = None
        away_ht_2plus_pct = None

    # Venue-scoped rates
    home_scored_2plus_pct = float((FTHG >= 2).mean()) if len(sub) > 0 else None
    away_scored_2plus_pct = float((FTAG >= 2).mean()) if len(sub) > 0 else None

    # Outcome masks (league-wide)
    home_win_mask = FTHG > FTAG
    draw_mask = FTHG == FTAG
    away_win_mask = FTHG < FTAG
    n = int(len(sub))
    def _pct(cnt: int) -> Optional[float]:
        return float(cnt) / float(n) if n > 0 else None

    home_win_count = int(home_win_mask.sum())
    draw_count = int(draw_mask.sum())
    away_win_count = int(away_win_mask.sum())

    # "Others" definitions at league level
    home_win_others_count = int((home_win_mask & (FTHG >= 4)).sum())
    draw_others_count = int((draw_mask & (FTHG >= 4) & (FTAG >= 4)).sum())
    away_win_others_count = int((away_win_mask & (FTAG >= 4)).sum())

    return {
        "n": n,
        # Keep legacy keys for compatibility
        "avg_total_goals": float(total_goals.mean()),
        "over_0_5_rate": float(1.0 - (zero_zero / len(sub))) if len(sub) > 0 else None,
        # New league metrics
        "avg_goals_home": float(FTHG.mean()),
        "avg_goals_away": float(FTAG.mean()),
        "home_scored_2plus_pct": home_scored_2plus_pct,
        "away_scored_2plus_pct": away_scored_2plus_pct,
        "home_ht_2plus_pct": home_ht_2plus_pct,
        "away_ht_2plus_pct": away_ht_2plus_pct,
        "home_win_count": home_win_count, "home_win_pct": _pct(home_win_count),
        "home_win_others_count": home_win_others_count, "home_win_others_pct": _pct(home_win_others_count),
        "draw_count": draw_count, "draw_pct": _pct(draw_count),
        "draw_others_count": draw_others_count, "draw_others_pct": _pct(draw_others_count),
        "away_win_count": away_win_count, "away_win_pct": _pct(away_win_count),
        "away_win_others_count": away_win_others_count, "away_win_others_pct": _pct(away_win_others_count),
    }


# Legacy: h2h_by_league removed per new scope (h2h ignores league).


# odds-highlevel computation removed per request


# odds-highlevel computation removed per request


def build_match_insights(
    df: pd.DataFrame,
    home_norm: str,
    away_norm: str,
    full_slug: Optional[str],
    max_matches: Optional[int] = None,
    h2h_max: Optional[int] = None,
) -> Dict[str, Any]:
    # Resolve event league
    event_div = resolve_div_from_slug(full_slug)
    # Gather rows for teams (any league)
    home_rows_any = _team_matches(df, home_norm, None)
    away_rows_any = _team_matches(df, away_norm, None)
    found_home = not home_rows_any.empty
    found_away = not away_rows_any.empty

    # Determine league scopes
    league_scope: List[Dict[str, Any]] = []
    if event_div:
        # Use event league for both teams
        home_stats = compute_team_stats(df, home_norm, event_div, max_matches)
        away_stats = compute_team_stats(df, away_norm, event_div, max_matches)
        league_scope.append({"type": "event", "div": str(event_div), "name": friendly_name_for_div(event_div)})
        status = "event-league-scope"
    else:
        # No event league resolved: pick latest known league per team
        def latest_div(rows: pd.DataFrame) -> Optional[str]:
            if rows is None or rows.empty:
                return None
            r = rows.dropna(subset=["Div"]).sort_values("date", ascending=False).head(1)
            if r.empty:
                return None
            try:
                return str(r.iloc[0]["Div"])
            except Exception:
                return None

        home_div = latest_div(home_rows_any)
        away_div = latest_div(away_rows_any)
        home_stats = compute_team_stats(df, home_norm, home_div, max_matches)
        away_stats = compute_team_stats(df, away_norm, away_div, max_matches)
        if home_div:
            league_scope.append({"type": "home", "div": str(home_div), "name": friendly_name_for_div(home_div)})
        if away_div and away_div != home_div:
            league_scope.append({"type": "away", "div": str(away_div), "name": friendly_name_for_div(away_div)})
        if not home_div and not away_div:
            status = "no-league-scope"
        elif home_div and away_div and home_div == away_div:
            status = "same-latest-league"
        else:
            status = "per-team-latest-league"

    # H2H ignores league; use all available in dataset (2 seasons merged)
    h2h = compute_h2h(df, home_norm, away_norm, max_matches=h2h_max)
    h2h_matches = compute_h2h_matches(df, home_norm, away_norm, max_matches=h2h_max)

    # Enrich league_scope blocks with league-wide metrics (all matches in that league)
    if league_scope:
        enriched = []
        for blk in league_scope:
            stats = compute_league_overview(df, blk.get("div"), max_matches=None)
            enriched.append({**blk, **stats})
        league_scope = enriched

    return {
        "league_div": event_div,
        "status": status,
        "home": home_stats,
        "away": away_stats,
        "h2h": h2h,
        "h2h_matches": h2h_matches,
        "league_scope": league_scope,
    }
