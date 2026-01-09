from __future__ import annotations
from typing import List, Tuple, Optional
import io
import os
from datetime import datetime
import pandas as pd
import requests
from .team_map import apply_team_alias

BASE_SEASON_URL = "https://www.football-data.co.uk/mmz4281/{season_code}/all-euro-data-{start_year}-{end_year}.xlsx"
LATEST_RESULTS_URL = "https://www.football-data.co.uk/mmz4281/{season_code}/Latest_Results.xlsx"
DOWNLOAD_PAGE = "https://www.football-data.co.uk/downloadm.php"

# World leagues URLs (new structure)
WORLD_SEASON_URL = "https://www.football-data.co.uk/new/new_leagues_data.xlsx"
WORLD_LATEST_URL = "https://www.football-data.co.uk/new/Latest_Results.xlsx"

def normalize_team_name(name: str) -> str:
    s = str(name).lower().strip()
    # Normalize special characters (Norwegian, Spanish, French, German, etc.)
    for k, v in {"ø": "o", "å": "a", "æ": "ae", "ö": "o", "ä": "a", "ü": "u", "é": "e", "è": "e", "ê": "e", "ë": "e", "á": "a", "à": "a", "â": "a", "ã": "a", "í": "i", "ì": "i", "î": "i", "ï": "i", "ó": "o", "ò": "o", "ô": "o", "õ": "o", "ú": "u", "ù": "u", "û": "u", "ç": "c", "ñ": "n"}.items():
        s = s.replace(k, v)
    for k, v in {"&": "and", "-": " ", "/": " ", "'": "", ".": ""}.items():
        s = s.replace(k, v)
    words = s.split()
    filtered = [w for w in words if w not in {"fc", "cf", "sc", "afc", "c.f."}]
    norm = " ".join(filtered)
    # Apply targeted aliases from centralized mapping
    return apply_team_alias(norm)

def season_code_from_date(today: Optional[datetime] = None) -> Tuple[str, int, int]:
    t = today or datetime.utcnow()
    year = t.year
    start_year = year
    end_year = year + 1
    code = f"{str(start_year)[-2:]}{str(end_year)[-2:]}"
    return code, start_year, end_year

def previous_season_code(today: Optional[datetime] = None) -> Tuple[str, int, int]:
    t = today or datetime.utcnow()
    year = t.year
    start_year = year - 1
    end_year = year
    code = f"{str(start_year)[-2:]}{str(end_year)[-2:]}"
    return code, start_year, end_year

def try_fetch_excel_all_sheets(url: str) -> Optional[pd.DataFrame]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    resp = requests.get(url, timeout=30, headers=headers, allow_redirects=False)
    
    # If it's a redirect, follow it once to get the actual file
    if resp.status_code in (301, 302, 303, 307, 308):
        # Don't follow Office viewer redirects, get the file directly
        if 'officeapps.live.com' in resp.headers.get('Location', ''):
            # Re-request with headers that force download
            resp = requests.get(url, timeout=30, headers=headers)
        else:
            resp = requests.get(resp.headers.get('Location'), timeout=30, headers=headers)
    
    if resp.status_code != 200:
        return None
    
    # Check if we got HTML instead of Excel
    content_type = resp.headers.get('Content-Type', '').lower()
    if 'html' in content_type:
        return None
        
    content = io.BytesIO(resp.content)
    try:
        sheets = pd.read_excel(content, sheet_name=None)
        frames: List[pd.DataFrame] = []
        for name, df in sheets.items():
            # Do not derive Div from sheet/tab name; use the sheet's own first-column Div.
            frames.append(df.copy())
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)
    except Exception:
        return None

def discover_latest_season_from_page(session: Optional[requests.Session] = None) -> Optional[Tuple[str, int, int]]:
    s = session or requests.Session()
    resp = s.get(DOWNLOAD_PAGE, timeout=30)
    if resp.status_code != 200:
        return None
    text = resp.text.lower()
    import re
    matches = re.findall(r"all-euro-data-(\d{4})-(\d{4})\.xlsx", text)
    if not matches:
        return None
    pairs = [(int(a), int(b)) for a, b in matches]
    pairs.sort(key=lambda x: x[1], reverse=True)
    start_year, end_year = pairs[0]
    code = f"{str(start_year)[-2:]}{str(end_year)[-2:]}"
    return code, start_year, end_year

def load_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    def find(name_opts: List[str]) -> Optional[str]:
        for n in name_opts:
            if n in cols:
                return cols[n]
        return None
    date_col = find(["date", "match date", "dateutc"]) or "Date"
    time_col = find(["time", "ko time"]) or "Time"
    home_col = find(["hometeam", "home team", "home"]) or "HomeTeam"
    away_col = find(["awayteam", "away team", "away"]) or "AwayTeam"
    div_col = find(["div", "league", "competition"]) or "Div"
    fthg_col = find(["fthg", "hg", "home goals"]) or "FTHG"
    ftag_col = find(["ftag", "ag", "away goals"]) or "FTAG"
    hthg_col = find(["hthg"]) or "HTHG"
    htag_col = find(["htag"]) or "HTAG"

    out = pd.DataFrame()
    # Original columns requested
    out["Div"] = df.get(div_col)
    out["Date"] = df.get(date_col)
    out["Time"] = df.get(time_col)
    out["HomeTeam"] = df.get(home_col)
    out["AwayTeam"] = df.get(away_col)
    out["FTHG"] = df.get(fthg_col)
    out["FTAG"] = df.get(ftag_col)
    out["HTHG"] = df.get(hthg_col) if hthg_col in cols else df.get("HTHG")
    out["HTAG"] = df.get(htag_col) if htag_col in cols else df.get("HTAG")

    # Odds
    for col in ["BFH","BFD","BFA","BFCH","BFCD","BFCA"]:
        out[col] = df.get(col)

    # Stats
    for col in [
        "HS","AS","HST","AST","HHW","AHW","HC","AC","HF","AF",
        "HFKC","AFKC","HO","AO","HY","AY","HR","AR","HBP","ABP",
    ]:
        out[col] = df.get(col)

    # Normalized helpers
    out["date"] = pd.to_datetime(out["Date"], dayfirst=True, errors="coerce")
    out["home"] = out["HomeTeam"].astype(str)
    out["away"] = out["AwayTeam"].astype(str)
    out["league"] = out["Div"].astype(str)
    out["home_goals"] = pd.to_numeric(out["FTHG"], errors="coerce")
    out["away_goals"] = pd.to_numeric(out["FTAG"], errors="coerce")
    out["home_norm"] = out["home"].apply(normalize_team_name)
    out["away_norm"] = out["away"].apply(normalize_team_name)
    out["league_norm"] = out["league"].str.lower().str.strip()
    out = out.dropna(subset=["date", "HomeTeam", "AwayTeam"])  # ensure core fields
    # Mark Europe leagues as having half-time data
    out["has_half_time_data"] = True
    return out

def dedupe_merge(base: pd.DataFrame, overlay: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["date", "home_norm", "away_norm", "league_norm"]
    base["_src"] = "base"
    overlay["_src"] = "overlay"
    combined = pd.concat([base, overlay], ignore_index=True)
    combined.sort_values(by=["date", "_src"], ascending=[True, True], inplace=True)
    deduped = combined.drop_duplicates(subset=key_cols, keep="last").drop(columns=["_src"])  # keep overlay
    return deduped

def fetch_season_dataset(today: Optional[datetime] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    code, sy, ey = season_code_from_date(today)
    url_current = BASE_SEASON_URL.format(season_code=code, start_year=sy, end_year=ey)
    df_current = try_fetch_excel_all_sheets(url_current)
    if df_current is None:
        code_prev, sy_prev, ey_prev = previous_season_code(today)
        url_current = BASE_SEASON_URL.format(season_code=code_prev, start_year=sy_prev, end_year=ey_prev)
        df_current = try_fetch_excel_all_sheets(url_current)
        code = code_prev
        sy, ey = sy_prev, ey_prev
    code_prev2, sy_prev2, ey_prev2 = f"{str(sy-1)[-2:]}{str(ey-1)[-2:]}", sy-1, ey-1
    url_previous = BASE_SEASON_URL.format(season_code=code_prev2, start_year=sy_prev2, end_year=ey_prev2)
    df_previous = try_fetch_excel_all_sheets(url_previous)
    if df_previous is None:
        df_previous = pd.DataFrame()

    url_latest = LATEST_RESULTS_URL.format(season_code=code)
    df_latest = try_fetch_excel_all_sheets(url_latest)
    if df_latest is None:
        df_latest = pd.DataFrame()

    if df_current is None:
        df_current = pd.DataFrame()
    return df_current, df_previous, df_latest

def load_and_normalize_world(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize world leagues data (Country_League format, no half-time data)"""
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    cols = {c.lower(): c for c in df.columns}
    def find(name_opts: List[str]) -> Optional[str]:
        for n in name_opts:
            if n in cols:
                return cols[n]
        return None
    
    # Clean Country and League (strip trailing spaces)
    if "Country" in df.columns:
        df["Country"] = df["Country"].astype(str).str.strip()
    if "League" in df.columns:
        df["League"] = df["League"].astype(str).str.strip()
    
    date_col = find(["date", "match date"]) or "Date"
    time_col = find(["time", "ko time"]) or "Time"
    home_col = find(["home"]) or "Home"
    away_col = find(["away"]) or "Away"
    hg_col = find(["hg", "home goals"]) or "HG"
    ag_col = find(["ag", "away goals"]) or "AG"
    country_col = find(["country"]) or "Country"
    league_col = find(["league"]) or "League"
    
    out = pd.DataFrame()
    # Create composite Div as "Country_League"
    country = df.get(country_col, pd.Series([""] * len(df))).astype(str).str.strip()
    league = df.get(league_col, pd.Series([""] * len(df))).astype(str).str.strip()
    out["Div"] = country + "_" + league
    
    out["Date"] = df.get(date_col)
    out["Time"] = df.get(time_col)
    out["HomeTeam"] = df.get(home_col)
    out["AwayTeam"] = df.get(away_col)
    out["FTHG"] = df.get(hg_col)
    out["FTAG"] = df.get(ag_col)
    out["HTHG"] = None  # No half-time data
    out["HTAG"] = None
    
    # Odds (same columns as Europe)
    for col in ["BFH","BFD","BFA","BFCH","BFCD","BFCA"]:
        out[col] = None  # World leagues don't have these specific odds
    
    # Stats (world leagues have different odd columns, set to None)
    for col in [
        "HS","AS","HST","AST","HHW","AHW","HC","AC","HF","AF",
        "HFKC","AFKC","HO","AO","HY","AY","HR","AR","HBP","ABP",
    ]:
        out[col] = None
    
    # Normalized helpers
    out["date"] = pd.to_datetime(out["Date"], dayfirst=True, errors="coerce")
    out["home"] = out["HomeTeam"].astype(str)
    out["away"] = out["AwayTeam"].astype(str)
    out["league"] = out["Div"].astype(str)
    out["home_goals"] = pd.to_numeric(out["FTHG"], errors="coerce")
    out["away_goals"] = pd.to_numeric(out["FTAG"], errors="coerce")
    out["home_norm"] = out["home"].apply(normalize_team_name)
    out["away_norm"] = out["away"].apply(normalize_team_name)
    # Normalize Div to match Smarkets format: country-league
    out["league_norm"] = out["Div"].str.lower().str.replace("_", "-").str.replace(" ", "-")
    out = out.dropna(subset=["date", "HomeTeam", "AwayTeam"])
    # Mark world leagues as NOT having half-time data
    out["has_half_time_data"] = False
    return out

def fetch_world_season_dataset() -> pd.DataFrame:
    """Fetch world leagues data, taking latest 2 seasons per league"""
    df_season = try_fetch_excel_all_sheets(WORLD_SEASON_URL)
    if df_season is None or df_season.empty:
        return pd.DataFrame()
    
    df_latest = try_fetch_excel_all_sheets(WORLD_LATEST_URL)
    
    # Process season data: group by Country+League and take latest 2 seasons
    df_season["Country"] = df_season["Country"].astype(str).str.strip()
    df_season["League"] = df_season["League"].astype(str).str.strip()
    df_season["Season"] = df_season["Season"].astype(str).str.strip()
    
    # Group by league and get latest 2 seasons - use list comprehension instead of apply
    filtered_dfs = []
    for (country, league), group in df_season.groupby(["Country", "League"]):
        unique_seasons = sorted(group["Season"].unique(), reverse=True)
        latest_2 = unique_seasons[:2]
        filtered_group = group[group["Season"].isin(latest_2)]
        filtered_dfs.append(filtered_group)
    
    df_filtered = pd.concat(filtered_dfs, ignore_index=True) if filtered_dfs else pd.DataFrame()
    
    # Normalize both
    season_normalized = load_and_normalize_world(df_filtered) if not df_filtered.empty else pd.DataFrame()
    latest_normalized = load_and_normalize_world(df_latest) if df_latest is not None and not df_latest.empty else pd.DataFrame()
    
    # Merge with dedup (latest takes precedence)
    if not season_normalized.empty and not latest_normalized.empty:
        return dedupe_merge(season_normalized, latest_normalized)
    elif not season_normalized.empty:
        return season_normalized
    else:
        return latest_normalized

def build_merged_dataset(today: Optional[datetime] = None) -> pd.DataFrame:
    """Build merged dataset from both Europe and World leagues"""
    # Fetch Europe leagues
    cur, prev, latest = fetch_season_dataset(today)
    cur_n = load_and_normalize(cur) if not cur.empty else pd.DataFrame()
    prev_n = load_and_normalize(prev) if not prev.empty else pd.DataFrame()
    latest_n = load_and_normalize(latest) if not latest.empty else pd.DataFrame()
    merged_cur = dedupe_merge(cur_n, latest_n) if not cur_n.empty else latest_n
    europe_merged = pd.concat([merged_cur, prev_n], ignore_index=True)
    
    # Fetch World leagues
    world_merged = fetch_world_season_dataset()
    
    # Combine both
    all_merged = pd.concat([europe_merged, world_merged], ignore_index=True)
    
    if not all_merged.empty and "date" in all_merged.columns:
        all_merged.sort_values(by=["date"], inplace=True)
    return all_merged

if __name__ == "__main__":
    print("Fetching and building dataset...")
    df = build_merged_dataset()
    out_path = os.environ.get("DATA_PATH", "data/matches_v1.parquet")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"Done. Saved {len(df)} rows to {out_path}")
