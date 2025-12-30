from __future__ import annotations
from typing import Dict, Any
from datetime import datetime
import pandas as pd

from .fetch_football_data import normalize_team_name

def _team_filter(df: pd.DataFrame, team_norm: str) -> pd.DataFrame:
    return df[(df["home_norm"] == team_norm) | (df["away_norm"] == team_norm)]

def _recent(df: pd.DataFrame, last_n: int) -> pd.DataFrame:
    df_sorted = df.sort_values(by=["date"], ascending=False)
    return df_sorted.head(last_n)

def compute_team_facts(df: pd.DataFrame, team_name: str, last_n: int = 20) -> Dict[str, Any]:
    tnorm = normalize_team_name(team_name)
    now = datetime.utcnow()
    hist = df[df["date"] < pd.Timestamp(now)]
    subset = _team_filter(hist, tnorm)
    recent = _recent(subset, last_n)
    if recent.empty:
        return {"team": team_name, "matches": 0}

    home_mask = recent["home_norm"] == tnorm
    away_mask = recent["away_norm"] == tnorm
    goals_scored = recent.loc[home_mask, "home_goals"].fillna(0).sum() + recent.loc[away_mask, "away_goals"].fillna(0).sum()
    goals_conceded = recent.loc[home_mask, "away_goals"].fillna(0).sum() + recent.loc[away_mask, "home_goals"].fillna(0).sum()

    total_goals = (recent["home_goals"].fillna(0) + recent["away_goals"].fillna(0))
    over25 = (total_goals > 2.5).sum()
    btts = ((recent["home_goals"].fillna(0) > 0) & (recent["away_goals"].fillna(0) > 0)).sum()

    # Form points: W=3, D=1, L=0 from the team perspective
    home_w = (recent["home_goals"] > recent["away_goals"]).astype(int)
    away_w = (recent["away_goals"] > recent["home_goals"]).astype(int)
    draw = (recent["home_goals"] == recent["away_goals"]).astype(int)
    points = ((home_mask.astype(int) * home_w * 3) + (away_mask.astype(int) * away_w * 3) + ((home_mask | away_mask).astype(int) * draw))

    return {
        "team": team_name,
        "matches": int(len(recent)),
        "goals_scored": float(goals_scored),
        "goals_conceded": float(goals_conceded),
        "avg_goals_scored": float(goals_scored) / max(len(recent), 1),
        "avg_goals_conceded": float(goals_conceded) / max(len(recent), 1),
        "btts_rate": float(btts) / max(len(recent), 1),
        "over25_rate": float(over25) / max(len(recent), 1),
        "form_points_last_n": int(points.sum()),
    }

def compute_match_facts(df: pd.DataFrame, home_name: str, away_name: str, last_n: int = 20) -> Dict[str, Any]:
    return {
        "last_n": last_n,
        "home": compute_team_facts(df, home_name, last_n=last_n),
        "away": compute_team_facts(df, away_name, last_n=last_n),
    }
