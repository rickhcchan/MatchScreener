from __future__ import annotations
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

class Team(BaseModel):
    name: str
    country: Optional[str] = None

class League(BaseModel):
    name: str
    country: Optional[str] = None
    tier: Optional[int] = None

class Fixture(BaseModel):
    id: Optional[str] = None
    league: League
    home: Team
    away: Team
    start_time: datetime
    odds_over_0_5: Optional[float] = None
    odds_under_0_5: Optional[float] = None
    market_source: Optional[str] = None  # e.g., "smarkets"

class HistoricalMatch(BaseModel):
    league: Optional[League] = None
    home: Team
    away: Team
    start_time: datetime
    goals_home: int
    goals_away: int

    @property
    def total_goals(self) -> int:
        return self.goals_home + self.goals_away

class MatchupStats(BaseModel):
    key: str  # composed key for matchup/league/team perspective
    samples: int
    avg_goals: float
    pct_over_0_5: float
    last_matches: List[HistoricalMatch] = []
