from __future__ import annotations
from typing import Optional

# Minimal league slug → football-data Div code mapping.
# Please expand/adjust as needed.
SLUG_TO_DIV = {
    # England
    "england-premier-league": "E0",
    "england-championship": "E1",
    "england-league-1": "E2",
    "england-league-2": "E3",
    "england-national-league": "E4",

    # Scotland
    "scotland-premiership": "SC0",
    "scotland-championship": "SC1",
    "scotland-league-one": "SC2",
    "scotland-league-two": "SC3",

    # Germany
    "germany-bundesliga": "D1",
    "germany-2-bundesliga": "D2",

    # Italy
    "italy-serie-a": "I1",
    # Note: Serie B may be rare in Smarkets, but include for completeness
    "italy-serie-b": "I2",

    # Spain
    "spain-la-liga": "SP1",
    "spain-la-liga-2": "SP2",

    # France
    "france-ligue-1": "F1",
    "france-ligue-2": "F2",

    # Netherlands
    "netherlands-eredivisie": "N1",

    # Belgium
    "belgium-first-division-a": "B1",

    # Portugal
    "portugal-primeira-liga": "P1",

    # Turkey
    "turkey-super-lig": "T1",

    # Greece
    "greece-super-league": "G1",
}

def resolve_div_from_slug(full_slug: Optional[str]) -> Optional[str]:
    if not full_slug:
        return None
    s = full_slug.lower()
    # Expect pattern like /sport/football/leagues/italy-serie-a/...
    import re
    m = re.search(r"/leagues/([^/]+)/", s)
    if not m:
        return None
    slug = m.group(1)
    return SLUG_TO_DIV.get(slug)

def friendly_name_for_div(div_code: str | None) -> str | None:
    if not div_code:
        return None
    try:
        # Find a slug that maps to this Div, then prettify
        for slug, div in SLUG_TO_DIV.items():
            if str(div) == str(div_code):
                name = slug.replace("-", " ").title()
                return name
    except Exception:
        pass
    return str(div_code) if div_code is not None else None
