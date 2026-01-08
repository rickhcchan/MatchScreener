from __future__ import annotations

# Mapping from Smarkets-style team names (after base normalization)
# to canonical names used in the historical football-data dataset.
# Left side: normalized Smarkets name
# Right side: normalized historical dataset name
TEAM_ALIASES: dict[str, str] = {
    # Example: Smarkets often uses full club names where football-data shortens
    # Add precise mappings here to avoid over-general replacements.
    "lincoln city": "lincoln",
    "burton albion": "burton",
    "peterborough": "peterboro",
    "sheff wed": "sheffield weds",
    "sheff utd": "sheffield united",
    "bristol rovers": "bristol rvs",
    "salford city": "salford",
    "man utd": "man united",
    "wolverhampton": "wolves",
    "usl dunkerque": "dunkerque",
    "aris thessaloniki": "aris",
    "1 magdeburg": "magdeburg",
    "nfc volos": "volos nfc",
    "red bull salzburg": "salzburg",
    "atromitos athens": "atromitos",
    "psg": "paris",
    "ac milan": "milan",
    "inter milan": "inter",
}


def apply_team_alias(norm_name: str) -> str:
    """Return canonical team name using alias mapping when present.
    Input should already be lowercase and punctuation-stripped.
    """
    try:
        key = (norm_name or "").strip().lower()
        return TEAM_ALIASES.get(key, key)
    except Exception:
        return norm_name
