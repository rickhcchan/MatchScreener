# MatchScreener — Upcoming Matches SPA

This project provides a minimal FastAPI app that serves a simple SPA at `/` showing upcoming football matches fetched from Smarkets. It also exposes `/api/events` returning the same data as JSON.

## Run locally (optional)
Make sure you have Python 3.11+.

```powershell
py -m venv .venv
. .venv\Scripts\Activate.ps1
py -m pip install -U pip
py -m pip install -r requirements.txt
py -m uvicorn app:APP --reload --port 8080
```

Optional: refresh historical data (if you use the ingestion route). Requires `REFRESH_TOKEN` if set:

```powershell
curl -X POST http://localhost:8080/refresh -H "Authorization: Bearer YOUR_REFRESH_TOKEN"
```

Open the SPA (defaults to today, UTC):

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/?"
```

Optional: specify a day (YYYY-MM-DD) for the API (SPA always shows today):

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/api/events?day=2025-12-28" | ConvertTo-Json -Depth 3
```

## API
- `/`: SPA that fetches `/api/events` and renders client-side.
- `/api/events`: JSON list of upcoming matches enriched with competitor info and `event_url`.
- `/refresh`: optional ingestion route to update historical Parquet (protected via bearer token if `REFRESH_TOKEN` is set).

## Environment variables
- `DATA_PATH`: where to store Parquet (default `/data/matches.parquet`).
- `REFRESH_TOKEN`: token required by `/refresh` (optional but recommended if you expose it).
- `SMARKETS_API_KEY`: Smarkets API key (optional; if provided it is used as bearer auth).

## Notes
- The SPA is intentionally simple and focuses on upcoming matches only.
