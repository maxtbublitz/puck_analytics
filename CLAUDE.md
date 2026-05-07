# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This project fetches NHL data from two public APIs and loads it into a local PostgreSQL database. The main entry point for all data updates is `scripts/update_data.py`.

## Environment

Copy `.env` (already present, git-ignored) with these variables:
- `NHL_API_URL` — `https://api-web.nhle.com` (newer v1 endpoints)
- `NHL_API_URL_2` — `https://api.nhle.com` (stats/rest endpoints)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` — PostgreSQL connection

Dependencies (install manually, no requirements.txt): `psycopg2`, `python-dotenv`, `requests`

## Running updates

Run from the repo root so relative imports resolve correctly.

```bash
# Full update (all entities in dependency order)
python -m scripts.update_data

# Single entity update
python -m scripts.update_data seasons
python -m scripts.update_data teams
python -m scripts.update_data team_seasons
python -m scripts.update_data players
python -m scripts.update_data rosters
python -m scripts.update_data standings
python -m scripts.update_data player_stats
python -m scripts.update_data playoff_data
```

## Architecture

### Two API sources
- `NHL_API_URL_2` (`api.nhle.com`) — used for seasons and teams (stats/rest endpoints returning `{"data": [...]}`)
- `NHL_API_URL` (`api-web.nhle.com`) — used for rosters, players, standings, stats, playoffs (v1 endpoints)

The full update sequence in `run_update_sequence()` uses `base_url` for seasons/teams and `base_url_2` for everything else.

### Module layout
- **`database/crud.py`** — all `get_*_from_api` and `insert_*_into_db` functions; the core business logic
- **`database/db_helpers.py`** — cursor-level helpers (`get_or_create_conference`, `get_or_create_division`, `get_team_id`, `get_team_season_id_from_team_name`)
- **`database/db_utils.py`** — `get_db_connection()` using env vars
- **`database/http_utils.py`** — `get_with_retry()` with exponential backoff and 429/5xx handling
- **`scripts/update_data.py`** — orchestration layer; wraps each crud pair in an `update_*` function and dispatches via `update_map`
- **`fetch/`** — standalone one-off scripts predating the modular design; mostly superseded by `crud.py`

### Data model dependency order
`seasons` → `teams` → `team_seasons` → `players` + `rosters` → `standings` → `player_stats` → `playoff_data`

`team_seasons` is the junction table between `teams` and `seasons`; its `id` column (`team_season_id`) is the FK used by `rosters`, `player_stats`, `player_stats_playoffs`, and `playoff_series`.

### Key patterns
- All inserts use `ON CONFLICT ... DO UPDATE` (upsert) so re-runs are safe
- `get_players_from_api(conn, base_url, include_roster_info=True)` serves double duty: called once for players, once for rosters, avoiding a second API round-trip
- Player stats split across two tables: `player_stats` (regular season, `game_type_id=2`) and `player_stats_playoffs` (playoffs, other values); goalies are excluded
- Seasons are filtered to `id >= 20052006` (2005–06 season onward)
- Utah (`UTA`) is skipped for seasons before 2024–25 because the API has no data for it
