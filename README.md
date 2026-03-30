# NEW (Neverending War)

OSINT-driven anomaly detection framework for military force posture indicators.

Correlates consumer behavior, real estate activity, and social signals near US military installations to detect anomalous patterns consistent with mobilization or deployment activity.

## Architecture

- **Collectors**: Independent Python modules that scrape/query specific data sources on a cron cadence
- **Store**: DuckDB analytical database with normalized temporal/geographic indexing
- **Analyzer**: Baseline calculation and anomaly detection per installation
- **API**: FastAPI endpoints for querying indicators and anomalies
- **Config**: Installation definitions, collector schedules, alert thresholds

## Data Sources (MVP)

| Source | Signal | Collection Method |
|--------|--------|-------------------|
| U-Haul | Truck rental pricing/availability | Web scraping |
| Zillow | Listing volume, days-on-market, price cuts | API (free tier) |
| Google Places | Foot traffic anomalies (Popular Times) | API |
| Reddit | Keyword monitoring in military subs | API (PRAW) |
| Local News | RSS feed monitoring | Feed parsing |

## Installation Tiers

- **Tier 1 (Power Projection)**: Ft. Liberty (Bragg), Ft. Campbell, Ft. Hood, Ft. Stewart, Camp Lejeune, Ft. Drum, JBLM
- **Tier 2 (Significant Force)**: Ft. Carson, Ft. Riley, Ft. Bliss, Camp Pendleton, Norfolk NAB, San Diego NAB
- **Tier 3 (Support/Training)**: Added as needed

## Quickstart

```bash
pip install -r requirements.txt
cp config/installations.yaml.example config/installations.yaml
python -m new.db.init
python -m new.collectors.uhaul  # run a single collector
uvicorn new.api.main:app --reload
```

## Project Layout

```
new/
├── config/
│   ├── installations.yaml    # base definitions + geocoords
│   └── settings.py           # env-driven config
├── collectors/
│   ├── base.py               # abstract collector interface
│   ├── uhaul.py              # U-Haul pricing/availability
│   ├── zillow.py             # real estate listing data
│   ├── google_places.py      # foot traffic
│   ├── reddit.py             # social monitoring
│   └── news_rss.py           # local news feeds
├── db/
│   ├── init.py               # schema creation
│   ├── store.py              # write helpers
│   └── queries.py            # analytical queries
├── analysis/
│   ├── baseline.py           # per-installation baseline calc
│   └── anomaly.py            # deviation detection
├── api/
│   ├── main.py               # FastAPI app
│   └── routes.py             # query endpoints
└── utils/
    ├── geo.py                # geocoding helpers
    └── logging.py            # structured logging
```
