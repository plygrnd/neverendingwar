# NEW (Neverending War) — Architecture Iteration Plan

You are working on **NEW**, an OSINT collection and anomaly detection framework that monitors publicly observable consumer behavior around US military installations to infer force posture changes (deployments, mobilizations, unusual troop movements).

The codebase is a working scaffold (~1500 lines of Python). The architecture is sound but has several structural issues that need to be resolved before any collector runs in production. **Do not add features. Do not add new collectors. Focus exclusively on making the existing code production-safe and structurally correct.**

## Repository Layout

```
new/
├── config/
│   ├── settings.py              # dataclass-based env config
│   └── installations.yaml.example
├── collectors/
│   ├── base.py                  # abstract collector with run loop
│   ├── uhaul.py                 # U-Haul pricing/availability scraper
│   ├── zillow.py                # real estate listing collector
│   └── reddit.py                # social signal keyword monitor
├── db/
│   ├── init.py                  # DuckDB schema DDL
│   └── store.py                 # write helpers
├── analysis/
│   └── anomaly.py               # baseline calc + z-score detection
├── api/
│   └── main.py                  # FastAPI query endpoints
├── scheduler.py                 # threaded collector runner
└── requirements.txt
```

## Critical Issues to Fix (ordered by severity)

### 1. DuckDB Concurrency — Connection-per-write will deadlock under threading

`db/store.py` opens and closes a DuckDB connection on every single `store_observation()` call. The scheduler in `scheduler.py` runs collectors in daemon threads. DuckDB is single-writer — concurrent threads calling `store_observation()` will produce locking errors.

**Fix:** Implement a connection management strategy. Options in order of preference:
- A write queue (e.g., `queue.Queue`) with a single dedicated writer thread that batches inserts
- A module-level connection with a threading lock
- Batch collection: have `collect()` return a list of records, and let `BaseCollector.run()` do a single bulk insert after collection completes

The batch approach is cleanest — it also eliminates the need for `store_observation()` to be called inside collector logic at all. Collectors become pure functions: take an installation config, return a list of observation dicts.

### 2. Observation Deduplication — No unique constraint, duplicate runs inflate baselines

If a collector runs twice in the same day for the same installation/source/metric/route combination, you get duplicate records. These duplicates corrupt baseline statistics (inflated sample counts, biased means).

**Fix:** Add a unique constraint or implement upsert logic. The natural key for `observations` is `(installation_id, source, observed_date, metric_name, metadata_hash)` where `metadata_hash` captures the specific route/pair being measured (e.g., the U-Haul origin/destination pair). Similarly for `text_observations`, deduplicate on URL.

### 3. Package Structure — Broken imports

The project uses bare module imports (`from config.settings import settings`, `from db.store import ...`) but has no `pyproject.toml`, `setup.py`, or any mechanism to make `new/` a proper importable package. The `__init__.py` files exist but the root package isn't installable.

**Fix:** Add a `pyproject.toml` with a minimal `[project]` and `[tool.setuptools.packages]` config. Or restructure so `new/` is the package root with a `-e .` install. The README's `python -m new.db.init` invocation implies this was the intent.

### 4. Collector Return Contract — Mixed responsibilities

Collectors currently do two things: fetch data AND write to the database (calling `store_observation()` inline). This makes them untestable without a live database and tightly couples collection logic to storage.

**Fix:** Refactor collectors to return structured data (list of dataclass/TypedDict records). Move all database writes to `BaseCollector.run()` or a dedicated ingestion layer. The `collect()` method signature becomes:

```python
@abstractmethod
def collect(self, installation: dict) -> list[Observation]:
    """Collect data for a single installation. Returns observation records."""
    ...
```

Define `Observation` and `TextObservation` as dataclasses in a `new/models.py` or similar.

### 5. SQL Injection Surface in anomaly.py

`analysis/anomaly.py` uses f-string interpolation to inject a SQL fragment:

```python
season_filter = "AND EXTRACT(MONTH FROM observed_date) NOT BETWEEN 5 AND 8"
# ...
f"...{season_filter}"
```

This is not user-controlled input so it's not exploitable, but it's bad practice that will spread. Refactor to use parameterized queries or conditional query building.

### 6. Error Handling in Collectors — Swallowed exceptions

`BaseCollector.run()` catches all exceptions per-installation and logs them, which is correct for resilience. But the individual collectors also have internal try/except blocks (e.g., `uhaul.py` catches per-route failures) that log warnings but don't propagate partial failure counts. The `records` counter only reflects successful writes, giving a false picture of collection health.

**Fix:** Track both `records_collected` and `records_failed` in collection runs. Add error counts to the `complete_collection_run()` call.

### 7. Configuration Validation — No validation on installation configs

`load_installations()` loads YAML and trusts it entirely. Missing required fields (`lat`, `lon`, `id`, `tier`) will cause cryptic runtime errors deep in collector logic.

**Fix:** Add validation in `load_installations()`. A Pydantic model for `Installation` is ideal here — it also replaces the raw dict passing throughout the codebase with a typed object. If you use Pydantic, make `Installation` the type hint for `collect()` instead of `dict`.

### 8. The Zillow Collector — Flip the priority

The Zillow collector tries `GetSearchPageState` first (heavily rate-limited and fingerprinted since mid-2024, will fail) then falls back to Redfin data downloads (stub that returns `[]`). Flip the priority: make Redfin the primary path and Zillow the opportunistic fallback. The Redfin TSV download is public, reliable, and contains all the metrics the collector needs (inventory, median DOM, median price).

### 9. Scheduler — Use proper scheduling, not sleep loops

The threaded sleep-loop scheduler works but has drift (collection time isn't subtracted from interval), no jitter, no backoff on repeated failures, and no way to trigger an ad-hoc run.

**Fix:** Replace with APScheduler (already a common dependency, lightweight) or at minimum fix the drift issue by calculating next run time from wall clock rather than sleeping a fixed interval after completion.

## Constraints

- **Python 3.11+** — use modern typing, `|` union syntax, etc.
- **DuckDB** — keep it. No Postgres, no SQLite. The analytical query patterns are the whole point.
- **No new collectors** — fix the three that exist. Don't add Google Places, news RSS, or anything else.
- **No auth on the API yet** — that's a later concern. Don't add API keys, JWT, or middleware.
- **Tests** — add pytest tests for the refactored components. Focus on: observation deduplication logic, baseline calculation, anomaly detection thresholds, and collector data parsing (U-Haul HTML parsing, Reddit scoring). Mock external HTTP calls.
- **Keep it simple** — this runs on a single VPS. No Celery, no Redis, no message brokers. APScheduler or cron is the ceiling for scheduling complexity.

## Execution Order

1. Models and types (`models.py` — Observation, TextObservation, Installation dataclasses/Pydantic models)
2. Package structure (`pyproject.toml`, fix imports)
3. Database layer (connection management, deduplication, batch writes)
4. Collector refactor (pure collection functions returning typed records)
5. Anomaly detection cleanup (parameterized queries, PCS season handling)
6. Scheduler replacement (APScheduler or fixed sleep-loop)
7. Tests
8. Zillow/Redfin priority flip

Work through these sequentially. Commit after each logical unit is complete and passing tests.