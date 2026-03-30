"""Initialize DuckDB schema for NEW."""

import duckdb
from pathlib import Path
from config.settings import settings


SCHEMA_SQL = """
-- Sequences must be created before tables that reference them
CREATE SEQUENCE IF NOT EXISTS obs_seq START 1;
CREATE SEQUENCE IF NOT EXISTS anom_seq START 1;
CREATE SEQUENCE IF NOT EXISTS text_seq START 1;
CREATE SEQUENCE IF NOT EXISTS run_seq START 1;

-- Raw observations from collectors, normalized to a common structure
-- Every collector writes here with a source tag and source-specific payload

CREATE TABLE IF NOT EXISTS observations (
    id              INTEGER PRIMARY KEY DEFAULT(nextval('obs_seq')),
    installation_id VARCHAR NOT NULL,       -- keys to installations.yaml
    source          VARCHAR NOT NULL,       -- 'uhaul', 'zillow', 'google_places', 'reddit', 'news'
    collected_at    TIMESTAMP NOT NULL,     -- when we scraped it
    observed_date   DATE NOT NULL,          -- what date the observation represents
    metric_name     VARCHAR NOT NULL,       -- e.g. 'one_way_price', 'listing_count', 'foot_traffic'
    metric_value    DOUBLE NOT NULL,        -- numeric value
    metadata        JSON,                   -- source-specific details
    metadata_hash   VARCHAR DEFAULT '',     -- deterministic hash for deduplication
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_dedup
    ON observations (installation_id, source, observed_date, metric_name, metadata_hash);

-- Computed baselines per installation/source/metric
CREATE TABLE IF NOT EXISTS baselines (
    installation_id VARCHAR NOT NULL,
    source          VARCHAR NOT NULL,
    metric_name     VARCHAR NOT NULL,
    window_start    DATE NOT NULL,
    window_end      DATE NOT NULL,
    mean            DOUBLE NOT NULL,
    stddev          DOUBLE NOT NULL,
    median          DOUBLE NOT NULL,
    sample_count    INTEGER NOT NULL,
    computed_at     TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (installation_id, source, metric_name)
);

-- Detected anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id              INTEGER PRIMARY KEY DEFAULT(nextval('anom_seq')),
    installation_id VARCHAR NOT NULL,
    source          VARCHAR NOT NULL,
    metric_name     VARCHAR NOT NULL,
    observed_date   DATE NOT NULL,
    metric_value    DOUBLE NOT NULL,
    baseline_mean   DOUBLE NOT NULL,
    baseline_stddev DOUBLE NOT NULL,
    zscore          DOUBLE NOT NULL,
    severity        VARCHAR NOT NULL,       -- 'elevated', 'high', 'critical'
    detected_at     TIMESTAMP NOT NULL DEFAULT current_timestamp,
    metadata        JSON,
);

-- Text observations (reddit posts, news articles) that don't reduce to a single metric
CREATE TABLE IF NOT EXISTS text_observations (
    id              INTEGER PRIMARY KEY DEFAULT(nextval('text_seq')),
    installation_id VARCHAR NOT NULL,
    source          VARCHAR NOT NULL,
    collected_at    TIMESTAMP NOT NULL,
    observed_date   DATE NOT NULL,
    title           VARCHAR,
    body            VARCHAR,
    url             VARCHAR,
    relevance_score DOUBLE,                 -- keyword match score
    metadata        JSON,
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_text_dedup
    ON text_observations (installation_id, source, url);

-- Collection run log for monitoring collector health
CREATE TABLE IF NOT EXISTS collection_runs (
    id              INTEGER PRIMARY KEY DEFAULT(nextval('run_seq')),
    source          VARCHAR NOT NULL,
    installation_id VARCHAR,                -- null for global runs
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP,
    status          VARCHAR NOT NULL,       -- 'running', 'success', 'error'
    records_collected INTEGER DEFAULT 0,
    records_failed  INTEGER DEFAULT 0,
    error_message   VARCHAR,
);
"""


def init_db():
    """Create the DuckDB database and schema."""
    db_dir = Path(settings.db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(settings.db_path)
    con.execute(SCHEMA_SQL)
    con.close()
    print(f"Database initialized at {settings.db_path}")


if __name__ == "__main__":
    init_db()
