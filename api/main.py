"""
FastAPI application for NEW.

Minimal API layer for querying indicators, anomalies, and collection status.
No auth on MVP -- this runs on a shitty VPS behind whatever reverse proxy
you want. Add API key middleware when it matters.
"""

import logging
from datetime import date, timedelta
from typing import Optional
from fastapi import FastAPI, Query
from db.store import _execute
from analysis.anomaly import compute_baselines, detect_anomalies, correlated_anomalies

app = FastAPI(
    title="NEW (Neverending War)",
    description="OSINT-driven military force posture indicator framework",
    version="0.1.0",
)

logger = logging.getLogger("new.api")


@app.get("/")
def root():
    return {
        "name": "NEW",
        "version": "0.1.0",
        "status": "operational",
    }


@app.get("/installations")
def list_installations():
    """List monitored installations and their latest collection status."""
    results = _execute(
        """
        SELECT
            installation_id,
            source,
            MAX(collected_at) as last_collected,
            COUNT(*) as total_observations
        FROM observations
        GROUP BY installation_id, source
        ORDER BY installation_id, source
        """
    ).fetchall()

    grouped = {}
    for inst_id, source, last_collected, count in results:
        if inst_id not in grouped:
            grouped[inst_id] = {"sources": {}}
        grouped[inst_id]["sources"][source] = {
            "last_collected": str(last_collected),
            "total_observations": count,
        }

    return grouped


@app.get("/observations/{installation_id}")
def get_observations(
    installation_id: str,
    source: Optional[str] = None,
    metric: Optional[str] = None,
    days: int = Query(default=30, ge=1, le=365),
):
    """Get recent observations for an installation."""
    cutoff = date.today() - timedelta(days=days)

    query = """
        SELECT observed_date, source, metric_name, metric_value, metadata
        FROM observations
        WHERE installation_id = ? AND observed_date >= ?
    """
    params: list = [installation_id, cutoff]

    if source:
        query += " AND source = ?"
        params.append(source)
    if metric:
        query += " AND metric_name = ?"
        params.append(metric)

    query += " ORDER BY observed_date DESC"

    results = _execute(query, params).fetchall()

    return [
        {
            "date": str(r[0]),
            "source": r[1],
            "metric": r[2],
            "value": r[3],
            "metadata": r[4],
        }
        for r in results
    ]


@app.get("/anomalies")
def get_anomalies(
    installation_id: Optional[str] = None,
    severity: Optional[str] = None,
    days: int = Query(default=7, ge=1, le=90),
):
    """Get detected anomalies."""
    cutoff = date.today() - timedelta(days=days)

    query = "SELECT * FROM anomalies WHERE detected_at >= ?"
    params: list = [cutoff]

    if installation_id:
        query += " AND installation_id = ?"
        params.append(installation_id)
    if severity:
        query += " AND severity = ?"
        params.append(severity)

    query += " ORDER BY zscore DESC"

    results = _execute(query, params).fetchall()

    return [
        {
            "id": r[0],
            "installation_id": r[1],
            "source": r[2],
            "metric": r[3],
            "date": str(r[4]),
            "value": r[5],
            "baseline_mean": r[6],
            "baseline_stddev": r[7],
            "zscore": r[8],
            "severity": r[9],
        }
        for r in results
    ]


@app.get("/anomalies/correlated")
def get_correlated_anomalies(days: int = Query(default=3, ge=1, le=30)):
    """
    Get installations with anomalies across multiple data sources.
    This is the money endpoint -- cross-source correlation is the
    strongest signal.
    """
    results = correlated_anomalies(days=days)
    return [
        {
            "installation_id": r[0],
            "source_count": r[1],
            "sources": r[2],
            "metrics": r[3],
            "max_zscore": r[4],
            "max_severity": r[5],
        }
        for r in results
    ]


@app.post("/analyze")
def run_analysis():
    """Trigger baseline recalculation and anomaly detection."""
    compute_baselines()
    detect_anomalies()
    correlated = correlated_anomalies()
    return {
        "status": "complete",
        "correlated_installations": len(correlated),
        "details": [
            {"installation_id": r[0], "sources": r[1], "max_zscore": r[4]}
            for r in correlated
        ],
    }


@app.get("/health")
def health_check():
    """Collection health -- are collectors running on schedule?"""
    runs = _execute(
        """
        SELECT
            source,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
            MAX(completed_at) as last_run,
            SUM(records_collected) as total_records
        FROM collection_runs
        WHERE started_at >= current_date - INTERVAL '24 hours'
        GROUP BY source
        """
    ).fetchall()

    return {
        source: {
            "runs_24h": total,
            "errors_24h": errors,
            "last_run": str(last_run) if last_run else None,
            "records_24h": total_records,
        }
        for source, total, errors, last_run, total_records in runs
    }
