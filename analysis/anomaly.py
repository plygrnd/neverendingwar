"""
Baseline calculation and anomaly detection.

For each installation/source/metric combination, calculates a rolling
baseline (mean, stddev, median) over a configurable window. Then flags
observations that deviate significantly from baseline.

PCS season adjustment: May-August is treated as a separate baseline
because normal military rotation creates elevated signals that would
otherwise mask real anomalies during those months.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from config.settings import settings
from db.store import _execute

logger = logging.getLogger("new.analysis")


def compute_baselines():
    """Recalculate baselines for all installation/source/metric combos."""
    window_days = settings.baseline_window_days
    cutoff = date.today() - timedelta(days=window_days)

    # Get all unique combos that have data
    combos = _execute(
        """
        SELECT DISTINCT installation_id, source, metric_name
        FROM observations
        WHERE observed_date >= ?
        """,
        [cutoff],
    ).fetchall()

    for installation_id, source, metric_name in combos:
        # Calculate baseline stats, excluding PCS season if we're not in it
        current_month = date.today().month
        in_pcs_season = 5 <= current_month <= 8

        if not in_pcs_season:
            # Exclude PCS season months from baseline when outside PCS season
            stats = _execute(
                """
                SELECT
                    AVG(metric_value) as mean,
                    STDDEV(metric_value) as stddev,
                    MEDIAN(metric_value) as median,
                    COUNT(*) as n
                FROM observations
                WHERE installation_id = ?
                  AND source = ?
                  AND metric_name = ?
                  AND observed_date >= ?
                  AND EXTRACT(MONTH FROM observed_date) NOT BETWEEN ? AND ?
                """,
                [installation_id, source, metric_name, cutoff, 5, 8],
            ).fetchone()
        else:
            stats = _execute(
                """
                SELECT
                    AVG(metric_value) as mean,
                    STDDEV(metric_value) as stddev,
                    MEDIAN(metric_value) as median,
                    COUNT(*) as n
                FROM observations
                WHERE installation_id = ?
                  AND source = ?
                  AND metric_name = ?
                  AND observed_date >= ?
                """,
                [installation_id, source, metric_name, cutoff],
            ).fetchone()

        mean, stddev, median, n = stats

        if n < 7:
            # not enough data points for a meaningful baseline
            logger.debug(
                f"Skipping {installation_id}/{source}/{metric_name}: only {n} observations"
            )
            continue

        # stddev of 0 means constant values -- use a small epsilon
        if stddev is None or stddev == 0:
            stddev = 0.001

        # Upsert baseline
        _execute(
            """
            INSERT OR REPLACE INTO baselines
                (installation_id, source, metric_name, window_start, window_end,
                 mean, stddev, median, sample_count, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                installation_id, source, metric_name,
                cutoff, date.today(),
                mean, stddev, median, n,
                datetime.now(timezone.utc),
            ],
        )

    logger.info(f"Baselines computed for {len(combos)} metric combinations")


def detect_anomalies(lookback_days: int = 1):
    """
    Check recent observations against baselines and flag anomalies.

    Severity levels:
    - elevated: z-score > threshold (default 2.0)
    - high:     z-score > threshold * 1.5
    - critical: z-score > threshold * 2.0
    """
    threshold = settings.anomaly_zscore_threshold
    cutoff = date.today() - timedelta(days=lookback_days)

    # Find observations that exceed baseline
    anomalies = _execute(
        """
        SELECT
            o.installation_id,
            o.source,
            o.metric_name,
            o.observed_date,
            o.metric_value,
            b.mean,
            b.stddev,
            (o.metric_value - b.mean) / b.stddev as zscore,
            o.metadata
        FROM observations o
        JOIN baselines b
            ON o.installation_id = b.installation_id
            AND o.source = b.source
            AND o.metric_name = b.metric_name
        WHERE o.observed_date >= ?
          AND ABS((o.metric_value - b.mean) / b.stddev) > ?
        ORDER BY ABS((o.metric_value - b.mean) / b.stddev) DESC
        """,
        [cutoff, threshold],
    ).fetchall()

    inserted = 0
    for row in anomalies:
        (inst_id, source, metric, obs_date, value,
         mean, stddev, zscore, metadata) = row

        abs_z = abs(zscore)
        if abs_z > threshold * 2.0:
            severity = "critical"
        elif abs_z > threshold * 1.5:
            severity = "high"
        else:
            severity = "elevated"

        _execute(
            """
            INSERT INTO anomalies
                (installation_id, source, metric_name, observed_date,
                 metric_value, baseline_mean, baseline_stddev, zscore,
                 severity, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [inst_id, source, metric, obs_date, value,
             mean, stddev, zscore, severity, metadata],
        )
        inserted += 1

        if severity in ("high", "critical"):
            logger.warning(
                f"ANOMALY [{severity}] {inst_id}/{source}/{metric}: "
                f"value={value:.2f} baseline={mean:.2f} z={zscore:.2f}"
            )

    logger.info(f"Anomaly detection complete: {inserted} anomalies from {len(anomalies)} deviations")


def correlated_anomalies(days: int = 3):
    """
    Find installations with anomalies across multiple sources within a
    time window. Cross-source correlation is where the real signal lives.
    """
    cutoff = date.today() - timedelta(days=days)

    results = _execute(
        """
        SELECT
            installation_id,
            COUNT(DISTINCT source) as source_count,
            LIST(DISTINCT source) as sources,
            LIST(DISTINCT metric_name) as metrics,
            MAX(zscore) as max_zscore,
            MAX(severity) as max_severity
        FROM anomalies
        WHERE detected_at >= ?
        GROUP BY installation_id
        HAVING COUNT(DISTINCT source) >= 2
        ORDER BY source_count DESC, max_zscore DESC
        """,
        [cutoff],
    ).fetchall()

    return results


if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level)
    compute_baselines()
    detect_anomalies()

    correlated = correlated_anomalies()
    if correlated:
        print("\n=== CORRELATED ANOMALIES ===")
        for row in correlated:
            print(f"  {row[0]}: {row[1]} sources ({row[2]}), max z={row[4]:.2f} [{row[5]}]")
    else:
        print("\nNo correlated anomalies detected.")
