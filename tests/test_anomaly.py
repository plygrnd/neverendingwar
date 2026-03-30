"""Tests for baseline calculation and anomaly detection."""

from datetime import date, timedelta
from models import Observation
from db.store import batch_store_observations
from analysis.anomaly import compute_baselines, detect_anomalies


class TestBaselineAndAnomaly:
    def _seed_observations(self, fresh_db, metric_value=100.0, count=30):
        """Seed `count` days of observations with a constant value."""
        obs_list = []
        for i in range(count):
            obs_list.append(Observation(
                installation_id="ft_liberty",
                source="uhaul",
                observed_date=date.today() - timedelta(days=i + 1),
                metric_name="outbound_price_10ft_truck",
                metric_value=metric_value,
                metadata={"origin": "Fayetteville, NC", "destination": "Atlanta, GA"},
            ))
        batch_store_observations(obs_list)

    def test_baseline_computed(self, fresh_db):
        """Baselines should be computed for metrics with enough data."""
        self._seed_observations(fresh_db, metric_value=100.0, count=10)
        compute_baselines()

        row = fresh_db.execute(
            "SELECT mean, sample_count FROM baselines WHERE metric_name = 'outbound_price_10ft_truck'"
        ).fetchone()
        assert row is not None
        assert row[0] == 100.0  # mean
        assert row[1] == 10  # sample_count

    def test_baseline_skipped_insufficient_data(self, fresh_db):
        """Should not compute baseline with fewer than 7 observations."""
        self._seed_observations(fresh_db, metric_value=100.0, count=5)
        compute_baselines()

        row = fresh_db.execute("SELECT COUNT(*) FROM baselines").fetchone()
        assert row[0] == 0

    def test_anomaly_detected(self, fresh_db):
        """An outlier observation should be flagged as an anomaly."""
        # Seed stable baseline
        self._seed_observations(fresh_db, metric_value=100.0, count=30)
        compute_baselines()

        # Insert outlier today
        outlier = Observation(
            installation_id="ft_liberty",
            source="uhaul",
            observed_date=date.today(),
            metric_name="outbound_price_10ft_truck",
            metric_value=500.0,  # way above baseline of 100
            metadata={"origin": "Fayetteville, NC", "destination": "Atlanta, GA"},
        )
        batch_store_observations([outlier])

        detect_anomalies(lookback_days=1)

        anomalies = fresh_db.execute("SELECT * FROM anomalies").fetchall()
        assert len(anomalies) >= 1

        # Check severity — with stddev epsilon of 0.001, z-score is huge → critical
        severity = anomalies[0][9]  # severity column
        assert severity == "critical"

    def test_no_anomaly_for_normal_value(self, fresh_db):
        """A value close to baseline should not trigger an anomaly."""
        self._seed_observations(fresh_db, metric_value=100.0, count=30)
        compute_baselines()

        normal = Observation(
            installation_id="ft_liberty",
            source="uhaul",
            observed_date=date.today(),
            metric_name="outbound_price_10ft_truck",
            metric_value=100.0,
            metadata={"origin": "Fayetteville, NC", "destination": "Atlanta, GA"},
        )
        batch_store_observations([normal])

        detect_anomalies(lookback_days=1)

        anomalies = fresh_db.execute("SELECT * FROM anomalies").fetchall()
        assert len(anomalies) == 0
