"""Shared test fixtures."""

import os
import pytest
import duckdb

# Set a temp DB path before any app imports
os.environ["NEW_DB_PATH"] = ":memory:"

from db.init import SCHEMA_SQL
from models import Installation
import db.store as store_module


@pytest.fixture(autouse=True)
def fresh_db(monkeypatch):
    """Provide a fresh in-memory DuckDB for each test, monkeypatched into db.store."""
    con = duckdb.connect(":memory:")
    con.execute(SCHEMA_SQL)

    # Monkeypatch the singleton connection
    monkeypatch.setattr(store_module, "_connection", con)

    yield con

    con.close()
    # Reset singleton so next test gets a fresh one
    monkeypatch.setattr(store_module, "_connection", None)


@pytest.fixture
def sample_installation() -> Installation:
    return Installation(
        id="ft_liberty",
        name="Fort Liberty (Bragg)",
        branch="army",
        tier=1,
        lat=35.138,
        lon=-79.0064,
        radius_km=25,
        nearby_city="Fayetteville, NC",
        county="Cumberland County, NC",
        uhaul_locations=["Fayetteville, NC", "Spring Lake, NC"],
        zillow_region="fayetteville-nc",
        subreddits=["r/fayetteville", "r/FortLiberty"],
    )
