"""
Real estate listing collector.

Tracks listing volume, days-on-market, price cuts, and new listing velocity
in base-adjacent markets. Anomalous listing surges or aggressive pricing
outside normal PCS season (May-August) are potential indicators.

Uses Zillow's unofficial API endpoints. For production, consider:
- ATTOM Data API (paid, structured, reliable)
- Redfin's data downloads (free, monthly granularity)
- Realtor.com API
- Direct MLS feeds via RESO Web API (if you can get access)
"""

import csv
import gzip
import io
import logging
import httpx
from datetime import date
from pathlib import Path
from collectors.base import BaseCollector
from config.settings import settings
from models import Installation, Observation, CollectorResult

logger = logging.getLogger("new.collectors.zillow")


class ZillowCollector(BaseCollector):
    source_name = "zillow"

    def __init__(self):
        super().__init__()
        self.client = httpx.Client(
            headers={
                "User-Agent": settings.user_agent,
                "Accept": "application/json",
            },
            follow_redirects=True,
            timeout=30.0,
        )

    def collect(self, installation: Installation) -> CollectorResult:
        """Collect listing metrics for an installation's real estate market."""
        region = installation.zillow_region
        if not region:
            logger.info(f"No zillow_region for {installation.id}, skipping")
            return CollectorResult()

        try:
            observations = self._get_market_metrics(region, installation)
        except Exception as e:
            logger.error(f"Failed to collect for {region}: {e}")
            raise

        return CollectorResult(observations=observations)

    def _get_market_metrics(
        self, region: str, installation: Installation
    ) -> list[Observation]:
        """
        Pull market-level metrics for a region.

        Metrics we care about:
        - active_listings: total for-sale inventory
        - new_listings_7d: new listings in the past week
        - median_dom: median days on market
        - price_cut_pct: percentage of listings with price reductions
        - median_list_price: for tracking urgency pricing

        NOTE: Zillow's actual API requires partner access. This scaffold
        targets their search endpoint which returns structured data.
        Real implementation options:

        1. Zillow's GetSearchPageState endpoint (undocumented, fragile)
        2. Redfin's data center downloads (free, reliable, monthly lag)
        3. ATTOM API (paid, $0.01-0.05/record, excellent coverage)
        4. Scrape individual listing pages and aggregate
        """
        # Primary: Redfin public data (reliable, free)
        try:
            redfin_results = self._redfin_data(region, installation)
            if redfin_results:
                return redfin_results
        except Exception as e:
            logger.debug(f"Redfin download failed: {e}")

        # Fallback: Zillow search API (undocumented, fragile, rate-limited)
        try:
            search_results = self._zillow_search(region, installation)
            if search_results:
                return search_results
        except Exception as e:
            logger.debug(f"Zillow API failed: {e}")

        return []

    def _zillow_search(
        self, region: str, installation: Installation
    ) -> list[Observation]:
        """
        Query Zillow's search endpoint for aggregate market data.

        The GetSearchPageState endpoint returns listing counts and
        aggregate metrics when queried with geographic bounds.
        """
        lat, lon = installation.lat, installation.lon
        radius_deg = installation.radius_km / 111  # rough km->degree

        # Zillow's internal search API
        url = "https://www.zillow.com/search/GetSearchPageState.htm"
        search_query = {
            "pagination": {},
            "mapBounds": {
                "north": lat + radius_deg,
                "south": lat - radius_deg,
                "east": lon + radius_deg,
                "west": lon - radius_deg,
            },
            "filterState": {
                "isForSaleByAgent": {"value": True},
                "isForSaleByOwner": {"value": True},
                "isNewConstruction": {"value": False},
                "isForSaleForeclosure": {"value": False},
                "isComingSoon": {"value": False},
                "isAuction": {"value": False},
            },
        }

        resp = self.client.get(
            url,
            params={"searchQueryState": str(search_query)},
            headers={"Referer": f"https://www.zillow.com/{region}/"},
        )

        if resp.status_code != 200:
            raise Exception(f"Zillow returned {resp.status_code}")

        data = resp.json()
        results_count = (
            data.get("cat1", {}).get("searchList", {}).get("totalResultCount", 0)
        )

        base_meta = {"region": region, "source_api": "zillow_search"}
        observations = [
            Observation(
                installation_id=installation.id,
                source=self.source_name,
                observed_date=date.today(),
                metric_name="active_listings",
                metric_value=float(results_count),
                metadata=base_meta,
            ),
        ]

        # Extract individual listings for DOM and price cut analysis
        listings = (
            data.get("cat1", {})
            .get("searchResults", {})
            .get("listResults", [])
        )

        if listings:
            dom_values = []
            price_cuts = 0
            prices = []

            for listing in listings:
                dom = listing.get("hdpData", {}).get("homeInfo", {}).get("daysOnZillow")
                if dom is not None:
                    dom_values.append(dom)

                price_change = listing.get("hdpData", {}).get("homeInfo", {}).get("priceChange")
                if price_change and price_change < 0:
                    price_cuts += 1

                price = listing.get("price")
                if price:
                    # price comes as string like "$250,000" or as int
                    if isinstance(price, str):
                        price = float(price.replace("$", "").replace(",", ""))
                    prices.append(price)

            if dom_values:
                median_dom = sorted(dom_values)[len(dom_values) // 2]
                observations.append(Observation(
                    installation_id=installation.id,
                    source=self.source_name,
                    observed_date=date.today(),
                    metric_name="median_dom",
                    metric_value=float(median_dom),
                    metadata=base_meta,
                ))

            if listings:
                cut_pct = (price_cuts / len(listings)) * 100
                observations.append(Observation(
                    installation_id=installation.id,
                    source=self.source_name,
                    observed_date=date.today(),
                    metric_name="price_cut_pct",
                    metric_value=cut_pct,
                    metadata=base_meta,
                ))

            if prices:
                median_price = sorted(prices)[len(prices) // 2]
                observations.append(Observation(
                    installation_id=installation.id,
                    source=self.source_name,
                    observed_date=date.today(),
                    metric_name="median_list_price",
                    metric_value=median_price,
                    metadata=base_meta,
                ))

        return observations

    def _redfin_data(
        self, region: str, installation: Installation
    ) -> list[Observation]:
        """
        Pull from Redfin's public data downloads.

        Redfin publishes market-level TSVs at their S3 data center.
        These update monthly and include: homes_sold, inventory,
        median_dom, median_sale_price, etc. Less granular than
        real-time scraping but very reliable.

        The TSV is ~100MB gzipped. We cache it locally for 24h and
        filter by the installation's city or county.
        """
        cache_path = Path(settings.db_path).parent / "redfin_city_market.tsv.gz"
        cache_max_age = 86400  # 24 hours

        # Download if cache missing or stale
        need_download = True
        if cache_path.exists():
            import time
            age = time.time() - cache_path.stat().st_mtime
            if age < cache_max_age:
                need_download = False

        if need_download:
            url = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz"
            logger.info(f"Downloading Redfin data to {cache_path}")
            try:
                with self.client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(cache_path, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=65536):
                            f.write(chunk)
            except Exception as e:
                logger.warning(f"Failed to download Redfin data: {e}")
                if not cache_path.exists():
                    return []

        # Filter rows matching this installation's city or county
        city_name = installation.nearby_city.split(",")[0].strip().lower()
        county_name = installation.county.lower() if installation.county else ""

        matching_rows: list[dict] = []
        with gzip.open(cache_path, "rt", encoding="utf-8") as gz:
            reader = csv.DictReader(gz, delimiter="\t")
            for row in reader:
                row_city = row.get("city", "").lower()
                row_county = row.get("county_name", "").lower()
                if (city_name and city_name in row_city) or (
                    county_name and county_name in row_county
                ):
                    matching_rows.append(row)

        if not matching_rows:
            logger.debug(f"No Redfin data for {city_name}/{county_name}")
            return []

        # Use the most recent row
        matching_rows.sort(key=lambda r: r.get("period_end", ""), reverse=True)
        latest = matching_rows[0]

        base_meta = {"region": region, "source_api": "redfin"}
        observations: list[Observation] = []

        # Map Redfin columns to our metric names
        field_map = {
            "inventory": "active_listings",
            "median_dom": "median_dom",
            "median_sale_price": "median_list_price",
            "homes_sold": "homes_sold",
        }

        for redfin_col, metric_name in field_map.items():
            raw = latest.get(redfin_col, "")
            if raw and raw not in ("", "N/A"):
                try:
                    value = float(raw)
                    observations.append(Observation(
                        installation_id=installation.id,
                        source=self.source_name,
                        observed_date=date.today(),
                        metric_name=metric_name,
                        metric_value=value,
                        metadata=base_meta,
                    ))
                except ValueError:
                    pass

        return observations


def main():
    """CLI entry point."""
    import sys
    logging.basicConfig(level=settings.log_level)
    collector = ZillowCollector()
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    collector.run(installation_ids=targets)


if __name__ == "__main__":
    main()
