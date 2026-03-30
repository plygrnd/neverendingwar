"""
U-Haul pricing and availability collector.

Proven scraping flow (no JS rendering required):
1. POST to /Misc/EquipmentSearch/ with origin/destination/date
   - Sets server-side session state
   - Returns JSON with redirect URL
   - Session carried via cookies
2. GET /Reservations/RatesTrucks/
   - Returns SSR HTML with truck pricing
   - Prices embedded in plain HTML, no JS needed

Key signal: DIRECTIONAL PRICE RATIO (outbound_from_base / inbound_to_base)
- Ratio < 1.0 = cheaper to leave base area (surplus outbound trucks, normal)
- Ratio > 1.0 = more expensive to leave (high outbound demand, possible mobilization)
- Ratio changes over time are more important than absolute values

Model codes:
  TM = 10' Truck
  DC = 15' Truck
  TT = 20' Truck
  JH = 26' Truck
  (8' Pickup and 9' Cargo Van not available for one-way long-distance)
"""

import re
import json
import time
import logging
import http.cookiejar
import urllib.request
import urllib.parse
from datetime import date, timedelta
from collectors.base import BaseCollector
from config.settings import settings
from models import Installation, Observation, CollectorResult

logger = logging.getLogger("new.collectors.uhaul")

MODEL_MAP = {
    "TM": "10ft_truck",
    "DC": "15ft_truck",
    "TT": "20ft_truck",
    "JH": "26ft_truck",
}

# Each base checks outbound pricing against these destinations.
# Also checks inbound FROM these destinations for ratio calculation.
# Destinations chosen to be geographically dispersed common relocation targets.
CANARY_DESTINATIONS = [
    "Atlanta, GA",
    "San Antonio, TX",
    "Virginia Beach, VA",
    "Phoenix, AZ",
    "Columbia, SC",
]


class UHaulCollector(BaseCollector):
    source_name = "uhaul"

    def collect(self, installation: Installation) -> CollectorResult:
        """Collect one-way pricing for each origin/destination pair, both directions."""
        observations: list[Observation] = []
        errors = 0
        origins = installation.uhaul_locations
        # Check pricing 3 days out to avoid same-day availability noise
        check_date = (date.today() + timedelta(days=3)).strftime("%m/%d/%Y")

        for origin in origins:
            origin_state = origin.split(",")[-1].strip()

            for destination in CANARY_DESTINATIONS:
                dest_state = destination.split(",")[-1].strip()
                if origin_state == dest_state:
                    continue  # skip same-state, not meaningful for one-way

                # Outbound: base -> destination
                out_prices: dict = {}
                try:
                    out_prices = self._get_prices(origin, destination, check_date)
                    for truck_size, price in out_prices.items():
                        if price is not None:
                            observations.append(Observation(
                                installation_id=installation.id,
                                source=self.source_name,
                                observed_date=date.today(),
                                metric_name=f"outbound_price_{truck_size}",
                                metric_value=price,
                                metadata={
                                    "origin": origin,
                                    "destination": destination,
                                    "direction": "outbound",
                                    "check_date": check_date,
                                },
                            ))
                        else:
                            # Truck unavailable -- this is itself a signal
                            observations.append(Observation(
                                installation_id=installation.id,
                                source=self.source_name,
                                observed_date=date.today(),
                                metric_name=f"unavailable_{truck_size}",
                                metric_value=1.0,
                                metadata={
                                    "origin": origin,
                                    "destination": destination,
                                    "direction": "outbound",
                                },
                            ))
                except Exception as e:
                    logger.warning(f"Outbound failed {origin} -> {destination}: {e}")
                    errors += 1

                time.sleep(settings.request_delay_seconds)

                # Inbound: destination -> base
                in_prices: dict = {}
                try:
                    in_prices = self._get_prices(destination, origin, check_date)
                    for truck_size, price in in_prices.items():
                        if price is not None:
                            observations.append(Observation(
                                installation_id=installation.id,
                                source=self.source_name,
                                observed_date=date.today(),
                                metric_name=f"inbound_price_{truck_size}",
                                metric_value=price,
                                metadata={
                                    "origin": destination,
                                    "destination": origin,
                                    "direction": "inbound",
                                    "check_date": check_date,
                                },
                            ))
                except Exception as e:
                    logger.warning(f"Inbound failed {destination} -> {origin}: {e}")
                    errors += 1

                time.sleep(settings.request_delay_seconds)

                # Calculate and store directional ratios
                for truck_size in MODEL_MAP.values():
                    out_p = out_prices.get(truck_size)
                    in_p = in_prices.get(truck_size)
                    if out_p and in_p and in_p > 0:
                        ratio = out_p / in_p
                        observations.append(Observation(
                            installation_id=installation.id,
                            source=self.source_name,
                            observed_date=date.today(),
                            metric_name=f"directional_ratio_{truck_size}",
                            metric_value=ratio,
                            metadata={
                                "base_origin": origin,
                                "canary_dest": destination,
                                "outbound_price": out_p,
                                "inbound_price": in_p,
                            },
                        ))

        return CollectorResult(observations=observations, errors=errors)

    def _get_prices(self, origin: str, destination: str, pickup_date: str) -> dict:
        """
        Get one-way truck rental prices via U-Haul's two-step session flow.

        Returns dict mapping truck size names to prices (float or None if unavailable).
        """
        cj = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

        # Step 1: POST to EquipmentSearch -- establishes server-side session
        post_data = urllib.parse.urlencode({
            "Scenario": "TruckOnly",
            "IsActionFrom": "False",
            "PickupLocation": origin,
            "DropoffLocation": destination,
            "PickupDate": pickup_date,
        }).encode()

        req = urllib.request.Request(
            "https://www.uhaul.com/Misc/EquipmentSearch/",
            data=post_data,
            headers={
                "User-Agent": settings.user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.uhaul.com/",
            },
        )

        resp = opener.open(req, timeout=30)
        body = resp.read().decode()
        redirect_data = json.loads(body)

        if "redirect" not in redirect_data:
            raise ValueError(f"No redirect in response: {redirect_data}")

        # Step 2: GET the rates page -- prices are in server-rendered HTML
        req2 = urllib.request.Request(
            "https://www.uhaul.com/Reservations/RatesTrucks/",
            headers={
                "User-Agent": settings.user_agent,
                "Referer": "https://www.uhaul.com/",
            },
        )

        resp2 = opener.open(req2, timeout=30)
        html = resp2.read().decode()

        return self._parse_prices(html)

    def _parse_prices(self, html: str) -> dict:
        """
        Extract truck prices from the rates page HTML.

        Each truck card contains a data-model-code attribute followed by
        the price as a dollar amount. The page splits cleanly on
        'data-model-code=' boundaries.
        """
        results = {}

        # Verify we got a rates page, not an error
        if "Truck rates" not in html and "RatesTrucks" not in html:
            logger.warning("Response doesn't appear to be a rates page")
            return results

        segments = html.split("data-model-code=")

        for seg in segments[1:]:
            code_match = re.match(r'"(\w+)"', seg)
            if not code_match:
                continue

            code = code_match.group(1)
            if code not in MODEL_MAP:
                continue

            truck_name = MODEL_MAP[code]

            # First dollar amount in this segment is the truck's price
            price_match = re.search(r"\$([\d,]+\.\d{2})", seg)
            if price_match:
                price_str = price_match.group(1).replace(",", "")
                results[truck_name] = float(price_str)
            else:
                results[truck_name] = None

        return results


def main():
    """CLI entry point. Run with optional installation IDs as args."""
    import sys

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    collector = UHaulCollector()
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    collector.run(installation_ids=targets)


if __name__ == "__main__":
    main()
