"""Tests for U-Haul HTML price parsing."""

from collectors.uhaul import UHaulCollector


SAMPLE_RATES_HTML = """
<html>
<body>
<h1>Truck rates</h1>
<div class="truck-card" data-model-code="TM">
    <span class="truck-name">10' Truck</span>
    <span class="price">$1,234.56</span>
</div>
<div class="truck-card" data-model-code="DC">
    <span class="truck-name">15' Truck</span>
    <span class="price">$1,567.89</span>
</div>
<div class="truck-card" data-model-code="TT">
    <span class="truck-name">20' Truck</span>
    <span class="price">$2,100.00</span>
</div>
<div class="truck-card" data-model-code="JH">
    <span class="truck-name">26' Truck</span>
    <!-- no price, truck unavailable -->
</div>
<div class="truck-card" data-model-code="XX">
    <span class="truck-name">Unknown model</span>
    <span class="price">$999.99</span>
</div>
</body>
</html>
"""


class TestParserPrices:
    def setup_method(self):
        # _parse_prices is a pure function on the instance, no init side effects needed
        self.collector = UHaulCollector.__new__(UHaulCollector)

    def test_extracts_known_models(self):
        prices = self.collector._parse_prices(SAMPLE_RATES_HTML)
        assert prices["10ft_truck"] == 1234.56
        assert prices["15ft_truck"] == 1567.89
        assert prices["20ft_truck"] == 2100.00

    def test_unavailable_truck_is_none(self):
        prices = self.collector._parse_prices(SAMPLE_RATES_HTML)
        assert prices["26ft_truck"] is None

    def test_unknown_model_ignored(self):
        prices = self.collector._parse_prices(SAMPLE_RATES_HTML)
        assert "unknown" not in str(prices).lower()
        assert len(prices) == 4  # only the 4 known models

    def test_non_rates_page_returns_empty(self):
        prices = self.collector._parse_prices("<html><body>Error</body></html>")
        assert prices == {}
