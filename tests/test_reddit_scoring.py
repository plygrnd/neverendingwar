"""Tests for Reddit keyword relevance scoring."""

from collectors.reddit import RedditCollector


class TestScoreRelevance:
    def setup_method(self):
        self.collector = RedditCollector.__new__(RedditCollector)

    def test_no_keywords_returns_zero(self):
        assert self.collector._score_relevance("just a normal day at the office") == 0.0

    def test_deployment_keyword(self):
        score = self.collector._score_relevance("my husband is deploying next month")
        assert score > 0
        # "deploying" matches deployment category (weight 1.0)
        assert score >= 1.0

    def test_multiple_categories(self):
        text = "deploying soon, need to sell house fast, short notice orders"
        score = self.collector._score_relevance(text)
        # Should match deployment, relocation, and urgency
        assert score > 2.0

    def test_case_insensitive(self):
        score_lower = self.collector._score_relevance("pcs orders came through")
        score_upper = self.collector._score_relevance("PCS orders came through")
        # Both should match since text is lowered
        assert score_lower > 0
        assert score_upper > 0

    def test_preparation_keywords(self):
        text = "updating my will and power of attorney before he leaves"
        score = self.collector._score_relevance(text)
        assert score > 0


class TestMatchedCategories:
    def setup_method(self):
        self.collector = RedditCollector.__new__(RedditCollector)

    def test_returns_matching_categories(self):
        text = "deploying soon, need to sell house asap"
        cats = self.collector._matched_categories(text)
        assert "deployment" in cats
        assert "urgency" in cats

    def test_empty_for_no_matches(self):
        cats = self.collector._matched_categories("beautiful weather today")
        assert cats == []
