"""
Reddit social signal collector.

Monitors military and base-adjacent subreddits for deployment-related
keywords. Also monitors r/MilitarySpouse, r/USMilitarySO, and similar
communities where behavioral signals often appear earliest.

Keyword matching is intentionally broad -- we're building a corpus for
baseline, not trying to be precise on individual posts. The anomaly
detector handles separating signal from noise.
"""

import logging
import httpx
from datetime import date, datetime, timezone
from collectors.base import BaseCollector
from config.settings import settings
from models import Installation, Observation, TextObservation, CollectorResult

logger = logging.getLogger("new.collectors.reddit")

# Global subreddits to monitor (not installation-specific)
GLOBAL_SUBREDDITS = [
    "MilitarySpouse",
    "USMilitarySO",
    "army",
    "USMC",
    "navy",
    "AirForce",
    "Military",
    "veterans",
]

# Keyword categories with weights for relevance scoring
KEYWORD_SETS = {
    "deployment": {
        "keywords": [
            "deploying", "deployment", "deployed", "getting deployed",
            "orders", "movement orders", "PCS orders",
            "shipping out", "heading out", "leaving soon",
            "tour", "rotation",
        ],
        "weight": 1.0,
    },
    "relocation": {
        "keywords": [
            "moving", "relocating", "PCS", "DITY move",
            "breaking lease", "selling house fast", "need to sell",
            "storage unit", "U-Haul", "moving truck",
            "power of attorney",  # families prep this before deployments
        ],
        "weight": 0.8,
    },
    "urgency": {
        "keywords": [
            "short notice", "last minute", "sudden", "unexpected",
            "quick sale", "must sell", "asap", "emergency",
            "accelerated", "rushed",
        ],
        "weight": 0.6,
    },
    "preparation": {
        "keywords": [
            "FRG", "family readiness", "rear detachment",
            "pre-deployment", "deployment checklist",
            "will", "power of attorney", "life insurance",
            "saying goodbye",
        ],
        "weight": 0.7,
    },
}


class RedditCollector(BaseCollector):
    source_name = "reddit"

    def __init__(self):
        super().__init__()
        self.client = httpx.Client(
            headers={
                "User-Agent": "NEW:v0.1 (OSINT research tool)",
            },
            follow_redirects=True,
            timeout=30.0,
        )
        # If we have API credentials, use the authenticated API
        self._authenticated = bool(
            settings.reddit_client_id and settings.reddit_client_secret
        )
        if self._authenticated:
            self._auth_token = self._authenticate()

    def _authenticate(self) -> str:
        """Get OAuth token for Reddit API."""
        resp = self.client.post(
            "https://www.reddit.com/api/v1/access_token",
            data={"grant_type": "client_credentials"},
            auth=(settings.reddit_client_id, settings.reddit_client_secret),
        )
        return resp.json().get("access_token", "")

    def collect(self, installation: Installation) -> CollectorResult:
        """Collect from installation-specific and global subreddits."""
        observations: list[Observation] = []
        text_observations: list[TextObservation] = []
        errors = 0
        subreddits = installation.subreddits

        # Collect from installation-specific subs
        for sub in subreddits:
            sub_name = sub.replace("r/", "")
            obs, text_obs, errs = self._collect_subreddit(sub_name, installation)
            observations.extend(obs)
            text_observations.extend(text_obs)
            errors += errs

        # Collect from global military subs with installation keyword filtering
        for sub in GLOBAL_SUBREDDITS:
            obs, text_obs, errs = self._collect_subreddit(
                sub, installation, require_installation_mention=True
            )
            observations.extend(obs)
            text_observations.extend(text_obs)
            errors += errs

        return CollectorResult(
            observations=observations,
            text_observations=text_observations,
            errors=errors,
        )

    def _collect_subreddit(
        self,
        subreddit: str,
        installation: Installation,
        require_installation_mention: bool = False,
    ) -> tuple[list[Observation], list[TextObservation], int]:
        """Fetch recent posts from a subreddit and score them."""
        observations: list[Observation] = []
        text_observations: list[TextObservation] = []
        errors = 0

        try:
            if self._authenticated:
                url = f"https://oauth.reddit.com/r/{subreddit}/new"
                headers = {"Authorization": f"Bearer {self._auth_token}"}
            else:
                url = f"https://www.reddit.com/r/{subreddit}/new.json"
                headers = {}

            resp = self.client.get(
                url,
                params={"limit": 50, "sort": "new"},
                headers=headers,
            )

            if resp.status_code != 200:
                logger.warning(f"Reddit returned {resp.status_code} for r/{subreddit}")
                return observations, text_observations, 1

            data = resp.json()
            posts = data.get("data", {}).get("children", [])

            for post in posts:
                post_data = post.get("data", {})
                title = post_data.get("title", "")
                selftext = post_data.get("selftext", "")
                full_text = f"{title} {selftext}".lower()

                # If we need installation mention in global subs
                if require_installation_mention:
                    install_names = [
                        installation.name.lower(),
                        installation.nearby_city.split(",")[0].lower(),
                        installation.id.replace("_", " "),
                    ]
                    if not any(name in full_text for name in install_names if name):
                        continue

                # Score against keyword sets
                score = self._score_relevance(full_text)
                if score <= 0:
                    continue

                created_utc = post_data.get("created_utc", 0)
                post_date = datetime.fromtimestamp(created_utc, tz=timezone.utc).date()
                permalink = f"https://reddit.com{post_data.get('permalink', '')}"

                text_observations.append(TextObservation(
                    installation_id=installation.id,
                    source=self.source_name,
                    observed_date=post_date,
                    title=title[:500],
                    body=selftext[:2000] if selftext else None,
                    url=permalink,
                    relevance_score=score,
                    metadata={
                        "subreddit": subreddit,
                        "author": post_data.get("author", "[deleted]"),
                        "score": post_data.get("score", 0),
                        "num_comments": post_data.get("num_comments", 0),
                        "matched_categories": self._matched_categories(full_text),
                    },
                ))

                # Also store as numeric observation for trend detection
                observations.append(Observation(
                    installation_id=installation.id,
                    source=self.source_name,
                    observed_date=post_date,
                    metric_name="deployment_mention_score",
                    metric_value=score,
                    metadata={"subreddit": subreddit, "post_id": post_data.get("id")},
                ))

        except Exception as e:
            logger.warning(f"Failed r/{subreddit}: {e}")
            errors += 1

        return observations, text_observations, errors

    def _score_relevance(self, text: str) -> float:
        """Score text against keyword sets. Returns 0.0 if no matches."""
        total = 0.0
        for category, config in KEYWORD_SETS.items():
            matches = sum(1 for kw in config["keywords"] if kw.lower() in text)
            if matches:
                total += matches * config["weight"]
        return total

    def _matched_categories(self, text: str) -> list[str]:
        """Return which keyword categories matched."""
        matched = []
        for category, config in KEYWORD_SETS.items():
            if any(kw.lower() in text for kw in config["keywords"]):
                matched.append(category)
        return matched


def main():
    import sys
    logging.basicConfig(level=settings.log_level)
    collector = RedditCollector()
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    collector.run(installation_ids=targets)


if __name__ == "__main__":
    main()
