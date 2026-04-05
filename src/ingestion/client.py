"""
Data360 API client.

Abstracts all interaction with the World Bank Data360 API behind a single
`fetch_indicators` method.  Supports:
  - Configurable retries with exponential back-off
  - Timeout handling
  - Falling back to a local sample-data file for offline / CI use
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests

from config.settings import Settings

logger = logging.getLogger(__name__)

# Type alias – each record is a flat dict straight from the API.
RawRecord = dict[str, Any]


class Data360ClientError(Exception):
    """Raised when the API returns an unrecoverable error."""


class Data360Client:
    """Thin wrapper around the Data360 REST API."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def fetch_indicators(self) -> list[RawRecord]:
        """Fetch all configured indicators for the configured countries / years.

        Returns a flat list of observation records, normalised to lowercase keys
        with consistent field names for downstream processing.
        """
        if self._settings.use_sample_data:
            return self._load_sample_data()

        all_records: list[RawRecord] = []

        for indicator in self._settings.indicators:
            logger.info("Fetching indicator %s …", indicator)
            records = self._fetch_with_retry(indicator)
            normalised = [self._normalise_record(r) for r in records]
            # Filter to configured time periods (done client-side because
            # the API returns HTTP 417 with many TIME_PERIOD values)
            filtered = [
                r for r in normalised
                if r["time_period"] in self._settings.time_periods
            ]
            all_records.extend(filtered)
            logger.info("  → received %d records", len(normalised))

        logger.info("Total records fetched: %d", len(all_records))
        return all_records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_url(self, indicator: str) -> str:
        """Build the API URL with uppercase parameter names as required by Data360.
        
        Note: TIME_PERIOD filtering is done post-fetch because the API
        returns HTTP 417 when too many periods are requested at once.
        """
        params = {
            "DATABASE_ID": self._settings.database_id,
            "INDICATOR": indicator,
            "REF_AREA": ",".join(self._settings.countries),
        }
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self._settings.api_base_url}/data360/data?{query}"

    @staticmethod
    def _normalise_record(raw: dict) -> RawRecord:
        """Convert the API's uppercase field names to the lowercase format
        used by our processing layer, and cast OBS_VALUE to float."""
        return {
            "database_id": raw.get("DATABASE_ID", ""),
            "indicator_id": raw.get("INDICATOR", ""),
            "indicator_name": raw.get("COMMENT_TS", ""),
            "ref_area": raw.get("REF_AREA", ""),
            "ref_area_name": raw.get("REF_AREA", ""),  # API doesn't return full name
            "time_period": raw.get("TIME_PERIOD", ""),
            "obs_value": float(raw.get("OBS_VALUE", 0)),
            "unit_measure": raw.get("UNIT_MEASURE", ""),
            "freq": raw.get("FREQ", ""),
        }

    def _fetch_with_retry(self, indicator: str) -> list[dict]:
        """GET with exponential back-off. Extracts the 'value' list from
        the API's {count, value} response wrapper."""
        url = self._build_url(indicator)
        last_error: Exception | None = None

        for attempt in range(1, self._settings.api_max_retries + 1):
            try:
                resp = self._session.get(
                    url, timeout=self._settings.api_timeout_seconds
                )
                resp.raise_for_status()
                data = resp.json()

                # API returns {"count": N, "value": [...]}
                if isinstance(data, dict) and "value" in data:
                    return data["value"]
                elif isinstance(data, list):
                    return data
                else:
                    raise Data360ClientError(
                        f"Unexpected response structure: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
                    )

            except requests.exceptions.Timeout as exc:
                last_error = exc
                logger.warning(
                    "Timeout on attempt %d/%d for %s",
                    attempt, self._settings.api_max_retries, indicator,
                )
            except requests.exceptions.HTTPError as exc:
                last_error = exc
                logger.warning(
                    "HTTP %s on attempt %d/%d for %s",
                    exc.response.status_code, attempt,
                    self._settings.api_max_retries, indicator,
                )
            except (requests.exceptions.ConnectionError, json.JSONDecodeError) as exc:
                last_error = exc
                logger.warning(
                    "Request error on attempt %d/%d for %s: %s",
                    attempt, self._settings.api_max_retries, indicator, exc,
                )

            if attempt < self._settings.api_max_retries:
                wait = 2 ** attempt
                logger.info("Retrying in %ds …", wait)
                time.sleep(wait)

        raise Data360ClientError(
            f"Failed to fetch indicator {indicator} after "
            f"{self._settings.api_max_retries} attempts"
        ) from last_error

    def _load_sample_data(self) -> list[RawRecord]:
        """Load records from a local JSON file (for dev / testing)."""
        path = Path(self._settings.sample_data_path)
        if not path.exists():
            raise FileNotFoundError(f"Sample data file not found: {path}")

        logger.info("Loading sample data from %s", path)
        with open(path) as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise Data360ClientError("Sample data must be a JSON array")

        logger.info("Loaded %d sample records", len(data))
        return data