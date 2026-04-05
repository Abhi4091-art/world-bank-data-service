"""
Application configuration.

Loads settings from environment variables with sensible defaults.
Uses a dataclass for type safety and easy overriding in tests.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Immutable application settings."""

    # API
    api_base_url: str = "https://data360api.worldbank.org"
    api_timeout_seconds: int = 30
    api_max_retries: int = 3

    # Data query defaults
    database_id: str = "WB_WDI"
    indicators: list[str] = field(default_factory=lambda: [
        "WB_WDI_SP_POP_TOTL",
        "WB_WDI_NY_GDP_PCAP_CD",
        "WB_WDI_SL_UEM_TOTL_ZS",
    ])
    countries: list[str] = field(default_factory=lambda: [
        "GBR", "USA", "DEU", "FRA", "JPN",
    ])
    time_periods: list[str] = field(default_factory=lambda: [
        "2018", "2019", "2020", "2021", "2022", "2023",
    ])

    # Indicators where a lower value is better (e.g. unemployment).
    # Used by transformations to invert ranking and recovery logic.
    lower_is_better_indicators: list[str] = field(default_factory=lambda: [
        "WB_WDI_SL_UEM_TOTL_ZS",
    ])
    # Paths
    output_dir: str = "output"
    sample_data_path: str = "data/samples/sample_response.json"

    # When True, load from local sample file instead of hitting the API.
    # Useful for development, CI, and offline work.
    use_sample_data: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from environment variables, falling back to defaults."""
        defaults = cls()  # instantiate with all defaults
        return cls(
            api_base_url=os.getenv("DATA360_API_BASE_URL", defaults.api_base_url),
            api_timeout_seconds=int(os.getenv("DATA360_API_TIMEOUT", str(defaults.api_timeout_seconds))),
            api_max_retries=int(os.getenv("DATA360_API_MAX_RETRIES", str(defaults.api_max_retries))),
            database_id=os.getenv("DATA360_DATABASE_ID", defaults.database_id),
            indicators=os.getenv("DATA360_INDICATORS", ";".join(defaults.indicators)).split(";"),
            countries=os.getenv("DATA360_COUNTRIES", ";".join(defaults.countries)).split(";"),
            time_periods=os.getenv("DATA360_TIME_PERIODS", ";".join(defaults.time_periods)).split(";"),
            output_dir=os.getenv("DATA360_OUTPUT_DIR", defaults.output_dir),
            sample_data_path=os.getenv("DATA360_SAMPLE_DATA_PATH", defaults.sample_data_path),
            use_sample_data=os.getenv("DATA360_USE_SAMPLE_DATA", "false").lower() == "true",
            lower_is_better_indicators=os.getenv(
                "DATA360_LOWER_IS_BETTER",
                ";".join(defaults.lower_is_better_indicators)
            ).split(";"),
        )