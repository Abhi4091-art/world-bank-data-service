"""
WellMatch Data Service – Entry point.

Usage:
    python main.py                  # fetch from API, transform, save
    python main.py --sample         # use local sample data (offline mode)
    python main.py --output-dir out # custom output directory
"""

import argparse
import logging
import sys

import pandas as pd

from config.settings import Settings
from src.ingestion.client import Data360Client, Data360ClientError
from src.processing.transformations import (
    compute_growth_rates,
    analyse_covid_impact,
    rank_and_normalise,
)
from src.output.writer import write_results


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch World Bank indicators, transform, and export results."
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use local sample data instead of calling the API.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write output files (default: from config).",
    )
    return parser.parse_args()


def main() -> int:
    setup_logging()
    logger = logging.getLogger("main")

    args = parse_args()
    settings = Settings.from_env()

    # CLI flags override env/config
    if args.sample:
        settings = Settings(
            **{**settings.__dict__, "use_sample_data": True}
        )
    if args.output_dir:
        settings = Settings(
            **{**settings.__dict__, "output_dir": args.output_dir}
        )

    # ---- 1. Ingest ----
    logger.info("Starting data ingestion …")
    client = Data360Client(settings)

    try:
        raw_records = client.fetch_indicators()
    except (Data360ClientError, FileNotFoundError) as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1

    if not raw_records:
        logger.error("No records returned – nothing to process.")
        return 1

    df = pd.DataFrame(raw_records)
    logger.info("Ingested %d records across %d indicators",
                len(df), df["indicator_id"].nunique())

    # ---- 2. Transform ----
    logger.info("Running transformations …")

    growth_rates = compute_growth_rates(df)
    logger.info("  Growth rates:    %d rows", len(growth_rates))

    covid_impact = analyse_covid_impact(df)
    logger.info("  COVID impact:    %d rows", len(covid_impact))

    rankings = rank_and_normalise(df)
    logger.info("  Rankings:        %d rows", len(rankings))

    # ---- 3. Output ----
    logger.info("Writing results …")
    written = write_results(
        output_dir=settings.output_dir,
        growth_rates=growth_rates,
        covid_impact=covid_impact,
        rankings=rankings,
    )

    logger.info("Done – %d files written to %s/", len(written), settings.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())