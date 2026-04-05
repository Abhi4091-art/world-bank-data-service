"""
Output writer.

Writes processed DataFrames to the configured output directory as both
JSON (for programmatic consumers) and CSV (for human inspection / Excel).
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def write_results(
    output_dir: str,
    growth_rates: pd.DataFrame,
    covid_impact: pd.DataFrame,
    rankings: pd.DataFrame,
) -> list[Path]:
    """Write all result DataFrames to disk.

    Returns a list of file paths that were created.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    datasets = {
        "growth_rates": growth_rates,
        "covid_impact": covid_impact,
        "rankings": rankings,
    }

    for name, df in datasets.items():
        json_path = out / f"{name}.json"
        csv_path = out / f"{name}.csv"

        df.to_json(json_path, orient="records", indent=2, force_ascii=False)
        df.to_csv(csv_path, index=False)

        written.extend([json_path, csv_path])
        logger.info("Wrote %s  (%d rows)", json_path, len(df))
        logger.info("Wrote %s  (%d rows)", csv_path, len(df))

    return written