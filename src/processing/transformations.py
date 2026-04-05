"""
Data transformations.

All functions are pure: they take a DataFrame in and return a new DataFrame.
This makes them easy to test and reason about independently of I/O.

Transformations implemented:
  1. year-over-year growth rates
  2. COVID-19 impact analysis  (2020 vs 2018–2019 baseline + recovery check)
  3. Country ranking & min-max normalisation per indicator per year
"""

import pandas as pd


# ------------------------------------------------------------------
# 1. Year-over-year growth rates
# ------------------------------------------------------------------

def compute_growth_rates(df: pd.DataFrame) -> pd.DataFrame:
    """For each (country, indicator) pair, compute the percentage change
    from one year to the next.

    Assumption: `time_period` can be cast to int and is annual.
    Records with no prior year get NaN and are dropped.

    Returns a new DataFrame with columns:
        ref_area, ref_area_name, indicator_id, indicator_name,
        time_period, obs_value, prev_value, growth_rate_pct
    """
    df = df.copy()
    df["time_period"] = df["time_period"].astype(int)
    df = df.sort_values(["ref_area", "indicator_id", "time_period"])

    df["prev_value"] = df.groupby(["ref_area", "indicator_id"])["obs_value"].shift(1)

    df["growth_rate_pct"] = (
        (df["obs_value"] - df["prev_value"]) / df["prev_value"] * 100
    )

    result = df.dropna(subset=["growth_rate_pct"]).reset_index(drop=True)

    return result[
        [
            "ref_area", "ref_area_name", "indicator_id", "indicator_name",
            "time_period", "obs_value", "prev_value", "growth_rate_pct",
        ]
    ]


# ------------------------------------------------------------------
# 2. COVID-19 impact analysis
# ------------------------------------------------------------------

def analyse_covid_impact(
    df: pd.DataFrame,
    baseline_years: tuple[int, int] = (2018, 2019),
    shock_year: int = 2020,
    recovery_year: int = 2023,
) -> pd.DataFrame:
    """Compare each country-indicator's shock-year value against a
    pre-COVID baseline (mean of baseline_years).

    Also checks whether the latest year has recovered past the baseline.

    Returns a DataFrame with columns:
        ref_area, ref_area_name, indicator_id, indicator_name,
        baseline_avg, shock_value, shock_change_pct,
        recovery_value, recovered (bool)
    """
    df = df.copy()
    df["time_period"] = df["time_period"].astype(int)

    # Baseline average
    baseline = (
        df[df["time_period"].isin(baseline_years)]
        .groupby(["ref_area", "ref_area_name", "indicator_id", "indicator_name"])["obs_value"]
        .mean()
        .rename("baseline_avg")
        .reset_index()
    )

    # Shock year value
    shock = (
        df[df["time_period"] == shock_year]
        [["ref_area", "indicator_id", "obs_value"]]
        .rename(columns={"obs_value": "shock_value"})
    )

    # Recovery year value
    recovery = (
        df[df["time_period"] == recovery_year]
        [["ref_area", "indicator_id", "obs_value"]]
        .rename(columns={"obs_value": "recovery_value"})
    )

    result = baseline.merge(shock, on=["ref_area", "indicator_id"], how="left")
    result = result.merge(recovery, on=["ref_area", "indicator_id"], how="left")

    result["shock_change_pct"] = (
        (result["shock_value"] - result["baseline_avg"]) / result["baseline_avg"] * 100
    )

    # "Recovered" means the recovery-year value has returned to or exceeded the
    # baseline. For unemployment (where lower = better), we invert the check.
    result["recovered"] = result["recovery_value"] >= result["baseline_avg"]

    # For unemployment-type indicators (unit is %), lower is better
    unemployment_mask = result["indicator_id"] == "SL.UEM.TOTL.ZS"
    result.loc[unemployment_mask, "recovered"] = (
        result.loc[unemployment_mask, "recovery_value"]
        <= result.loc[unemployment_mask, "baseline_avg"]
    )

    return result


# ------------------------------------------------------------------
# 3. Country ranking & normalisation
# ------------------------------------------------------------------

def rank_and_normalise(df: pd.DataFrame) -> pd.DataFrame:
    """For each (indicator, year), rank countries and apply min-max
    normalisation to produce a 0–100 score.

    Returns a DataFrame with added columns:
        rank, normalised_score
    """
    df = df.copy()
    df["time_period"] = df["time_period"].astype(int)

    # --- Ranking ---
    # For unemployment, lower = better, so we rank ascending.
    # For everything else, higher = better, so we rank descending.
    unemployment_mask = df["indicator_id"] == "SL.UEM.TOTL.ZS"

    # Default: rank descending (highest value = rank 1)
    df["rank"] = df.groupby(["indicator_id", "time_period"])["obs_value"].rank(
        ascending=False, method="min"
    ).astype(int)

    # Override for unemployment: rank ascending (lowest value = rank 1)
    if unemployment_mask.any():
        unemp_ranks = df.loc[unemployment_mask].groupby(
            ["indicator_id", "time_period"]
        )["obs_value"].rank(ascending=True, method="min").astype(int)
        df.loc[unemployment_mask, "rank"] = unemp_ranks

    # --- Min-max normalisation ---
    group_min = df.groupby(["indicator_id", "time_period"])["obs_value"].transform("min")
    group_max = df.groupby(["indicator_id", "time_period"])["obs_value"].transform("max")

    range_values = group_max - group_min
    df["normalised_score"] = ((df["obs_value"] - group_min) / range_values * 100).where(
        range_values != 0, other=50.0  # all values identical → 50
    )

    return df[
        [
            "ref_area", "ref_area_name", "indicator_id", "indicator_name",
            "time_period", "obs_value", "rank", "normalised_score",
        ]
    ].reset_index(drop=True) 