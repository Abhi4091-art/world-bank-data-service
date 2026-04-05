"""
Tests for src.processing.transformations.

Each test uses small, hand-crafted DataFrames so the expected results
can be verified by mental arithmetic.
"""

import pandas as pd
import pytest

from src.processing.transformations import (
    compute_growth_rates,
    analyse_covid_impact,
    rank_and_normalise,
)


# ---- helpers ----

def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from a list of dicts with sensible defaults."""
    defaults = {
        "database_id": "WB_WDI",
        "unit_measure": "Number",
        "freq": "A",
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---- compute_growth_rates ----

class TestGrowthRates:

    def test_basic_growth(self):
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Indicator 1", "time_period": "2020", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Indicator 1", "time_period": "2021", "obs_value": 110},
        ])
        result = compute_growth_rates(df)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["growth_rate_pct"] == pytest.approx(10.0)
        assert row["prev_value"] == 100
        assert row["obs_value"] == 110

    def test_negative_growth(self):
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Indicator 1", "time_period": "2020", "obs_value": 200},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Indicator 1", "time_period": "2021", "obs_value": 180},
        ])
        result = compute_growth_rates(df)

        assert result.iloc[0]["growth_rate_pct"] == pytest.approx(-10.0)

    def test_multiple_countries_independent(self):
        """Growth rates should be computed independently per country."""
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2021", "obs_value": 120},
            {"ref_area": "BBB", "ref_area_name": "Bland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 50},
            {"ref_area": "BBB", "ref_area_name": "Bland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2021", "obs_value": 60},
        ])
        result = compute_growth_rates(df)

        assert len(result) == 2
        aaa = result[result["ref_area"] == "AAA"].iloc[0]
        bbb = result[result["ref_area"] == "BBB"].iloc[0]
        assert aaa["growth_rate_pct"] == pytest.approx(20.0)
        assert bbb["growth_rate_pct"] == pytest.approx(20.0)

    def test_first_year_excluded(self):
        """The first year has no prior value so should not appear in output."""
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2019", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 105},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2021", "obs_value": 110},
        ])
        result = compute_growth_rates(df)

        assert len(result) == 2
        assert result["time_period"].tolist() == [2020, 2021]


# ---- analyse_covid_impact ----

class TestCovidImpact:

    def test_basic_impact(self):
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2018", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2019", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 90},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2023", "obs_value": 105},
        ])
        result = analyse_covid_impact(df)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["baseline_avg"] == pytest.approx(100.0)
        assert row["shock_value"] == pytest.approx(90.0)
        assert row["shock_change_pct"] == pytest.approx(-10.0)
        assert row["recovered"] == True  # 105 > 100

    def test_not_recovered(self):
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2018", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2019", "obs_value": 100},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 80},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2023", "obs_value": 95},
        ])
        result = analyse_covid_impact(df)
        assert result.iloc[0]["recovered"] == False  # 95 < 100

    def test_unemployment_recovery_inverted(self):
        """For unemployment, 'recovered' means the value went DOWN (lower = better)."""
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "WB_WDI_SL_UEM_TOTL_ZS",
             "indicator_name": "Unemp", "time_period": "2018", "obs_value": 5.0},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "WB_WDI_SL_UEM_TOTL_ZS",
             "indicator_name": "Unemp", "time_period": "2019", "obs_value": 5.0},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "WB_WDI_SL_UEM_TOTL_ZS",
             "indicator_name": "Unemp", "time_period": "2020", "obs_value": 8.0},
            {"ref_area": "AAA", "ref_area_name": "Aland", "indicator_id": "WB_WDI_SL_UEM_TOTL_ZS",
             "indicator_name": "Unemp", "time_period": "2023", "obs_value": 4.0},
        ])
        result = analyse_covid_impact(df)
        assert result.iloc[0]["recovered"] == True  # 4.0 <= 5.0 baseline


# ---- rank_and_normalise ----

class TestRankAndNormalise:

    def test_ranking_order(self):
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "A", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 300},
            {"ref_area": "BBB", "ref_area_name": "B", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 100},
            {"ref_area": "CCC", "ref_area_name": "C", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 200},
        ])
        result = rank_and_normalise(df)

        aaa = result[result["ref_area"] == "AAA"].iloc[0]
        bbb = result[result["ref_area"] == "BBB"].iloc[0]
        ccc = result[result["ref_area"] == "CCC"].iloc[0]

        # Highest value = rank 1
        assert aaa["rank"] == 1
        assert ccc["rank"] == 2
        assert bbb["rank"] == 3

    def test_normalisation_range(self):
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "A", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 0},
            {"ref_area": "BBB", "ref_area_name": "B", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 50},
            {"ref_area": "CCC", "ref_area_name": "C", "indicator_id": "IND1",
             "indicator_name": "Ind", "time_period": "2020", "obs_value": 100},
        ])
        result = rank_and_normalise(df)

        scores = result.set_index("ref_area")["normalised_score"]
        assert scores["AAA"] == pytest.approx(0.0)
        assert scores["BBB"] == pytest.approx(50.0)
        assert scores["CCC"] == pytest.approx(100.0)

    def test_unemployment_ranking_inverted(self):
        """For unemployment, lower value = rank 1 (better)."""
        df = _make_df([
            {"ref_area": "AAA", "ref_area_name": "A", "indicator_id": "WB_WDI_SL_UEM_TOTL_ZS",
             "indicator_name": "Unemp", "time_period": "2020", "obs_value": 2.0},
            {"ref_area": "BBB", "ref_area_name": "B", "indicator_id": "WB_WDI_SL_UEM_TOTL_ZS",
             "indicator_name": "Unemp", "time_period": "2020", "obs_value": 8.0},
        ])
        result = rank_and_normalise(df)

        aaa = result[result["ref_area"] == "AAA"].iloc[0]
        bbb = result[result["ref_area"] == "BBB"].iloc[0]

        assert aaa["rank"] == 1  # lower unemployment = better
        assert bbb["rank"] == 2