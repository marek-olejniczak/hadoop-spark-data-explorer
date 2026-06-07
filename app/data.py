"""Data layer of the app: loads aggregated results from results/
"""
from pathlib import Path

import pandas as pd
import streamlit as st

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

# IATA codes of carriers present in the dataset
CARRIER_NAMES = {
    "AA": "American Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue Airways",
    "CO": "Continental Airlines (until 2011)",
    "DL": "Delta Air Lines",
    "EV": "ExpressJet",
    "F9": "Frontier Airlines",
    "FL": "AirTran Airways",
    "G4": "Allegiant Air",
    "HA": "Hawaiian Airlines",
    "MQ": "Envoy Air (American Eagle)",
    "NK": "Spirit Airlines",
    "NW": "Northwest Airlines (until 2010)",
    "OH": "PSA Airlines",
    "OO": "SkyWest Airlines",
    "UA": "United Airlines",
    "US": "US Airways (until 2015)",
    "VX": "Virgin America (until 2018)",
    "WN": "Southwest Airlines",
    "XE": "ExpressJet (Continental)",
    "YV": "Mesa Airlines",
    "YX": "Republic Airways",
    "9E": "Endeavor Air",
}

CANCELLATION_CODES = {
    "A": "Carrier",
    "B": "Weather",
    "C": "NAS (air traffic control)",
    "D": "Security",
}

# Spark's dayofweek(): 1 = Sunday
DAY_OF_WEEK = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

MONTHS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

DELAY_CAUSE_LABELS = {
    "sum_carrier_delay": "Carrier",
    "sum_weather_delay": "Weather",
    "sum_nas_delay": "NAS (air traffic control)",
    "sum_security_delay": "Security",
    "sum_late_aircraft_delay": "Late aircraft",
}


@st.cache_data
def load(name: str) -> pd.DataFrame:
    """Load one aggregated result (cached)"""
    return pd.read_parquet(RESULTS_DIR / f"{name}.parquet")


def carrier_label(code: str) -> str:
    return f"{code} - {CARRIER_NAMES.get(code, 'other carrier')}"


def weighted_avg(df: pd.DataFrame, avg_col: str, weight_col: str) -> float:
    """Correctly combine partial averages (weighted mean)
    """
    weights = df[weight_col].fillna(0)
    if weights.sum() == 0:
        return float("nan")
    return (df[avg_col].fillna(0) * weights).sum() / weights.sum()


def aggregate_years(
    df: pd.DataFrame, group_cols: list, year_range: tuple
) -> pd.DataFrame:
    """Filter to a year range and merge years: sum counts, weight averages."""
    mask = (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    df = df[mask]

    sum_cols = [c for c in df.columns if c.startswith(("n_", "sum_"))]
    avg_cols = [c for c in df.columns if c.startswith("avg_")]

    def _agg(group: pd.DataFrame) -> pd.Series:
        out = {c: group[c].sum() for c in sum_cols}
        weight = (
            "n_with_delay_data" if "n_with_delay_data" in group else "n_flights"
        )
        for c in avg_cols:
            out[c] = weighted_avg(group, c, weight)
        return pd.Series(out)

    return df.groupby(group_cols).apply(_agg).reset_index()
