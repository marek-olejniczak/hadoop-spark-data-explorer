"""Flight Delay Explorer - app for browsing the Hadoop/PySpark analysis results.

Run with:  streamlit run app/app.py
Requires: results/*.parquet (scripts/export_results.sh after computing aggregates)
"""
import plotly.express as px
import streamlit as st

from data import (
    CANCELLATION_CODES,
    DAY_OF_WEEK,
    DAY_ORDER,
    DELAY_CAUSE_LABELS,
    MONTHS,
    aggregate_years,
    carrier_label,
    load,
)

st.set_page_config(page_title="Flight Delay Explorer", layout="wide")

st.sidebar.title("Flight Delay Explorer")
st.sidebar.caption(
    "US domestic flights 2009-2018 (~65M flights), "
    "processed with Apache Hadoop + PySpark"
)

VIEWS = [
    "1. Airline delays",
    "2. Flight cancellations",
    "3. Airport taxi-out times",
    "4. Delay causes by airport",
    "5. Short vs long haul",
    "6. Busiest routes",
    "7. Carrier recommendation",
]
view = st.sidebar.radio("Choose an analysis", VIEWS)

year_range = st.sidebar.slider("Year range", 2009, 2018, (2009, 2018))


def years_label() -> str:
    return f"{year_range[0]}-{year_range[1]}"


# 1. Airline delays
if view == VIEWS[0]:
    st.header("Airline delays")
    df = load("carrier_delays")

    agg = aggregate_years(df, ["OP_CARRIER"], year_range)
    agg["pct_delayed"] = 100 * agg["n_delayed"] / agg["n_with_delay_data"]
    agg["carrier"] = agg["OP_CARRIER"].map(carrier_label)
    agg = agg.sort_values("avg_arr_delay", ascending=False)

    min_flights = st.slider("Min. flights per carrier", 0, 500_000, 50_000)
    agg = agg[agg["n_flights"] >= min_flights]

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            agg, x="avg_arr_delay", y="carrier", orientation="h",
            labels={"avg_arr_delay": "average arrival delay [min]",
                    "carrier": ""},
            title=f"Average arrival delay ({years_label()})",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(
            agg.sort_values("pct_delayed", ascending=False),
            x="pct_delayed", y="carrier", orientation="h",
            labels={"pct_delayed": "% of flights delayed >=15 min",
                    "carrier": ""},
            title=f"Share of delayed flights ({years_label()})",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Trend over time")
    carriers = st.multiselect(
        "Carriers", sorted(df["OP_CARRIER"].unique()),
        default=["AA", "DL", "UA", "WN"], format_func=carrier_label,
    )
    trend = df[df["OP_CARRIER"].isin(carriers)].copy()
    trend = trend[(trend["year"] >= year_range[0]) & (trend["year"] <= year_range[1])]
    fig = px.line(
        trend.sort_values("year"), x="year", y="avg_arr_delay",
        color="OP_CARRIER", markers=True,
        labels={"avg_arr_delay": "avg arrival delay [min]", "year": "year"},
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        agg[["carrier", "n_flights", "avg_dep_delay", "avg_arr_delay",
             "pct_delayed", "n_cancelled"]].round(2),
        use_container_width=True, hide_index=True,
    )

# 2. Flight cancellations
elif view == VIEWS[1]:
    st.header("Flight cancellations: when and why")
    df = load("cancellations_time")

    agg_m = aggregate_years(df, ["month"], year_range)
    agg_m["pct"] = 100 * agg_m["n_cancelled"] / agg_m["n_flights"]
    agg_m["month_name"] = agg_m["month"].map(MONTHS)

    agg_d = aggregate_years(df, ["day_of_week"], year_range)
    agg_d["pct"] = 100 * agg_d["n_cancelled"] / agg_d["n_flights"]
    agg_d["day"] = agg_d["day_of_week"].map(DAY_OF_WEEK)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            agg_m.sort_values("month"), x="month_name", y="pct",
            labels={"pct": "% cancelled", "month_name": ""},
            title=f"Cancellations by month ({years_label()})",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(
            agg_d.set_index("day").loc[DAY_ORDER].reset_index(),
            x="day", y="pct",
            labels={"pct": "% cancelled", "day": ""},
            title=f"Cancellations by day of week ({years_label()})",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Cancellation causes")
    causes = load("cancellation_causes")
    causes = causes[
        (causes["year"] >= year_range[0]) & (causes["year"] <= year_range[1])
    ]
    causes_agg = causes.groupby("CANCELLATION_CODE", as_index=False)[
        "n_cancelled"
    ].sum()
    causes_agg["cause"] = causes_agg["CANCELLATION_CODE"].map(
        CANCELLATION_CODES
    )
    fig = px.pie(
        causes_agg, names="cause", values="n_cancelled",
        title=f"Cancellation cause breakdown ({years_label()})",
    )
    st.plotly_chart(fig, use_container_width=True)

# 3. Taxi-out times
elif view == VIEWS[2]:
    st.header("Taxi-out time (waiting for take-off)")
    df = load("airport_taxi_out")

    agg = aggregate_years(df, ["ORIGIN"], year_range)
    min_flights = st.slider("Min. flights from airport", 0, 200_000, 20_000)
    agg = agg[agg["n_flights"] >= min_flights]

    top_n = st.slider("Show top N airports", 5, 40, 15)
    worst = agg.sort_values("avg_taxi_out", ascending=False).head(top_n)
    fig = px.bar(
        worst, x="avg_taxi_out", y="ORIGIN", orientation="h",
        labels={"avg_taxi_out": "avg taxi-out [min]", "ORIGIN": "airport"},
        title=f"Longest taxi-out times ({years_label()})",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        worst[["ORIGIN", "n_flights", "avg_taxi_out", "avg_taxi_in"]].round(2),
        use_container_width=True, hide_index=True,
    )

# 4. Delay causes by airport 
elif view == VIEWS[3]:
    st.header("Delay cause structure at an airport")
    df = load("airport_delay_causes")

    airports = sorted(df["ORIGIN"].unique())
    default_idx = airports.index("JFK") if "JFK" in airports else 0
    airport = st.selectbox("Airport (ORIGIN)", airports, index=default_idx)

    sel = df[df["ORIGIN"] == airport]
    agg = aggregate_years(sel, ["ORIGIN"], year_range)

    melted = agg.melt(
        value_vars=list(DELAY_CAUSE_LABELS.keys()),
        var_name="cause_col", value_name="minutes",
    )
    melted["cause"] = melted["cause_col"].map(DELAY_CAUSE_LABELS)
    fig = px.pie(
        melted, names="cause", values="minutes",
        title=f"{airport}: total delay minutes by cause ({years_label()})",
    )
    st.plotly_chart(fig, use_container_width=True)

    sel_years = sel[(sel["year"] >= year_range[0]) & (sel["year"] <= year_range[1])]
    melted_y = sel_years.melt(
        id_vars=["year"], value_vars=list(DELAY_CAUSE_LABELS.keys()),
        var_name="cause_col", value_name="minutes",
    )
    melted_y["cause"] = melted_y["cause_col"].map(DELAY_CAUSE_LABELS)
    fig = px.bar(
        melted_y, x="year", y="minutes", color="cause",
        labels={"minutes": "delay minutes", "year": "year"},
        title="Distribution over time",
    )
    st.plotly_chart(fig, use_container_width=True)

# 5. Short vs long haul
elif view == VIEWS[4]:
    st.header("Short vs long haul flights")
    df = load("distance_carriers")

    agg = aggregate_years(df, ["OP_CARRIER", "distance_bucket"], year_range)
    agg["carrier"] = agg["OP_CARRIER"].map(carrier_label)

    min_flights = st.slider("Min. flights per carrier (total)", 0, 500_000, 50_000)
    totals = agg.groupby("OP_CARRIER")["n_flights"].sum()
    keep = totals[totals >= min_flights].index
    agg = agg[agg["OP_CARRIER"].isin(keep)]

    fig = px.bar(
        agg, x="carrier", y="n_flights", color="distance_bucket",
        labels={"n_flights": "number of flights", "carrier": ""},
        title=f"Distance mix per carrier ({years_label()})",
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)

    fig = px.bar(
        agg, x="carrier", y="avg_arr_delay", color="distance_bucket",
        barmode="group",
        labels={"avg_arr_delay": "avg arrival delay [min]", "carrier": ""},
        title="Average delay by distance bucket",
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)

# 6. Busiest routes
elif view == VIEWS[5]:
    st.header("Busiest routes")
    df = load("routes")

    agg = aggregate_years(df, ["ORIGIN", "DEST"], year_range)
    agg["route"] = agg["ORIGIN"] + " -> " + agg["DEST"]
    agg["pct_delayed"] = 100 * agg["n_delayed"] / agg["n_flights"]

    origin_filter = st.text_input(
        "Origin airport filter (empty = all)", ""
    ).strip().upper()
    if origin_filter:
        agg = agg[agg["ORIGIN"] == origin_filter]

    top_n = st.slider("Top N routes", 5, 50, 20)
    top = agg.sort_values("n_flights", ascending=False).head(top_n)

    fig = px.bar(
        top, x="n_flights", y="route", orientation="h",
        color="pct_delayed", color_continuous_scale="RdYlGn_r",
        labels={"n_flights": "number of flights", "route": "",
                "pct_delayed": "% delayed"},
        title=f"Traffic volume ({years_label()})",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        top[["route", "n_flights", "avg_arr_delay", "pct_delayed",
             "n_cancelled", "avg_distance"]].round(2),
        use_container_width=True, hide_index=True,
    )

# 7. Carrier recommendation
elif view == VIEWS[6]:
    st.header("Which carrier on this route? (recommendation)")
    df = load("route_carrier_stats")

    origins = sorted(df["ORIGIN"].unique())
    default_o = origins.index("JFK") if "JFK" in origins else 0
    origin = st.selectbox("From (ORIGIN)", origins, index=default_o)

    dests = sorted(df[df["ORIGIN"] == origin]["DEST"].unique())
    default_d = dests.index("LAX") if "LAX" in dests else 0
    dest = st.selectbox("To (DEST)", dests, index=default_d)

    sel = df[(df["ORIGIN"] == origin) & (df["DEST"] == dest)]
    agg = aggregate_years(sel, ["OP_CARRIER"], year_range)

    min_flights = st.slider(
        "Min. flights per carrier on the route", 10, 1000, 50,
        help="Filters out carriers whose sample is too small to be reliable",
    )
    agg = agg[agg["n_flights"] >= min_flights]

    if agg.empty:
        st.warning(
            "No carriers meet the threshold. Lower the minimum flights "
            "or widen the year range."
        )
    else:
        st.markdown(
            "**Risk score** = w_d x P(delay >=15 min) + w_c x "
            "P(cancellation) + w_a x (avg delay / 30 min). Lower = better."
        )
        col1, col2, col3 = st.columns(3)
        w_delay = col1.slider("weight: delays", 0.0, 2.0, 1.0, 0.1)
        w_cancel = col2.slider("weight: cancellations", 0.0, 4.0, 2.0, 0.1)
        w_avg = col3.slider("weight: avg delay", 0.0, 2.0, 0.5, 0.1)

        agg["p_delayed"] = agg["n_delayed"] / agg["n_with_delay_data"]
        agg["p_cancelled"] = agg["n_cancelled"] / agg["n_flights"]
        agg["risk_score"] = (
            w_delay * agg["p_delayed"]
            + w_cancel * agg["p_cancelled"]
            + w_avg * agg["avg_arr_delay"].clip(lower=0) / 30
        )
        agg["carrier"] = agg["OP_CARRIER"].map(carrier_label)
        agg = agg.sort_values("risk_score")

        best = agg.iloc[0]
        st.success(
            f"**Recommendation for {origin} -> {dest} ({years_label()}):** "
            f"{best['carrier']} - "
            f"{100 * best['p_delayed']:.1f}% of flights delayed, "
            f"{100 * best['p_cancelled']:.2f}% cancelled, "
            f"avg delay {best['avg_arr_delay']:.1f} min "
            f"({int(best['n_flights'])} flights in sample)"
        )

        show = agg[["carrier", "n_flights", "p_delayed", "p_cancelled",
                    "avg_arr_delay", "risk_score"]].copy()
        show["p_delayed"] = (100 * show["p_delayed"]).round(1)
        show["p_cancelled"] = (100 * show["p_cancelled"]).round(2)
        show = show.rename(columns={
            "p_delayed": "% delayed", "p_cancelled": "% cancelled",
            "avg_arr_delay": "avg delay [min]",
        })
        st.dataframe(
            show.round(3), use_container_width=True, hide_index=True
        )

        fig = px.bar(
            agg, x="carrier", y="risk_score",
            labels={"risk_score": "risk score (lower = better)", "carrier": ""},
            title="Carrier comparison on the route",
        )
        st.plotly_chart(fig, use_container_width=True)
