import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data
def get_risk_metrics(portfolio_id):
    query = f"""
    WITH nav_with_returns AS (
        SELECT
            v.portfolio_id,
            v.nav_dt,
            v.net_asset_value_amt,
            LAG(v.net_asset_value_amt) OVER (
                PARTITION BY v.portfolio_id 
                ORDER BY v.nav_dt
            ) AS prev_nav,
            (v.net_asset_value_amt / NULLIF(LAG(v.net_asset_value_amt) OVER (
                PARTITION BY v.portfolio_id 
                ORDER BY v.nav_dt
            ), 0) - 1) AS daily_return
        FROM contoso_daily_valuation_fact v
        WHERE v.portfolio_id = {portfolio_id}
    ),
    risk_data AS (
        SELECT
            portfolio_id,
            nav_dt,
            STDDEV(daily_return) OVER (
                PARTITION BY portfolio_id
                ORDER BY nav_dt 
                ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
            ) AS volatility,
            AVG(daily_return) OVER (
                PARTITION BY portfolio_id
                ORDER BY nav_dt 
                ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
            ) / NULLIF(
                STDDEV(daily_return) OVER (
                    PARTITION BY portfolio_id
                    ORDER BY nav_dt 
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
                ), 0
            ) AS sharpe_ratio,
            (net_asset_value_amt - MAX(net_asset_value_amt) OVER (
                PARTITION BY portfolio_id 
                ORDER BY nav_dt 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) / NULLIF(
                MAX(net_asset_value_amt) OVER (
                    PARTITION BY portfolio_id 
                    ORDER BY nav_dt 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0
            ) AS drawdown
        FROM nav_with_returns
        WHERE daily_return IS NOT NULL
    )
    SELECT portfolio_id, nav_dt, volatility, sharpe_ratio, drawdown
    FROM risk_data
    ORDER BY nav_dt;
"""
    return session.sql(query).to_pandas()

def render_risk_metrics(portfolio_id, target_vol=0.15, target_sharpe=1.0, target_drawdown=-0.20):
    st.subheader("📉 Risk Metrics Trend")

    df_risk = get_risk_metrics(portfolio_id)
    st.session_state["df_risk"] = df_risk  

    if df_risk.empty:
        st.warning("⚠️ No risk data available for this portfolio.")
        return

    latest = df_risk.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Volatility", f"{latest['VOLATILITY']:.2%}")
    col2.metric("Latest Sharpe Ratio", f"{latest['SHARPE_RATIO']:.2f}")
    col3.metric("Latest Drawdown", f"{latest['DRAWDOWN']:.2%}")

    # Melt data
    df_melted = df_risk.melt(
        id_vars=["NAV_DT"],
        value_vars=["VOLATILITY", "SHARPE_RATIO", "DRAWDOWN"],
        var_name="Metric",
        value_name="Value"
    )

    # Separate line metrics and bar metric
    df_line = df_melted[df_melted["Metric"].isin(["VOLATILITY", "SHARPE_RATIO"])]
    df_bar = df_melted[df_melted["Metric"] == "DRAWDOWN"]

    # Line chart (Volatility + Sharpe)
    line_chart = (
        alt.Chart(df_line)
        .mark_line(point=True)
        .encode(
            x=alt.X("NAV_DT:T", title="Date"),
            y=alt.Y("Value:Q", title="Metric Value"),
            color="Metric:N",
            tooltip=["NAV_DT:T", "Metric:N", alt.Tooltip("Value:Q", format=".4f")]
        )
    )

    # Bar chart (Drawdown)
    bar_chart = (
        alt.Chart(df_bar)
        .mark_bar(opacity=0.5)
        .encode(
            x=alt.X("NAV_DT:T", title="Date"),
            y=alt.Y("Value:Q"),
            color=alt.value("#f94144"),  # red bars for drawdown
            tooltip=["NAV_DT:T", "Metric:N", alt.Tooltip("Value:Q", format=".4f")]
        )
    )

    # Threshold lines
    thresholds = [
        {"Metric": "VOLATILITY", "Threshold": target_vol},
        {"Metric": "SHARPE_RATIO", "Threshold": target_sharpe},
        {"Metric": "DRAWDOWN", "Threshold": target_drawdown},
    ]
    threshold_df = pd.DataFrame(thresholds)

    threshold_lines = (
        alt.Chart(threshold_df)
        .mark_rule(strokeDash=[4, 2], color="red")
        .encode(
            y="Threshold:Q",
            detail="Metric:N"
        )
    )

    final_chart = (line_chart + bar_chart + threshold_lines).interactive()
    st.altair_chart(final_chart, use_container_width=True)

    return df_risk
