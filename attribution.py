# attribution.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data
def get_attribution_data(portfolio_id):
    """Fetch attribution analysis data for a portfolio"""
    try:
        query = f"""
        WITH nav_by_portfolio AS (
    SELECT
        portfolio_id,
        account_region_cd AS region,
        SUM(net_asset_value_amt) AS nav_amt
    FROM contoso_daily_valuation_fact
    GROUP BY portfolio_id, account_region_cd
),
combo_count AS (
    SELECT portfolio_id, COUNT(*) AS num_combos
    FROM portfolio_dim_extra
    GROUP BY portfolio_id
),
portfolio_combinations AS (
    SELECT
        d.portfolio_id,
        d.investment_type AS asset_class,
        d.fund_focus AS sector,
        d.investment_theme AS theme,
        n.region,
        n.nav_amt / c.num_combos AS nav_contribution
    FROM portfolio_dim_extra d
    JOIN nav_by_portfolio n
      ON d.portfolio_id = n.portfolio_id
    JOIN combo_count c
      ON d.portfolio_id = c.portfolio_id
)
SELECT
    portfolio_id,
    asset_class,
    sector,
    region,
    theme,
    nav_contribution,
    ROUND(100 * nav_contribution / NULLIF(SUM(nav_contribution) OVER (PARTITION BY portfolio_id),0), 2) AS contribution_pct
FROM portfolio_combinations
ORDER BY portfolio_id, contribution_pct DESC;

        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching attribution data: {str(e)}")
        return pd.DataFrame()


def display_attribution(portfolio_id):
    """Display attribution analysis and heatmap in Streamlit"""
    df_attr = get_attribution_data(portfolio_id)
    st.session_state["df_attr"] = df_attr  

    st.subheader("🔎 Attribution Analysis")

    if not df_attr.empty:
        # Highest & Worst contribution
        summary_df = df_attr.groupby("PORTFOLIO_ID").agg(
            highest_contribution=("CONTRIBUTION_PCT", "max"),
            worst_contribution=("CONTRIBUTION_PCT", "min")
        ).reset_index()
        col1, col2 = st.columns(2)
        
        portfolio_summary = summary_df[summary_df["PORTFOLIO_ID"] == portfolio_id].iloc[0]
        with col1:
            st.metric("Highest Contribution", f"{portfolio_summary['highest_contribution']:.2f}%")
        with col2:
            st.metric("Worst Contribution", f"{portfolio_summary['worst_contribution']:.2f}%")

        # Heatmap of sector vs region
        st.markdown("### Sector vs Region")
        sector_region = df_attr.groupby(["REGION", "SECTOR"])["CONTRIBUTION_PCT"].sum().reset_index()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(data=sector_region, x="REGION", y="CONTRIBUTION_PCT", hue="SECTOR", ax=ax)
        ax.set_ylabel("Contribution %")
        st.pyplot(fig)


    else:
        st.warning("⚠️ No attribution data found for this portfolio.")

    return df_attr
