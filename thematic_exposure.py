import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data
def get_thematic_data(portfolio_id):
    """
    Fetch thematic/ESG exposure data for a given portfolio.
    Returns a DataFrame with theme, nav_amt, and returns.
    """
    try:
        query = f"""
        SELECT
            d.investment_theme AS theme,
            SUM(v.net_asset_value_amt) AS nav_amt,
            AVG(v.net_investment_income_amt / NULLIF(v.net_asset_value_amt,0)) * 100 AS return_pct
        FROM contoso_daily_valuation_fact v
        JOIN portfolio_dim_extra d
        ON v.portfolio_id = d.portfolio_id
        GROUP BY theme
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching thematic data: {str(e)}")
        return pd.DataFrame()


def display_thematic_exposure(portfolio_id):
    """
    Display Thematic/ESG exposure panel:
    - Bar chart: % weight by theme
    - Bubble chart: Return vs % weight
    """
    df_them = get_thematic_data(portfolio_id)
    st.session_state["df_them"] = df_them  

    if df_them.empty:
        st.warning("⚠️ No thematic/ESG data found for this portfolio.")
        return

    st.subheader("🌱 Thematic / ESG Exposure")

    # Calculate % weight
    df_them["weight_pct"] = 100 * df_them["NAV_AMT"] / df_them["NAV_AMT"].sum()

    # Bar chart for % weight
    fig1, ax1 = plt.subplots(figsize=(8, 5))
    sns.barplot(data=df_them, x="THEME", y="weight_pct", palette="Set2", ax=ax1)
    ax1.set_ylabel("% Weight")
    ax1.set_xlabel("Theme")
    ax1.set_title("Portfolio Weight by Investment Theme")
    st.pyplot(fig1)

    # Bubble chart: Return vs Weight
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    scatter = ax2.scatter(
        df_them["weight_pct"],
        df_them["RETURN_PCT"],
        s=df_them["NAV_AMT"] / 1000,  # scale bubble size
        alpha=0.6,
        c=range(len(df_them)),
        cmap="viridis"
    )
    for i, row in df_them.iterrows():
        ax2.text(row["weight_pct"], row["RETURN_PCT"], row["THEME"], fontsize=9, ha='center', va='bottom')
    ax2.set_xlabel("% Weight")
    ax2.set_ylabel("Return (%)")
    ax2.set_title("Return vs Portfolio Weight by Theme")
    st.pyplot(fig2)

    return df_them