# allocation_exposure.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data
def get_allocation_data(portfolio_id):
    """
    Fetch asset allocation and exposure data for a given portfolio.
    Returns a DataFrame with asset_class, sector, region, nav_amt.
    """
    try:
        query = f"""
        SELECT
            d.investment_type AS asset_class,
            d.fund_focus AS sector,
            v.account_region_cd AS region,
            SUM(v.net_asset_value_amt) AS nav_amt
        FROM contoso_daily_valuation_fact v
        JOIN portfolio_dim_extra d
        ON v.portfolio_id = d.portfolio_id
        GROUP BY asset_class, sector, region
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching allocation data: {str(e)}")
        return pd.DataFrame()


def display_allocation(portfolio_id):
    """
    Display asset allocation breakdown for a portfolio:
    - Pie chart for asset class distribution
    - Bar chart for drilling down by sector or region
    """
    df_allocation = get_allocation_data(portfolio_id)
    st.session_state["df_allocation"] = df_allocation  

    if df_allocation.empty:
        st.warning("⚠️ No allocation data found for this portfolio.")
        return

    st.subheader("📊 Asset Allocation Breakdown")

    # Pie chart by asset class
    pie_data = df_allocation.groupby("ASSET_CLASS")["NAV_AMT"].sum().reset_index()
    fig1, ax1 = plt.subplots(figsize=(6, 6))
    ax1.pie(pie_data["NAV_AMT"], labels=pie_data["ASSET_CLASS"], autopct='%1.1f%%', startangle=140)
    ax1.set_title("Asset Allocation by Class")
    st.pyplot(fig1)

    # Bar chart drill-down
    drill_option = st.selectbox("Drill-down by:", ["Sector", "Region"])
    if drill_option == "Sector":
        bar_data = df_allocation.groupby(["ASSET_CLASS", "SECTOR"])["NAV_AMT"].sum().reset_index()
        fig2, ax2 = plt.subplots(figsize=(10, 6))
        sns.barplot(data=bar_data, x="ASSET_CLASS", y="NAV_AMT", hue="SECTOR", ax=ax2)
        ax2.set_ylabel("NAV Amount")
        ax2.set_title("Asset Allocation by Sector")
        st.pyplot(fig2)
    else:
        bar_data = df_allocation.groupby(["ASSET_CLASS", "REGION"])["NAV_AMT"].sum().reset_index()
        fig2, ax2 = plt.subplots(figsize=(10, 6))
        sns.barplot(data=bar_data, x="ASSET_CLASS", y="NAV_AMT", hue="REGION", ax=ax2)
        ax2.set_ylabel("NAV Amount")
        ax2.set_title("Asset Allocation by Region")
        st.pyplot(fig2)

    return df_allocation
