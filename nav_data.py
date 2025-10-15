import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data
def get_nav_data(portfolio_id, start_dt, end_dt):
    """Fetch NAV values"""
    try:
        query = f"""
        SELECT v.portfolio_id, v.data_dt as nav_dt, v.net_asset_value_amt
        FROM contoso_daily_valuation_fact v
        WHERE v.portfolio_id = {portfolio_id}
          AND v.data_dt BETWEEN '{start_dt}' AND '{end_dt}'
          AND v.net_asset_value_amt IS NOT NULL
        ORDER BY v.data_dt;
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching NAV data: {str(e)}")
        return pd.DataFrame()

def display_nav_data(portfolio_id,start_dt,end_dt):
    df_nav = get_nav_data(portfolio_id, start_dt, end_dt)
    st.session_state["df_nav"] = df_nav  
    
    st.subheader("💰 Portfolio NAV Trend")
    if not df_nav.empty:
        # NAV metrics
        col1, col2, col3 = st.columns(3)
        
        current_nav = df_nav["NET_ASSET_VALUE_AMT"].iloc[-1]
        start_nav = df_nav["NET_ASSET_VALUE_AMT"].iloc[0]
        nav_change = ((current_nav - start_nav) / start_nav) * 100
        
        with col1:
            st.metric("Current NAV", f"${current_nav:,.2f}")
        with col2:
            st.metric("Starting NAV", f"${start_nav:,.2f}")
        with col3:
            st.metric("Total Return", f"{nav_change:+.2f}%",
                     delta_color="normal" if nav_change >= 0 else "inverse")
        
        # Area chart
        st.area_chart(df_nav.set_index("NAV_DT")["NET_ASSET_VALUE_AMT"])
    
    else:
        st.warning("⚠️ No NAV data found for this selection.")

    return df_nav

def get_nav_summary_for_chat(df_nav):
    """Generate a concise summary of NAV data for chat context"""
    if df_nav is None or df_nav.empty:
        return "No NAV data available"
    
    current_nav = df_nav["NET_ASSET_VALUE_AMT"].iloc[-1]
    start_nav = df_nav["NET_ASSET_VALUE_AMT"].iloc[0]
    nav_change = ((current_nav - start_nav) / start_nav) * 100
    
    summary = f"""
    Portfolio NAV Summary:
    - Current NAV: ${current_nav:,.2f}
    - Starting NAV: ${start_nav:,.2f}
    - Total Return: {nav_change:+.2f}%
    - Data Points: {len(df_nav)} days
    - Period: {df_nav['NAV_DT'].iloc[0]} to {df_nav['NAV_DT'].iloc[-1]}
    """
    
    if "DAILY_RETURN_PCT" in df_nav.columns:
        daily_returns = df_nav["DAILY_RETURN_PCT"].dropna()
        if len(daily_returns) > 0:
            summary += f"""
    - Best Day: {daily_returns.max():+.3f}%
    - Worst Day: {daily_returns.min():+.3f}%
    - Average Daily Return: {daily_returns.mean():+.3f}%
    - Volatility: {daily_returns.std():.3f}%
    """
    
    return summary
        