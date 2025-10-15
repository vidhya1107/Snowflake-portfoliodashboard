import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def get_portfolio_benchmark_data(portfolio_id, start_dt, end_dt):
    """Fetch portfolio vs benchmark comparison data"""
    try:
        query = f"""
        SELECT
            v.data_dt AS Date,
            v.portfolio_id,
            d.benchmark_desc AS Benchmark,
            100 * v.net_asset_value_amt / FIRST_VALUE(v.net_asset_value_amt) 
                OVER (PARTITION BY v.portfolio_id ORDER BY v.data_dt) AS Portfolio_NAV_Index,
            100 * b.benchmarknav / FIRST_VALUE(b.benchmarknav) 
                OVER (PARTITION BY v.portfolio_id ORDER BY v.data_dt) AS Benchmark_Index
        FROM
            contoso_daily_valuation_fact v
        INNER JOIN
            portfolio_dim d ON v.portfolio_id = d.portfolio_id
        INNER JOIN
            benchmark_timeseries b 
            ON d.benchmark_desc = b.benchmarkname 
           AND v.data_dt = b.date
        WHERE
           v.portfolio_id = {portfolio_id}
           AND v.data_dt BETWEEN '{start_dt}' AND '{end_dt}'
           AND v.net_asset_value_amt IS NOT NULL
           AND b.benchmarknav IS NOT NULL
        ORDER BY v.data_dt;
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error fetching portfolio benchmark data: {str(e)}")
        return pd.DataFrame()

def display_portfolio_benchmark_data(selected_portfolio, start_date, end_date):
    df = get_portfolio_benchmark_data(selected_portfolio, start_date, end_date)
    st.session_state["df"] = df  
    
    st.subheader("📊 Portfolio vs Benchmark Performance")
    if not df.empty:
        col1, col2, col3 = st.columns(3)
        
        latest_portfolio = df["PORTFOLIO_NAV_INDEX"].iloc[-1]
        latest_benchmark = df["BENCHMARK_INDEX"].iloc[-1]
        outperformance = latest_portfolio - latest_benchmark
        
        with col1:
            st.metric("Portfolio Index", f"{latest_portfolio:.2f}", f"{latest_portfolio - 100:.2f}%")
        with col2:
            st.metric("Benchmark Index", f"{latest_benchmark:.2f}", f"{latest_benchmark - 100:.2f}%")
        with col3:
            st.metric("Outperformance", f"{outperformance:+.2f}%", 
                     delta_color="normal" if outperformance >= 0 else "inverse")
        
        # Line chart
        chart_data = df.set_index("DATE")[["PORTFOLIO_NAV_INDEX", "BENCHMARK_INDEX"]]
        st.line_chart(chart_data)
    
    else:
        st.warning("⚠️ No benchmark comparison data found for this selection.")

    return df