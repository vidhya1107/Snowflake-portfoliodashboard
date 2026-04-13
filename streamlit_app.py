# streamlit_app.py

import streamlit as st
from datetime import date

# Import all modules
import nav_data
import portfolio_benchmark
import risk_metrics
import attribution
import allocation_exposure
import thematic_exposure
import chat


# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(
    page_title="Portfolio Analytics Dashboard",
    layout="wide"
)

st.title("📈 Portfolio Analytics Dashboard")


# ---------------------------------------------------------
# SIDEBAR INPUTS
# ---------------------------------------------------------
st.sidebar.header("Portfolio Selection")

portfolio_id = st.sidebar.number_input(
    "Enter Portfolio ID",
    min_value=1,
    value=1,
    step=1
)

start_date = st.sidebar.date_input(
    "Start Date",
    value=date(2023, 1, 1)
)

end_date = st.sidebar.date_input(
    "End Date",
    value=date(2024, 12, 31)
)

start_dt = start_date.strftime("%Y-%m-%d")
end_dt = end_date.strftime("%Y-%m-%d")


# ---------------------------------------------------------
# 1️⃣ NAV Trend
# ---------------------------------------------------------
df_nav = nav_data.display_nav_data(portfolio_id, start_dt, end_dt)


# ---------------------------------------------------------
# 2️⃣ Portfolio vs Benchmark
# ---------------------------------------------------------
df_benchmark = portfolio_benchmark.display_portfolio_benchmark_data(
    portfolio_id,
    start_dt,
    end_dt
)


# ---------------------------------------------------------
# 3️⃣ Risk Metrics
# ---------------------------------------------------------
df_risk = risk_metrics.render_risk_metrics(portfolio_id)


# ---------------------------------------------------------
# 4️⃣ Attribution Analysis
# ---------------------------------------------------------
df_attr = attribution.display_attribution(portfolio_id)


# ---------------------------------------------------------
# 5️⃣ Allocation & Exposure
# ---------------------------------------------------------
df_allocation = allocation_exposure.display_allocation(portfolio_id)


# ---------------------------------------------------------
# 6️⃣ Thematic / ESG Exposure
# ---------------------------------------------------------
df_them = thematic_exposure.display_thematic_exposure(portfolio_id)


# ---------------------------------------------------------
# 7️⃣ Chatbot (Cortex)
# ---------------------------------------------------------
chat.chatbot_ui(
    context_keys=[
        "df_nav",
        "df_risk",
        "df_attr",
        "df_allocation",
        "df_them"
    ],
    model="mistral-7b"
)