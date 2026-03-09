import streamlit as st
from datetime import datetime, timedelta
from data import load_summary_stats, get_warehouses, get_users

st.set_page_config(page_title="Snowflake Usage Analyzer", page_icon=":material/analytics:", layout="wide")

st.session_state.setdefault("filter_start", datetime.now().date() - timedelta(days=30))
st.session_state.setdefault("filter_end", datetime.now().date())
st.session_state.setdefault("filter_warehouse", None)
st.session_state.setdefault("filter_user", None)

with st.sidebar:
    st.subheader(":material/calendar_today: Date Range")

    date_option = st.radio(
        "Select period",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Last 365 days", "Custom"],
        index=1,
        horizontal=False
    )

    if date_option == "Last 7 days":
        st.session_state.filter_start = datetime.now().date() - timedelta(days=7)
        st.session_state.filter_end = datetime.now().date()
    elif date_option == "Last 30 days":
        st.session_state.filter_start = datetime.now().date() - timedelta(days=30)
        st.session_state.filter_end = datetime.now().date()
    elif date_option == "Last 90 days":
        st.session_state.filter_start = datetime.now().date() - timedelta(days=90)
        st.session_state.filter_end = datetime.now().date()
    elif date_option == "Last 365 days":
        st.session_state.filter_start = datetime.now().date() - timedelta(days=365)
        st.session_state.filter_end = datetime.now().date()
    else:
        st.session_state.filter_start = st.date_input(
            "Start Date",
            value=st.session_state.filter_start,
            key="start_input"
        )
        st.session_state.filter_end = st.date_input(
            "End Date",
            value=st.session_state.filter_end,
            key="end_input"
        )

    st.caption(f"Analyzing: **{st.session_state.filter_start}** to **{st.session_state.filter_end}**")

    st.divider()

    st.subheader(":material/filter_alt: Filters (Optional)")

    warehouses = get_warehouses()
    selected_warehouse = st.selectbox("Warehouse", warehouses, index=0)
    st.session_state.filter_warehouse = None if selected_warehouse == "All" else selected_warehouse

    users = get_users()
    selected_user = st.selectbox("User", users, index=0)
    st.session_state.filter_user = None if selected_user == "All" else selected_user

    st.divider()
    st.caption(":material/info: Data has up to 45 min latency")

try:
    summary = load_summary_stats(
        st.session_state.filter_start,
        st.session_state.filter_end,
        st.session_state.filter_warehouse,
        st.session_state.filter_user,
    )
    st.session_state["summary_stats"] = summary
except Exception as e:
    st.session_state["summary_stats"] = None

page = st.navigation([
    st.Page("app_pages/home.py", title="Overview", icon=":material/analytics:"),
    st.Page("app_pages/query_performance.py", title="Query Performance", icon=":material/bolt:"),
    st.Page("app_pages/warehouse_analysis.py", title="Warehouse Analysis", icon=":material/warehouse:"),
    st.Page("app_pages/users_features.py", title="Users & Features", icon=":material/group:"),
    st.Page("app_pages/errors_recommendations.py", title="Errors & Recommendations", icon=":material/build:"),
    st.Page("app_pages/cortex_ai_usage.py", title="Cortex AI Usage", icon=":material/smart_toy:"),
])

page.run()
