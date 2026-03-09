import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

if "filter_start" not in st.session_state:
    st.session_state.filter_start = (datetime.now().date() - timedelta(days=30))
if "filter_end" not in st.session_state:
    st.session_state.filter_end = datetime.now().date()

@st.cache_resource
def get_connection():
    conn = st.connection("snowflake")
    try:
        conn.session().sql("ALTER SESSION SET QUERY_TAG = 'USAGE_ANALYZER_APP'").collect()
    except:
        pass
    return conn

def execute_query(query):
    conn = get_connection()
    return conn.query(query)

st.title(":material/analytics: Snowflake Usage Analyzer")

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
    
    @st.cache_data(ttl=600)
    def get_warehouses():
        try:
            df = execute_query("""
                SELECT DISTINCT warehouse_name 
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
                WHERE warehouse_name IS NOT NULL 
                ORDER BY warehouse_name
                LIMIT 100
            """)
            return ["All"] + df['WAREHOUSE_NAME'].tolist()
        except:
            return ["All"]
    
    @st.cache_data(ttl=600)
    def get_users():
        try:
            df = execute_query("""
                SELECT DISTINCT user_name 
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
                WHERE user_name IS NOT NULL 
                ORDER BY user_name
                LIMIT 100
            """)
            return ["All"] + df['USER_NAME'].tolist()
        except:
            return ["All"]
    
    warehouses = get_warehouses()
    selected_warehouse = st.selectbox("Warehouse", warehouses, index=0)
    st.session_state.filter_warehouse = None if selected_warehouse == "All" else selected_warehouse
    
    users = get_users()
    selected_user = st.selectbox("User", users, index=0)
    st.session_state.filter_user = None if selected_user == "All" else selected_user
    
    st.divider()
    st.caption(":material/info: Data has up to 45 min latency")

start = st.session_state.filter_start
end = st.session_state.filter_end

def build_where_clause():
    clauses = [f"start_time >= '{start}'", f"start_time <= '{end}'"]
    if st.session_state.get("filter_warehouse"):
        clauses.append(f"warehouse_name = '{st.session_state.filter_warehouse}'")
    if st.session_state.get("filter_user"):
        clauses.append(f"user_name = '{st.session_state.filter_user}'")
    return " AND ".join(clauses)

@st.cache_data(ttl=300)
def load_summary_stats(start, end, warehouse_filter, user_filter):
    where = f"start_time >= '{start}' AND start_time <= '{end}'"
    if warehouse_filter:
        where += f" AND warehouse_name = '{warehouse_filter}'"
    if user_filter:
        where += f" AND user_name = '{user_filter}'"
    
    return execute_query(f"""
        SELECT 
            COUNT(*) as total_queries,
            COUNT(DISTINCT user_name) as unique_users,
            COUNT(DISTINCT warehouse_name) as unique_warehouses,
            COUNT(DISTINCT database_name) as unique_databases,
            SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_queries,
            SUM(CASE WHEN execution_status = 'FAIL' THEN 1 ELSE 0 END) as failed_queries,
            SUM(bytes_spilled_to_local_storage) as total_local_spill,
            SUM(bytes_spilled_to_remote_storage) as total_remote_spill,
            AVG(percentage_scanned_from_cache) as avg_cache_hit,
            SUM(credits_used_cloud_services) as total_cloud_credits,
            SUM(total_elapsed_time)/1000/60/60 as total_hours
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
    """)

try:
    stats = load_summary_stats(
        start, end, 
        st.session_state.get("filter_warehouse"),
        st.session_state.get("filter_user")
    )
    
    if stats.empty or stats['TOTAL_QUERIES'].iloc[0] == 0:
        st.warning("No query data found for the selected filters. Try expanding the date range.", icon=":material/warning:")
    else:
        st.subheader("Overview")
        
        row1 = st.columns(4)
        row1[0].metric("Total Queries", f"{stats['TOTAL_QUERIES'].iloc[0]:,}")
        row1[1].metric("Unique Users", stats['UNIQUE_USERS'].iloc[0])
        row1[2].metric("Warehouses", stats['UNIQUE_WAREHOUSES'].iloc[0])
        row1[3].metric("Databases", stats['UNIQUE_DATABASES'].iloc[0])
        
        row2 = st.columns(4)
        total_q = stats['TOTAL_QUERIES'].iloc[0] or 1
        success_rate = (stats['SUCCESSFUL_QUERIES'].iloc[0] or 0) * 100 / total_q
        row2[0].metric("Success Rate", f"{success_rate:.1f}%")
        
        cache_hit = stats['AVG_CACHE_HIT'].iloc[0]
        row2[1].metric("Avg Cache Hit", f"{(cache_hit or 0)*100:.1f}%")
        
        total_hours = stats['TOTAL_HOURS'].iloc[0] or 0
        row2[2].metric("Total Query Hours", f"{total_hours:,.1f}")
        
        cloud_credits = stats['TOTAL_CLOUD_CREDITS'].iloc[0] or 0
        row2[3].metric("Cloud Services Credits", f"{cloud_credits:,.2f}")
        
        local_spill = (stats['TOTAL_LOCAL_SPILL'].iloc[0] or 0) / 1e9
        remote_spill = (stats['TOTAL_REMOTE_SPILL'].iloc[0] or 0) / 1e9
        if local_spill > 10 or remote_spill > 1:
            st.warning(f"Spilling detected: {local_spill:.1f} GB local, {remote_spill:.1f} GB remote. Check Warehouse Analysis for details.", icon=":material/warning:")
        
        st.divider()
        st.caption("Navigate to detailed analysis pages using the sidebar menu.")

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure you have IMPORTED PRIVILEGES on the SNOWFLAKE database. See setup.sql for details.", icon=":material/info:")

st.divider()

st.markdown("### What this app provides")
col1, col2 = st.columns(2)
with col1:
    with st.container(border=True):
        st.markdown(":material/query_stats: **Query Performance**")
        st.caption("Pruning efficiency, spilling analysis, cache utilization")
    with st.container(border=True):
        st.markdown(":material/dns: **Warehouse Analysis**")
        st.caption("Sizing recommendations, queue times, utilization")
    with st.container(border=True):
        st.markdown(":material/group: **User & Role Patterns**")
        st.caption("Who's running what, role distribution, heavy users")
with col2:
    with st.container(border=True):
        st.markdown(":material/error: **Error Analysis**")
        st.caption("Failed queries, common errors, retry patterns")
    with st.container(border=True):
        st.markdown(":material/smart_toy: **Cortex AI Usage**")
        st.caption("LLM functions, Search, Analyst, Agents, Intelligence")
    with st.container(border=True):
        st.markdown(":material/lightbulb: **AI Recommendations**")
        st.caption("Cortex-powered optimization suggestions on every page")
