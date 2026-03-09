import streamlit as st

st.title(":material/analytics: Snowflake Usage Analyzer")

stats = st.session_state.get("summary_stats")

if stats is None:
    st.error("Error loading data.")
    st.info("Make sure you have IMPORTED PRIVILEGES on the SNOWFLAKE database. See setup.sql for details.", icon=":material/info:")
    st.stop()

if stats.empty or stats['TOTAL_QUERIES'].iloc[0] == 0:
    st.warning("No query data found for the selected filters. Try expanding the date range.", icon=":material/warning:")
    st.stop()

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
