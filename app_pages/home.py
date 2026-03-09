import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta
from data import get_revenue_trend, get_ai_adoption_summary, get_platform_adoption_summary
from utils import create_query_history_view, discover_customer_views

customer = st.session_state.get("customer_name", "Unknown")
sfdc_id = st.session_state.get("salesforce_account_id")

st.title(f":material/analytics: {customer}")

if sfdc_id:
    st.subheader("Consumption Trend")
    revenue = get_revenue_trend(sfdc_id)
    if revenue is not None and not revenue.empty:
        col1, col2, col3 = st.columns(3)
        latest = revenue.iloc[-1]
        col1.metric("Total Credits (last month)", f"{latest['TOTAL_CREDITS']:,.0f}")
        col2.metric("Compute Credits", f"{latest['COMPUTE_CREDITS']:,.0f}")
        col3.metric("AI Services Credits", f"{latest['AI_SERVICES_CREDITS']:,.0f}")

        fig = px.bar(
            revenue, x='MONTH', y=['COMPUTE_CREDITS', 'AI_SERVICES_CREDITS'],
            title="Monthly Credit Consumption", barmode='stack'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No consumption data available.", icon=":material/info:")

stats = st.session_state.get("summary_stats")
if stats is not None and not stats.empty and stats['TOTAL_QUERIES'].iloc[0] > 0:
    st.divider()
    st.subheader("Query Summary")

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
elif not st.session_state.get("query_history_view"):
    st.divider()
    st.subheader(":material/build: Create Query History View")

    selected_accounts = st.session_state.get("selected_accounts")
    if selected_accounts is not None and not selected_accounts.empty:
        st.markdown("No query history view exists for this customer. Create one to enable performance analysis pages.")
        st.caption("The view will query `SNOWHOUSE_IMPORT.<DEPLOYMENT>.JOB_ETL_V` for the selected accounts.")

        with st.expander("Selected Snowflake accounts", expanded=False):
            st.dataframe(
                selected_accounts[['SNOWFLAKE_ACCOUNT_NAME', 'DEPLOYMENT', 'TOTAL_CREDITS_L90D']],
                use_container_width=True, hide_index=True
            )

        view_col1, view_col2 = st.columns(2)
        view_start = view_col1.date_input(
            "View start date",
            value=datetime.now().date() - timedelta(days=365),
            key="view_create_start"
        )
        view_end = view_col2.date_input(
            "View end date",
            value=datetime.now().date(),
            key="view_create_end"
        )

        safe_name = customer.replace(" ", "_").upper()
        view_name = st.text_input("View name", value=f"{safe_name}_QUERY_HISTORY_V", key="view_name_input")

        if st.button(":material/add: Create View", type="primary"):
            with st.spinner("Creating view... this may take a moment."):
                success, error = create_query_history_view(
                    view_name, selected_accounts, view_start, view_end
                )
            if success:
                st.success(f"View `TEMP.EDENDULK.{view_name}` created successfully!", icon=":material/check_circle:")
                views = discover_customer_views(customer)
                st.session_state.available_views = views
                st.session_state.query_history_view = views.get('QUERY_HISTORY_V')
                st.rerun()
            else:
                st.error(f"Failed to create view: {error}", icon=":material/error:")
    else:
        st.info("No Snowflake account data available. Search for the customer via SFDC to enable view creation.", icon=":material/info:")

if sfdc_id:
    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader(":material/smart_toy: AI/ML Adoption")
        ai_df = get_ai_adoption_summary(sfdc_id)
        if ai_df is not None and not ai_df.empty:
            for _, row in ai_df.iterrows():
                icon = ":material/check_circle:" if row['IS_USING'] == 'Yes' else ":material/cancel:"
                credits = f"{row['TOTAL_CREDITS']:,.2f}" if row['TOTAL_CREDITS'] > 0 else "—"
                st.markdown(f"{icon} **{row['FEATURE']}** — {credits} credits (L30D)")
        else:
            st.info("No AI/ML adoption data.", icon=":material/info:")

    with col2:
        st.subheader(":material/widgets: Platform Features")
        platform_df = get_platform_adoption_summary(sfdc_id)
        if platform_df is not None and not platform_df.empty:
            for _, row in platform_df.iterrows():
                icon = ":material/check_circle:" if row['IS_USING'] == 'Yes' else ":material/cancel:"
                credits = f"{row['TOTAL_CREDITS']:,.2f}" if row['TOTAL_CREDITS'] > 0 else "—"
                st.markdown(f"{icon} **{row['FEATURE']}** — {credits} credits (L30D)")
        else:
            st.info("No platform adoption data.", icon=":material/info:")

sf_accounts = st.session_state.get("snowflake_accounts")
if sf_accounts is not None and not sf_accounts.empty:
    st.divider()
    st.subheader(":material/cloud: Snowflake Accounts")
    st.dataframe(sf_accounts, use_container_width=True)

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
