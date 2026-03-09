import streamlit as st
from datetime import datetime, timedelta
from utils import (
    execute_query, resolve_customer, discover_customer_views,
    get_snowflake_accounts_for_sfdc, create_query_history_view
)
from data import load_summary_stats, get_warehouses, get_users

st.set_page_config(
    page_title="Customer Usage Analyzer",
    page_icon=":material/analytics:",
    layout="wide"
)

st.session_state.setdefault("customer_name", None)
st.session_state.setdefault("salesforce_account_id", None)
st.session_state.setdefault("snowflake_accounts", None)
st.session_state.setdefault("selected_accounts", None)
st.session_state.setdefault("query_history_view", None)
st.session_state.setdefault("available_views", {})
st.session_state.setdefault("accounts_confirmed", False)
st.session_state.setdefault("filter_start", datetime.now().date() - timedelta(days=30))
st.session_state.setdefault("filter_end", datetime.now().date())
st.session_state.setdefault("filter_warehouse", None)
st.session_state.setdefault("filter_user", None)


def reset_customer():
    for key in ['customer_name', 'salesforce_account_id', 'snowflake_accounts',
                 'selected_accounts', 'query_history_view', 'available_views',
                 'summary_stats']:
        st.session_state[key] = None
    st.session_state.accounts_confirmed = False
    st.session_state.filter_warehouse = None
    st.session_state.filter_user = None


if not st.session_state.customer_name:
    st.title(":material/analytics: Customer Usage Analyzer")
    st.markdown("Analyze a Snowflake customer's query performance, warehouse utilization, AI/ML adoption, and get optimization recommendations.")
    st.divider()

    st.subheader(":material/search: Select a Customer")

    search_term = st.text_input(
        "Customer name (SFDC account name)",
        placeholder="e.g. Acme Corp, aviv, salomon...",
        key="customer_search"
    )

    if search_term and len(search_term) >= 2:
        with st.spinner("Searching..."):
            matches = resolve_customer(search_term)

        if matches is not None and not matches.empty:
            st.markdown(f"**{len(matches)} match(es) found:**")

            for idx, row in matches.iterrows():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                col1.write(f"**{row['NAME']}**")
                col2.write(row.get('INDUSTRY', ''))
                col3.write(row.get('TYPE', ''))
                if col4.button("Select", key=f"select_{idx}"):
                    st.session_state.customer_name = row['NAME']
                    st.session_state.salesforce_account_id = row['SALESFORCE_ACCOUNT_ID']
                    st.session_state.accounts_confirmed = False
                    sf_accounts = get_snowflake_accounts_for_sfdc(row['SALESFORCE_ACCOUNT_ID'])
                    st.session_state.snowflake_accounts = sf_accounts
                    views = discover_customer_views(row['NAME'])
                    st.session_state.available_views = views
                    st.session_state.query_history_view = views.get('QUERY_HISTORY_V')
                    st.rerun()
        else:
            st.warning("No matching customers found. Try a different search term.", icon=":material/search_off:")

    st.divider()
    existing_views = execute_query("""
        SELECT DISTINCT REPLACE(TABLE_NAME, '_QUERY_HISTORY_V', '') as CUSTOMER
        FROM TEMP.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'EDENDULK' AND TABLE_NAME LIKE '%_QUERY_HISTORY_V'
        ORDER BY CUSTOMER
    """)
    if existing_views is not None and not existing_views.empty:
        st.subheader(":material/view_list: Customers with Query History Views")
        st.caption("These customers have pre-built query history views for detailed performance analysis.")
        cols = st.columns(4)
        for i, name in enumerate(existing_views['CUSTOMER'].tolist()):
            with cols[i % 4]:
                if st.button(name, key=f"quick_{name}", use_container_width=True):
                    matches = resolve_customer(name)
                    if matches is not None and not matches.empty:
                        row = matches.iloc[0]
                        st.session_state.customer_name = row['NAME']
                        st.session_state.salesforce_account_id = row['SALESFORCE_ACCOUNT_ID']
                        sf_accounts = get_snowflake_accounts_for_sfdc(row['SALESFORCE_ACCOUNT_ID'])
                        st.session_state.snowflake_accounts = sf_accounts
                    else:
                        st.session_state.customer_name = name
                        st.session_state.salesforce_account_id = None
                        st.session_state.snowflake_accounts = None

                    views = discover_customer_views(name)
                    st.session_state.available_views = views
                    st.session_state.query_history_view = views.get('QUERY_HISTORY_V')
                    st.session_state.accounts_confirmed = True
                    st.rerun()

    st.stop()

sf_accounts = st.session_state.snowflake_accounts
has_multiple_accounts = sf_accounts is not None and not sf_accounts.empty and len(sf_accounts) > 1
needs_account_step = has_multiple_accounts and not st.session_state.accounts_confirmed and not st.session_state.query_history_view

if needs_account_step:
    st.title(f":material/dns: Snowflake Accounts for {st.session_state.customer_name}")
    st.markdown("This customer has multiple Snowflake accounts. Select which ones to include in the analysis.")

    if st.button(":material/arrow_back: Back to search"):
        reset_customer()
        st.rerun()

    st.divider()

    account_selections = {}
    for idx, row in sf_accounts.iterrows():
        col1, col2, col3, col4 = st.columns([1, 3, 2, 2])
        checked = col1.checkbox(
            "Include",
            value=True,
            key=f"acct_{idx}",
            label_visibility="collapsed"
        )
        account_selections[idx] = checked
        col2.write(f"**{row['SNOWFLAKE_ACCOUNT_NAME']}**")
        col3.write(row['DEPLOYMENT'])
        credits = row.get('TOTAL_CREDITS_L90D', 0) or 0
        col4.write(f"{credits:,.0f} credits (L90D)")

    st.divider()

    selected_indices = [idx for idx, checked in account_selections.items() if checked]
    selected_df = sf_accounts.loc[selected_indices] if selected_indices else None

    if not selected_indices:
        st.warning("Select at least one account to continue.", icon=":material/warning:")
    else:
        st.info(f"**{len(selected_indices)}** of **{len(sf_accounts)}** accounts selected", icon=":material/check:")

        if st.button(":material/arrow_forward: Continue with selected accounts", type="primary", use_container_width=True):
            st.session_state.selected_accounts = selected_df
            st.session_state.accounts_confirmed = True
            st.rerun()

    st.stop()

if not st.session_state.accounts_confirmed:
    if sf_accounts is not None and not sf_accounts.empty:
        st.session_state.selected_accounts = sf_accounts
    st.session_state.accounts_confirmed = True

with st.sidebar:
    st.subheader(f":material/person: {st.session_state.customer_name}")
    if st.session_state.salesforce_account_id:
        st.caption(f"SFDC: `{st.session_state.salesforce_account_id[:15]}...`")

    selected = st.session_state.selected_accounts
    if selected is not None and not selected.empty:
        acct_names = selected['SNOWFLAKE_ACCOUNT_NAME'].tolist()
        if len(acct_names) <= 3:
            st.caption(f"Accounts: {', '.join(acct_names)}")
        else:
            st.caption(f"Accounts: {', '.join(acct_names[:3])} +{len(acct_names)-3} more")

    views = st.session_state.available_views
    if views.get('QUERY_HISTORY_V'):
        st.success("Query history view available", icon=":material/check_circle:")
    else:
        st.warning("No query history view", icon=":material/warning:")

    if st.button(":material/swap_horiz: Change Customer", use_container_width=True):
        reset_customer()
        st.rerun()

    st.divider()
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

    if st.session_state.query_history_view:
        st.divider()
        st.subheader(":material/filter_alt: Filters (Optional)")

        warehouses = get_warehouses(
            st.session_state.filter_start,
            st.session_state.filter_end,
            st.session_state.query_history_view
        )
        selected_warehouse = st.selectbox("Warehouse", warehouses, index=0)
        st.session_state.filter_warehouse = None if selected_warehouse == "All" else selected_warehouse

        users = get_users(
            st.session_state.filter_start,
            st.session_state.filter_end,
            st.session_state.query_history_view
        )
        selected_user = st.selectbox("User", users, index=0)
        st.session_state.filter_user = None if selected_user == "All" else selected_user

    st.divider()
    st.caption(":material/info: Data has up to 45 min latency")

if st.session_state.query_history_view:
    try:
        summary = load_summary_stats(
            st.session_state.filter_start,
            st.session_state.filter_end,
            st.session_state.filter_warehouse,
            st.session_state.filter_user,
            st.session_state.query_history_view,
        )
        st.session_state["summary_stats"] = summary
    except Exception as e:
        st.session_state["summary_stats"] = None
else:
    st.session_state["summary_stats"] = None

pages = [
    st.Page("app_pages/home.py", title="Overview", icon=":material/analytics:"),
]

if st.session_state.query_history_view:
    pages.extend([
        st.Page("app_pages/query_performance.py", title="Query Performance", icon=":material/bolt:"),
        st.Page("app_pages/warehouse_analysis.py", title="Warehouse Analysis", icon=":material/warehouse:"),
        st.Page("app_pages/users_features.py", title="Users & Features", icon=":material/group:"),
        st.Page("app_pages/errors_recommendations.py", title="Errors & Recommendations", icon=":material/build:"),
    ])

if st.session_state.salesforce_account_id:
    pages.append(
        st.Page("app_pages/cortex_ai_usage.py", title="Cortex AI Usage", icon=":material/smart_toy:")
    )

page = st.navigation(pages)
page.run()
