import streamlit as st
import pandas as pd
import plotly.express as px
from utils import execute_query, get_ai_suggestions, build_where_clause, get_query_source, to_pandas_native
from data import get_error_summary, get_error_codes, get_long_queries, generate_recommendations

where_clause = build_where_clause()
source = get_query_source()

st.title(":material/build: Errors & Optimization Recommendations")

selected = st.segmented_control(
    "Analysis",
    [":material/error: Error Analysis", ":material/timer: Long Running Queries",
     ":material/lightbulb: Optimization Summary"],
    default=":material/error: Error Analysis",
)

if selected == ":material/error: Error Analysis":
    st.subheader("Failed Query Analysis")

    error_summary = get_error_summary(where_clause, source)

    if not error_summary.empty:
        col1, col2, col3, col4 = st.columns(4)
        total = error_summary['TOTAL_QUERIES'].iloc[0]
        failed = error_summary['FAILED'].iloc[0]
        col1.metric("Total Queries", f"{total:,}")
        col2.metric("Successful", f"{error_summary['SUCCESS'].iloc[0]:,}")
        col3.metric("Failed", f"{failed:,}")
        col4.metric("Failure Rate", f"{(failed*100/(total or 1)):.2f}%")

    error_codes = get_error_codes(where_clause, source)

    if not error_codes.empty:
        st.markdown("**Top error codes**")
        fig = px.bar(error_codes.head(15), x='ERROR_CODE', y='ERROR_COUNT',
                    title="Most Common Error Codes")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(error_codes, use_container_width=True)
    else:
        st.success("No failed queries in this period!")

    st.markdown("**Error trends**")
    error_trend = execute_query(f"""
        SELECT 
            DATE_TRUNC('day', start_time) as day,
            execution_status,
            COUNT(*) as query_count
        FROM {source}
        WHERE {where_clause}
        GROUP BY day, execution_status
        ORDER BY day
    """)

    if not error_trend.empty:
        fig = px.area(error_trend, x='DAY', y='QUERY_COUNT', color='EXECUTION_STATUS',
                     title="Daily Query Status Distribution")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Failed queries by user**")
    user_errors = execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as failed_queries,
            COUNT(DISTINCT error_code) as unique_errors
        FROM {source}
        WHERE {where_clause}
        AND execution_status = 'FAIL'
        GROUP BY user_name
        ORDER BY failed_queries DESC
        LIMIT 20
    """)

    if not user_errors.empty:
        st.dataframe(user_errors, use_container_width=True)

        if st.button(":material/psychology: Get AI error analysis", key="error_ai"):
            with st.spinner("Analyzing error patterns..."):
                error_stats = error_codes.to_string() if not error_codes.empty else "No error codes"
                user_error_stats = user_errors.to_string()

                prompt = """You are a Snowflake troubleshooting expert analyzing query history data.
Analyze these error patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Root cause analysis for common error codes
2. User training recommendations based on error patterns
3. Query patterns that commonly fail and how to prevent them
4. Permission/role issues causing failures
5. Resource contention issues"""

                data_context = f"ERROR CODE DISTRIBUTION:\n{error_stats}\n\nERRORS BY USER:\n{user_error_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

elif selected == ":material/timer: Long Running Queries":
    st.subheader("Long Running Queries")

    threshold_min = st.slider("Execution time threshold (minutes)", 1, 60, 5)

    long_queries = get_long_queries(where_clause, threshold_min * 60 * 1000, source)

    if not long_queries.empty:
        st.info(f"Found {len(long_queries)} queries running longer than {threshold_min} minutes", icon=":material/info:")

        col1, col2 = st.columns(2)
        with col1:
            fig = px.box(long_queries, x='WAREHOUSE_SIZE', y='EXEC_SEC',
                        title="Execution Time by Warehouse Size")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(long_queries, x='GB_SCANNED', y='EXEC_SEC',
                           color='WAREHOUSE_SIZE', hover_name='QUERY_ID',
                           title="Data Scanned vs Execution Time")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(long_queries.drop(columns=['QUERY_TEXT']), use_container_width=True)

        with st.expander("View Query Text"):
            for idx, row in long_queries.head(10).iterrows():
                st.code(row['QUERY_TEXT'][:1000] if row['QUERY_TEXT'] else "N/A", language='sql')
                st.divider()

        if st.button(":material/psychology: Get AI long query analysis", key="longquery_ai"):
            with st.spinner("Analyzing long-running queries..."):
                query_samples = long_queries[['WAREHOUSE_NAME', 'WAREHOUSE_SIZE', 'EXEC_SEC', 'GB_SCANNED',
                                              'LOCAL_SPILL_GB', 'PRUNING_RATIO', 'QUERY_TEXT']].head(10).to_string()

                prompt = """You are a Snowflake query optimization expert analyzing query history data.
Analyze these long-running queries.

RECOMMENDATIONS SHOULD INCLUDE:
1. Query rewrite suggestions for common anti-patterns
2. Index/clustering recommendations
3. Materialized view opportunities
4. Warehouse sizing vs query optimization trade-offs
5. Incremental processing opportunities"""

                data_context = f"LONG-RUNNING QUERIES:\n{query_samples}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)
    else:
        st.success(f"No queries exceeded {threshold_min} minute threshold")

elif selected == ":material/lightbulb: Optimization Summary":
    st.subheader("Optimization Recommendations")

    recs_df = generate_recommendations(where_clause, source)

    high_priority = recs_df[recs_df['Priority'] == 'High']
    med_priority = recs_df[recs_df['Priority'] == 'Medium']
    low_priority = recs_df[recs_df['Priority'] == 'Low']

    if not high_priority.empty:
        st.error(f"{len(high_priority)} High Priority Recommendations")
        for _, rec in high_priority.iterrows():
            st.markdown(f"**[{rec['Category']}]** {rec['Recommendation']}")

    if not med_priority.empty:
        st.warning(f"{len(med_priority)} medium priority recommendations", icon=":material/warning:")
        for _, rec in med_priority.iterrows():
            st.markdown(f"**[{rec['Category']}]** {rec['Recommendation']}")

    if not low_priority.empty:
        st.info(f"{len(low_priority)} low priority recommendations", icon=":material/info:")
        for _, rec in low_priority.iterrows():
            st.markdown(f"**[{rec['Category']}]** {rec['Recommendation']}")

    if high_priority.empty and med_priority.empty and low_priority.empty:
        st.success("No critical issues detected!")

    st.markdown("**Recommendations table**")
    st.dataframe(recs_df, use_container_width=True)

    st.markdown("**Quick stats for discussion**")
    quick_stats = execute_query(f"""
        SELECT 
            COUNT(*) as total_queries,
            COUNT(DISTINCT user_name) as users,
            COUNT(DISTINCT warehouse_name) as warehouses,
            SUM(total_elapsed_time)/1000/60/60 as total_hours,
            SUM(bytes_scanned)/1e15 as pb_scanned,
            AVG(percentage_scanned_from_cache) as avg_cache_hit,
            SUM(bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage)/1e12 as tb_spilled,
            SUM(credits_used_cloud_services) as cloud_credits
        FROM {source}
        WHERE {where_clause}
        AND execution_status = 'SUCCESS'
    """)

    if not quick_stats.empty:
        st.json(quick_stats.iloc[0].to_dict())

    if st.button(":material/psychology: Get AI overall recommendations", key="overall_ai"):
        with st.spinner("Generating comprehensive recommendations..."):
            recs_text = recs_df.to_string()
            stats_text = quick_stats.iloc[0].to_dict() if not quick_stats.empty else {}

            prompt = """You are a Snowflake optimization expert analyzing query history data.
Based on the recommendations and overall statistics, provide a prioritized action plan.

RECOMMENDATIONS SHOULD INCLUDE:
1. Top 3-5 actions sorted by impact/effort ratio
2. Estimated cost savings or performance improvements
3. Dependencies between recommendations
4. Quick wins that can be implemented immediately
5. Long-term architectural improvements"""

            data_context = f"AUTO-GENERATED RECOMMENDATIONS:\n{recs_text}\n\nOVERALL STATISTICS:\n{stats_text}"
            suggestions = get_ai_suggestions(prompt, data_context)
            st.markdown("### :material/lightbulb: AI Action Plan")
            st.markdown(suggestions)
