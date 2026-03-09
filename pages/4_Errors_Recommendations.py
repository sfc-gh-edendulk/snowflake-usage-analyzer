import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

if "filter_start" not in st.session_state:
    st.warning("Please set a date range on the main page first.")
    st.stop()

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

def get_ai_suggestions(prompt, data_context):
    conn = get_connection()
    full_prompt = f"""{prompt}

DATA CONTEXT:
{data_context}

Provide specific, actionable recommendations based on this data. Be concise and focus on the highest-impact fixes."""
    
    try:
        result = conn.query(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'claude-3-5-sonnet',
                '{full_prompt.replace("'", "''")}'
            ) as response
        """)
        return result['RESPONSE'].iloc[0]
    except Exception as e:
        return f"Unable to generate AI suggestions: {e}"

def to_pandas_native(df):
    return pd.DataFrame(df.to_dict())

def build_where_clause(start, end):
    clauses = [f"start_time >= '{start}'", f"start_time <= '{end}'"]
    if st.session_state.get("filter_warehouse"):
        clauses.append(f"warehouse_name = '{st.session_state.filter_warehouse}'")
    if st.session_state.get("filter_user"):
        clauses.append(f"user_name = '{st.session_state.filter_user}'")
    return " AND ".join(clauses)

start = st.session_state.get("filter_start", datetime.now().date() - timedelta(days=30))
end = st.session_state.get("filter_end", datetime.now().date())
where_clause = build_where_clause(start, end)

st.title(":material/build: Errors & Optimization Recommendations")

tab1, tab2, tab3 = st.tabs([
    ":material/error: Error Analysis", 
    ":material/timer: Long Running Queries", 
    ":material/lightbulb: Optimization Summary"
])

with tab1:
    st.subheader("Failed Query Analysis")
    
    @st.cache_data(ttl=300)
    def get_error_summary(where):
        return execute_query(f"""
            SELECT 
                COUNT(*) as total_queries,
                SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
                SUM(CASE WHEN execution_status = 'FAIL' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN execution_status = 'INCIDENT' THEN 1 ELSE 0 END) as incident
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
        """)
    
    error_summary = get_error_summary(where_clause)
    
    if not error_summary.empty:
        col1, col2, col3, col4 = st.columns(4)
        total = error_summary['TOTAL_QUERIES'].iloc[0]
        failed = error_summary['FAILED'].iloc[0]
        col1.metric("Total Queries", f"{total:,}")
        col2.metric("Successful", f"{error_summary['SUCCESS'].iloc[0]:,}")
        col3.metric("Failed", f"{failed:,}")
        col4.metric("Failure Rate", f"{(failed*100/(total or 1)):.2f}%")
    
    @st.cache_data(ttl=300)
    def get_error_codes(where):
        return execute_query(f"""
            SELECT 
                error_code,
                error_message,
                COUNT(*) as error_count,
                COUNT(DISTINCT user_name) as affected_users,
                COUNT(DISTINCT warehouse_name) as warehouses
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'FAIL'
            AND error_code IS NOT NULL
            GROUP BY error_code, error_message
            ORDER BY error_count DESC
            LIMIT 30
        """)
    
    error_codes = get_error_codes(where_clause)
    
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
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
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
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
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
                
                prompt = """You are a Snowflake troubleshooting expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these error patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Root cause analysis for common error codes
2. User training recommendations based on error patterns
3. Query patterns that commonly fail and how to prevent them
4. Permission/role issues causing failures
5. Resource contention issues (warehouse sizing, concurrency limits)"""
                
                data_context = f"ERROR CODE DISTRIBUTION:\n{error_stats}\n\nERRORS BY USER:\n{user_error_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab2:
    st.subheader("Long Running Queries")
    
    threshold_min = st.slider("Execution time threshold (minutes)", 1, 60, 5)
    
    @st.cache_data(ttl=300)
    def get_long_queries(where, threshold_ms):
        return execute_query(f"""
            SELECT 
                query_id,
                user_name,
                role_name,
                warehouse_name,
                warehouse_size,
                query_type,
                execution_time/1000 as exec_sec,
                total_elapsed_time/1000 as total_sec,
                compilation_time/1000 as compile_sec,
                queued_provisioning_time/1000 as queue_prov_sec,
                queued_overload_time/1000 as queue_overload_sec,
                bytes_scanned/1e9 as gb_scanned,
                bytes_spilled_to_local_storage/1e9 as local_spill_gb,
                CASE WHEN partitions_total > 0 THEN partitions_scanned::float/partitions_total ELSE NULL END as pruning_ratio,
                query_text
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            AND execution_time > {threshold_ms}
            ORDER BY execution_time DESC
            LIMIT 100
        """)
    
    long_queries = get_long_queries(where_clause, threshold_min * 60 * 1000)
    
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
                
                prompt = """You are a Snowflake query optimization expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these long-running queries.

RECOMMENDATIONS SHOULD INCLUDE:
1. Query rewrite suggestions for common anti-patterns (Cartesian products, unnecessary subqueries)
2. Index/clustering recommendations based on WHERE clause patterns
3. Materialized view opportunities for repeated complex queries
4. Warehouse sizing vs query optimization trade-offs
5. Incremental processing opportunities (streams/tasks, dynamic tables)"""
                
                data_context = f"LONG-RUNNING QUERIES:\n{query_samples}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)
    else:
        st.success(f"No queries exceeded {threshold_min} minute threshold")

with tab3:
    st.subheader("Optimization Recommendations")
    
    @st.cache_data(ttl=300)
    def generate_recommendations(where):
        recs = []
        
        spill_check = execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                SUM(bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage)/1e9 as total_spill_gb,
                COUNT(*) as spilling_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND (bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0)
            GROUP BY warehouse_name, warehouse_size
            HAVING total_spill_gb > 10
            ORDER BY total_spill_gb DESC
        """)
        
        if not spill_check.empty:
            for _, row in spill_check.iterrows():
                recs.append({
                    'Category': 'Warehouse Sizing',
                    'Priority': 'High',
                    'Recommendation': f"Warehouse {row['WAREHOUSE_NAME']} ({row['WAREHOUSE_SIZE']}) has {row['TOTAL_SPILL_GB']:.1f} GB spilled across {row['SPILLING_QUERIES']} queries. Consider upsizing.",
                    'Impact': 'Performance'
                })
        
        queue_check = execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                SUM(queued_overload_time)/1000/60 as queue_min,
                COUNT(*) as queued_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND queued_overload_time > 5000
            GROUP BY warehouse_name, warehouse_size
            HAVING queue_min > 30
            ORDER BY queue_min DESC
        """)
        
        if not queue_check.empty:
            for _, row in queue_check.iterrows():
                recs.append({
                    'Category': 'Concurrency',
                    'Priority': 'High',
                    'Recommendation': f"Warehouse {row['WAREHOUSE_NAME']} has {row['QUEUE_MIN']:.1f} minutes of queue time. Consider multi-cluster or additional warehouses.",
                    'Impact': 'Performance'
                })
        
        pruning_check = execute_query(f"""
            SELECT 
                database_name || '.' || schema_name as location,
                COUNT(*) as query_count,
                AVG(CASE WHEN partitions_total > 0 THEN partitions_scanned::float/partitions_total END) as avg_pruning_ratio,
                SUM(bytes_scanned)/1e12 as tb_scanned
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            AND partitions_total > 100
            GROUP BY database_name, schema_name
            HAVING avg_pruning_ratio > 0.7 AND tb_scanned > 1
            ORDER BY tb_scanned DESC
            LIMIT 5
        """)
        
        if not pruning_check.empty:
            for _, row in pruning_check.iterrows():
                recs.append({
                    'Category': 'Clustering',
                    'Priority': 'Medium',
                    'Recommendation': f"Schema {row['LOCATION']} has {row['AVG_PRUNING_RATIO']:.0%} avg pruning ratio across {row['TB_SCANNED']:.1f} TB scanned. Review clustering keys.",
                    'Impact': 'Cost & Performance'
                })
        
        cache_check = execute_query(f"""
            SELECT 
                warehouse_name,
                AVG(percentage_scanned_from_cache) as avg_cache_hit,
                SUM(bytes_scanned)/1e12 as tb_scanned
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            GROUP BY warehouse_name
            HAVING avg_cache_hit < 0.3 AND tb_scanned > 1
            ORDER BY tb_scanned DESC
            LIMIT 3
        """)
        
        if not cache_check.empty:
            for _, row in cache_check.iterrows():
                recs.append({
                    'Category': 'Caching',
                    'Priority': 'Medium',
                    'Recommendation': f"Warehouse {row['WAREHOUSE_NAME']} has {row['AVG_CACHE_HIT']:.0%} cache hit rate. Consider warehouse assignment strategy for similar queries.",
                    'Impact': 'Performance'
                })
        
        tag_check = execute_query(f"""
            SELECT 
                COUNT(*) as total_queries,
                SUM(CASE WHEN query_tag IS NOT NULL AND query_tag != '' THEN 1 ELSE 0 END) as tagged_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
        """)
        
        if not tag_check.empty:
            total_q = tag_check['TOTAL_QUERIES'].iloc[0] or 0
            pct_tagged = (tag_check['TAGGED_QUERIES'].iloc[0] or 0) * 100.0 / (total_q or 1)
            if pct_tagged < 20:
                recs.append({
                    'Category': 'Governance',
                    'Priority': 'Low',
                    'Recommendation': f"Only {pct_tagged:.1f}% of queries use query tags. Implement query tagging for better workload attribution.",
                    'Impact': 'Observability'
                })
        
        if not recs:
            recs.append({
                'Category': 'General',
                'Priority': 'Info',
                'Recommendation': 'No critical issues detected. Account appears well-optimized.',
                'Impact': 'N/A'
            })
        
        return pd.DataFrame(recs)
    
    recs_df = generate_recommendations(where_clause)
    
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
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where_clause}
        AND execution_status = 'SUCCESS'
    """)
    
    if not quick_stats.empty:
        st.json(quick_stats.iloc[0].to_dict())
    
    if st.button(":material/psychology: Get AI overall recommendations", key="overall_ai"):
        with st.spinner("Generating comprehensive recommendations..."):
            recs_text = recs_df.to_string()
            stats_text = quick_stats.iloc[0].to_dict() if not quick_stats.empty else {}
            
            prompt = """You are a Snowflake optimization expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Based on the recommendations generated and overall statistics,
provide a prioritized action plan.

RECOMMENDATIONS SHOULD INCLUDE:
1. Top 3-5 actions sorted by impact/effort ratio
2. Estimated cost savings or performance improvements
3. Dependencies between recommendations (what to do first)
4. Quick wins that can be implemented immediately
5. Long-term architectural improvements to consider"""
            
            data_context = f"AUTO-GENERATED RECOMMENDATIONS:\n{recs_text}\n\nOVERALL STATISTICS:\n{stats_text}"
            suggestions = get_ai_suggestions(prompt, data_context)
            st.markdown("### :material/lightbulb: AI Action Plan")
            st.markdown(suggestions)
