import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

st.title(":material/warehouse: Warehouse Analysis & Optimization")

tab1, tab2, tab3, tab4 = st.tabs([
    ":material/dashboard: Overview", 
    ":material/straighten: Right-sizing", 
    ":material/insights: Utilization Patterns", 
    ":material/hub: Multi-cluster Analysis"
])

with tab1:
    st.subheader("Warehouse Summary")
    
    @st.cache_data(ttl=300)
    def get_warehouse_summary(where):
        return execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                warehouse_type,
                COUNT(*) as total_queries,
                COUNT(DISTINCT user_name) as unique_users,
                SUM(total_elapsed_time)/1000/60/60 as total_hours,
                AVG(execution_time)/1000 as avg_exec_sec,
                MEDIAN(execution_time)/1000 as median_exec_sec,
                MAX(execution_time)/1000 as max_exec_sec,
                SUM(bytes_scanned)/1e12 as total_tb_scanned,
                SUM(credits_used_cloud_services) as cloud_credits,
                SUM(queued_provisioning_time + queued_overload_time)/1000/60 as total_queue_min,
                SUM(bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage)/1e9 as total_spill_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name, warehouse_size, warehouse_type
            ORDER BY total_hours DESC
        """)
    
    wh_df = get_warehouse_summary(where_clause)
    
    if not wh_df.empty:
        wh_plot = to_pandas_native(wh_df)
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(wh_plot.head(15), x='WAREHOUSE_NAME', y='TOTAL_HOURS', 
                        color='WAREHOUSE_SIZE', title="Total Query Hours by Warehouse")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            wh_plot['TOTAL_TB_SCANNED'] = wh_plot['TOTAL_TB_SCANNED'].fillna(0).astype(float)
            fig = px.scatter(wh_plot, x='TOTAL_QUERIES', y='AVG_EXEC_SEC', 
                           size='TOTAL_TB_SCANNED', color='WAREHOUSE_SIZE',
                           hover_name='WAREHOUSE_NAME',
                           title="Query Count vs Avg Execution Time")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(wh_df, use_container_width=True)
        
        if st.button(":material/psychology: Get AI optimization suggestions", key="wh_overview_ai"):
            with st.spinner("Analyzing warehouse patterns..."):
                wh_stats = wh_df.to_string()
                
                prompt = """You are a Snowflake warehouse optimization expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these warehouse usage patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Identify warehouses with disproportionate queue time or spilling vs query volume
2. Suggest warehouse consolidation opportunities (similar workloads, low utilization)
3. Identify warehouses that may be over-provisioned (high query count but low exec time)
4. Recommend warehouse specialization strategies (separate ETL from BI workloads)
5. Cost optimization opportunities based on usage patterns"""
                
                data_context = f"WAREHOUSE SUMMARY:\n{wh_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab2:
    st.subheader("Warehouse Right-sizing Analysis")
    st.markdown("""
    **Sizing Indicators:**
    - High spilling → Consider upsizing
    - High queue time → Consider upsizing or multi-cluster
    - Low query load % → Consider downsizing
    - Short query times with large warehouse → Consider downsizing
    """)
    
    @st.cache_data(ttl=300)
    def get_sizing_analysis(where):
        return execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                COUNT(*) as query_count,
                AVG(query_load_percent) as avg_load_pct,
                AVG(execution_time)/1000 as avg_exec_sec,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time)/1000 as p95_exec_sec,
                SUM(bytes_spilled_to_local_storage)/1e9 as local_spill_gb,
                SUM(bytes_spilled_to_remote_storage)/1e9 as remote_spill_gb,
                SUM(queued_overload_time)/1000/60 as queue_overload_min,
                SUM(queued_provisioning_time)/1000/60 as queue_prov_min,
                CASE 
                    WHEN SUM(bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage) > 1e10 THEN 'UPSIZE - High Spilling'
                    WHEN SUM(queued_overload_time) > 60000 THEN 'UPSIZE - High Queue'
                    WHEN AVG(query_load_percent) < 20 AND warehouse_size IN ('Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large') THEN 'DOWNSIZE - Low Utilization'
                    WHEN AVG(execution_time) < 5000 AND warehouse_size IN ('Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large') THEN 'REVIEW - Fast Queries on Large WH'
                    ELSE 'OK'
                END as recommendation
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name, warehouse_size
            ORDER BY query_count DESC
        """)
    
    sizing_df = get_sizing_analysis(where_clause)
    
    if not sizing_df.empty:
        needs_attention = sizing_df[sizing_df['RECOMMENDATION'] != 'OK']
        
        if not needs_attention.empty:
            st.warning(f"{len(needs_attention)} warehouses may need sizing adjustments", icon=":material/warning:")
            st.dataframe(needs_attention, use_container_width=True)
        else:
            st.success("All warehouses appear appropriately sized")
        
        st.markdown("**All warehouses**")
        st.dataframe(sizing_df, use_container_width=True)
        
        st.markdown("**Execution time distribution by warehouse size**")
        exec_dist = execute_query(f"""
            SELECT 
                warehouse_size,
                CASE 
                    WHEN execution_time < 1000 THEN '< 1s'
                    WHEN execution_time < 10000 THEN '1-10s'
                    WHEN execution_time < 60000 THEN '10-60s'
                    WHEN execution_time < 300000 THEN '1-5min'
                    ELSE '> 5min'
                END as exec_bucket,
                COUNT(*) as query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND warehouse_size IS NOT NULL
            GROUP BY warehouse_size, exec_bucket
            ORDER BY warehouse_size, exec_bucket
        """)
        
        if not exec_dist.empty:
            fig = px.bar(exec_dist, x='WAREHOUSE_SIZE', y='QUERY_COUNT', color='EXEC_BUCKET',
                        title="Query Duration Distribution by Warehouse Size",
                        category_orders={"EXEC_BUCKET": ["< 1s", "1-10s", "10-60s", "1-5min", "> 5min"]})
            st.plotly_chart(fig, use_container_width=True)
        
        if st.button(":material/psychology: Get AI sizing suggestions", key="sizing_ai"):
            with st.spinner("Analyzing sizing opportunities..."):
                sizing_stats = sizing_df.to_string()
                attention_stats = needs_attention.to_string() if not needs_attention.empty else "No warehouses need attention"
                
                prompt = """You are a Snowflake warehouse sizing expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these right-sizing indicators.

RECOMMENDATIONS SHOULD INCLUDE:
1. Specific upsizing recommendations with target sizes for high-spilling warehouses
2. Specific downsizing recommendations for under-utilized warehouses
3. Queries that could be optimized instead of upsizing (JOINs, sorts, aggregations)
4. Auto-suspend/auto-resume settings based on query patterns
5. Consider workload isolation - separate long-running ETL from interactive queries"""
                
                data_context = f"SIZING ANALYSIS:\n{sizing_stats}\n\nWAREHOUSES NEEDING ATTENTION:\n{attention_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab3:
    st.subheader("Usage Patterns Over Time")
    
    @st.cache_data(ttl=300)
    def get_hourly_pattern(where):
        return execute_query(f"""
            SELECT 
                EXTRACT(HOUR FROM start_time) as hour_of_day,
                EXTRACT(DAYOFWEEK FROM start_time) as day_of_week,
                COUNT(*) as query_count,
                AVG(execution_time)/1000 as avg_exec_sec
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            GROUP BY hour_of_day, day_of_week
        """)
    
    pattern_df = get_hourly_pattern(where_clause)
    
    if not pattern_df.empty:
        try:
            heatmap_data = pattern_df.pivot(index='DAY_OF_WEEK', columns='HOUR_OF_DAY', values='QUERY_COUNT').fillna(0)
            if not heatmap_data.empty:
                fig = px.imshow(heatmap_data, 
                               labels=dict(x="Hour of Day", y="Day of Week", color="Query Count"),
                               y=['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][:len(heatmap_data)],
                               title="Query Volume Heatmap (Day vs Hour)",
                               color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not render heatmap: {e}", icon=":material/warning:")
        
        daily_vol = execute_query(f"""
            SELECT 
                DATE_TRUNC('day', start_time) as day,
                warehouse_name,
                COUNT(*) as query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND warehouse_name IS NOT NULL
            GROUP BY day, warehouse_name
            ORDER BY day
        """)
        
        if not daily_vol.empty:
            top_wh = daily_vol.groupby('WAREHOUSE_NAME')['QUERY_COUNT'].sum().nlargest(5).index.tolist()
            daily_vol_top = daily_vol[daily_vol['WAREHOUSE_NAME'].isin(top_wh)]
            
            fig = px.line(daily_vol_top, x='DAY', y='QUERY_COUNT', color='WAREHOUSE_NAME',
                         title="Daily Query Volume (Top 5 Warehouses)")
            st.plotly_chart(fig, use_container_width=True)
        
        if st.button(":material/psychology: Get AI pattern analysis", key="pattern_ai"):
            with st.spinner("Analyzing usage patterns..."):
                pattern_stats = pattern_df.to_string() if not pattern_df.empty else "No pattern data"
                
                prompt = """You are a Snowflake workload optimization expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these usage patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Identify peak usage hours and days - recommend scheduling batch jobs during off-peak
2. Suggest auto-suspend timeout adjustments based on activity gaps
3. Identify opportunities for serverless computing for sporadic workloads
4. Recommend warehouse scheduling policies (scaling during peak, downsizing off-peak)
5. Identify potential cost savings from shifting workloads to off-peak hours"""
                
                data_context = f"HOURLY/DAILY USAGE PATTERNS:\n{pattern_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab4:
    st.subheader("Multi-cluster Warehouse Analysis")
    
    @st.cache_data(ttl=300)
    def get_cluster_data(where):
        return execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                cluster_number,
                COUNT(*) as query_count,
                AVG(execution_time)/1000 as avg_exec_sec,
                SUM(queued_overload_time)/1000 as total_queue_sec
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
            AND cluster_number IS NOT NULL
            GROUP BY warehouse_name, warehouse_size, cluster_number
            ORDER BY warehouse_name, cluster_number
        """)
    
    cluster_df = get_cluster_data(where_clause)
    
    max_cluster = cluster_df['CLUSTER_NUMBER'].max() if not cluster_df.empty else None
    if not cluster_df.empty and max_cluster is not None and max_cluster > 1:
        multi_cluster_wh = cluster_df.groupby('WAREHOUSE_NAME').filter(lambda x: x['CLUSTER_NUMBER'].nunique() > 1)
        
        if not multi_cluster_wh.empty:
            st.success(f"Found {multi_cluster_wh['WAREHOUSE_NAME'].nunique()} multi-cluster warehouses")
            
            fig = px.bar(multi_cluster_wh, x='WAREHOUSE_NAME', y='QUERY_COUNT', color='CLUSTER_NUMBER',
                        title="Query Distribution Across Clusters", barmode='group')
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(multi_cluster_wh, use_container_width=True)
        else:
            st.info("No multi-cluster activity detected in this period.", icon=":material/info:")
    else:
        st.info("No multi-cluster warehouse activity detected.", icon=":material/info:")
    
    st.markdown("**Candidates for multi-cluster**")
    candidates = execute_query(f"""
        SELECT 
            warehouse_name,
            warehouse_size,
            COUNT(*) as query_count,
            SUM(queued_overload_time)/1000/60 as queue_overload_min,
            AVG(queued_overload_time)/1000 as avg_queue_sec,
            MAX(cluster_number) as max_clusters_used
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where_clause}
        AND execution_status = 'SUCCESS'
        AND warehouse_name IS NOT NULL
        GROUP BY warehouse_name, warehouse_size
        HAVING SUM(queued_overload_time) > 60000
        ORDER BY queue_overload_min DESC
    """)
    
    if not candidates.empty:
        st.warning("These warehouses have significant queue time and may benefit from multi-cluster:", icon=":material/warning:")
        st.dataframe(candidates, use_container_width=True)
        
        if st.button(":material/psychology: Get AI multi-cluster suggestions", key="multicluster_ai"):
            with st.spinner("Analyzing multi-cluster opportunities..."):
                candidate_stats = candidates.to_string()
                cluster_stats = cluster_df.to_string() if not cluster_df.empty else "No cluster data"
                
                prompt = """You are a Snowflake multi-cluster warehouse expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these concurrency patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Specific multi-cluster configurations (min/max clusters, scaling policy)
2. Economy vs Standard scaling mode recommendations based on workload patterns
3. Identify if queue time is from concurrency (multi-cluster helps) or query complexity (upsizing helps)
4. Resource monitor recommendations to control costs with multi-cluster
5. Alternatives to multi-cluster (query optimization, workload scheduling)"""
                
                data_context = f"MULTI-CLUSTER CANDIDATES:\n{candidate_stats}\n\nCURRENT CLUSTER USAGE:\n{cluster_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)
    else:
        st.success("No warehouses show significant queuing that would benefit from multi-cluster.")
