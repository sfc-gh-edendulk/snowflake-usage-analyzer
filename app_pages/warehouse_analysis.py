import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import execute_query, get_ai_suggestions, build_where_clause, to_pandas_native
from data import get_warehouse_summary, get_sizing_analysis, get_hourly_pattern, get_cluster_data

where_clause = build_where_clause()

st.title(":material/warehouse: Warehouse Analysis & Optimization")

selected = st.segmented_control(
    "Analysis",
    [":material/dashboard: Overview", ":material/straighten: Right-sizing",
     ":material/insights: Utilization Patterns", ":material/hub: Multi-cluster Analysis"],
    default=":material/dashboard: Overview",
)

if selected == ":material/dashboard: Overview":
    st.subheader("Warehouse Summary")

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

elif selected == ":material/straighten: Right-sizing":
    st.subheader("Warehouse Right-sizing Analysis")
    st.markdown("""
    **Sizing Indicators:**
    - High spilling → Consider upsizing
    - High queue time → Consider upsizing or multi-cluster
    - Low query load % → Consider downsizing
    - Short query times with large warehouse → Consider downsizing
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

elif selected == ":material/insights: Utilization Patterns":
    st.subheader("Usage Patterns Over Time")

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

elif selected == ":material/hub: Multi-cluster Analysis":
    st.subheader("Multi-cluster Warehouse Analysis")

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
