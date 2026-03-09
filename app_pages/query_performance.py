import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import execute_query, get_ai_suggestions, build_where_clause, get_query_source, to_pandas_native
from data import get_pruning_data, get_spilling_data, get_cache_data, get_time_breakdown

where_clause = build_where_clause()
source = get_query_source()

st.title(":material/bolt: Query Performance Analysis")

selected = st.segmented_control(
    "Analysis",
    [":material/filter_alt: Pruning Efficiency", ":material/memory: Spilling Analysis",
     ":material/cached: Cache Utilization", ":material/schedule: Time Breakdown"],
    default=":material/filter_alt: Pruning Efficiency",
)

if selected == ":material/filter_alt: Pruning Efficiency":
    st.subheader("Partition Pruning Efficiency")
    st.caption("Poor pruning (ratio > 0.5) indicates missing or ineffective clustering keys.")

    pruning_df = get_pruning_data(where_clause, source)

    if not pruning_df.empty:
        col1, col2, col3 = st.columns(3)
        poor_pruning = pruning_df[pruning_df['PRUNING_RATIO'] > 0.5]
        col1.metric("Queries with poor pruning (>50%)", len(poor_pruning))
        col2.metric("Avg pruning ratio", f"{pruning_df['PRUNING_RATIO'].mean():.2%}")
        col3.metric("Median pruning ratio", f"{pruning_df['PRUNING_RATIO'].median():.2%}")

        fig = px.histogram(pruning_df, x='PRUNING_RATIO', nbins=20,
                          title="Distribution of Pruning Ratios (lower is better)")
        fig.add_vline(x=0.5, line_dash="dash", line_color="red", annotation_text="Poor threshold (50%)")
        fig.update_xaxes(range=[0, max(pruning_df['PRUNING_RATIO'].max() * 1.1, 1.0)])
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Worst pruning by schema**")
        table_pruning = execute_query(f"""
            SELECT 
                database_name || '.' || schema_name as table_location,
                COUNT(*) as query_count,
                AVG(CASE WHEN partitions_total > 0 THEN partitions_scanned::float / partitions_total END) as avg_pruning_ratio,
                SUM(partitions_scanned) as total_partitions_scanned,
                SUM(bytes_scanned)/1e9 as total_gb_scanned
            FROM {source}
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND partitions_total > 100
            GROUP BY database_name, schema_name
            HAVING AVG(CASE WHEN partitions_total > 0 THEN partitions_scanned::float / partitions_total END) > 0.5
            ORDER BY total_gb_scanned DESC
            LIMIT 20
        """)
        st.dataframe(table_pruning, use_container_width=True)

        with st.expander("View queries with worst pruning", icon=":material/expand_more:"):
            st.dataframe(poor_pruning[['QUERY_ID', 'DATABASE_NAME', 'SCHEMA_NAME', 'PRUNING_RATIO',
                                       'PARTITIONS_SCANNED', 'PARTITIONS_TOTAL', 'EXECUTION_TIME', 'QUERY_TEXT']].head(50),
                        use_container_width=True)

        if st.button(":material/psychology: Get AI optimization suggestions", key="pruning_ai"):
            with st.spinner("Analyzing pruning patterns..."):
                worst_queries = poor_pruning[['DATABASE_NAME', 'SCHEMA_NAME', 'PRUNING_RATIO', 'QUERY_TEXT']].head(10).to_string()
                table_stats = table_pruning.to_string() if not table_pruning.empty else "No table-level stats"

                prompt = """You are a Snowflake performance expert analyzing query history data.
Analyze these queries with poor partition pruning.

RECOMMENDATIONS SHOULD INCLUDE:
1. Identify columns used in WHERE/JOIN clauses that could benefit from clustering
2. Suggest specific clustering key recommendations for the tables mentioned
3. Identify predicates that may block pruning (functions on columns, OR conditions, CASE expressions)
4. Recommend Search Optimization for selective equality/IN patterns on VARIANT columns
5. Suggest query rewrites to improve pruning"""

                data_context = f"QUERIES WITH POOR PRUNING:\n{worst_queries}\n\nTABLE-LEVEL STATISTICS:\n{table_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)
    else:
        st.info("No queries with partition data found in this period.", icon=":material/info:")

elif selected == ":material/memory: Spilling Analysis":
    st.subheader("Memory Spilling Analysis")
    st.caption("Spilling to local/remote storage indicates warehouse undersizing or inefficient queries.")

    spill_stats = get_spilling_data(where_clause, source)

    if not spill_stats.empty:
        col1, col2, col3, col4 = st.columns(4)
        pct_spilled = spill_stats['PCT_QUERIES_SPILLED'].iloc[0]
        pct_time = spill_stats['PCT_TIME_SPILLED'].iloc[0]
        local_gb = spill_stats['LOCAL_SPILL_GB'].iloc[0]
        remote_gb = spill_stats['REMOTE_SPILL_GB'].iloc[0]
        col1.metric("% queries spilling", f"{pct_spilled:.1f}%" if pct_spilled is not None else "N/A")
        col2.metric("% time in spilled queries", f"{pct_time:.1f}%" if pct_time is not None else "N/A")
        col3.metric("Local spill (GB)", f"{local_gb:,.1f}" if local_gb is not None else "N/A")
        col4.metric("Remote spill (GB)", f"{remote_gb:,.1f}" if remote_gb is not None else "N/A")

        spill_by_wh = execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                COUNT(*) as total_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) as spilled_queries,
                SUM(bytes_spilled_to_local_storage)/1e9 as local_spill_gb,
                SUM(bytes_spilled_to_remote_storage)/1e9 as remote_spill_gb,
                AVG(execution_time)/1000 as avg_exec_sec
            FROM {source}
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            GROUP BY warehouse_name, warehouse_size
            HAVING SUM(bytes_spilled_to_local_storage) > 0 OR SUM(bytes_spilled_to_remote_storage) > 0
            ORDER BY local_spill_gb + remote_spill_gb DESC
        """)

        st.markdown("**Spilling by warehouse**")
        if not spill_by_wh.empty:
            spill_plot = to_pandas_native(spill_by_wh)
            spill_melted = spill_plot.melt(id_vars=['WAREHOUSE_NAME'], value_vars=['LOCAL_SPILL_GB', 'REMOTE_SPILL_GB'],
                                           var_name='Spill Type', value_name='GB')
            fig = px.bar(spill_melted, x='WAREHOUSE_NAME', y='GB', color='Spill Type',
                        title="Spill Volume by Warehouse", barmode='stack')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(spill_by_wh, use_container_width=True)

        st.markdown("**Top spilling queries**")
        spilling_queries = execute_query(f"""
            SELECT 
                query_id,
                DATE(start_time) as query_date,
                warehouse_name,
                warehouse_size,
                bytes_spilled_to_local_storage/1e6 as local_spill_mb,
                bytes_spilled_to_remote_storage/1e6 as remote_spill_mb,
                execution_time/1000 as exec_sec,
                query_text
            FROM {source}
            WHERE {where_clause}
            AND (bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0)
            ORDER BY bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage DESC
            LIMIT 50
        """)
        st.dataframe(spilling_queries, use_container_width=True)

        if st.button(":material/psychology: Get AI optimization suggestions", key="spilling_ai"):
            with st.spinner("Analyzing spilling patterns..."):
                wh_stats = spill_by_wh.to_string() if not spill_by_wh.empty else "No warehouse stats"
                query_samples = spilling_queries[['WAREHOUSE_NAME', 'WAREHOUSE_SIZE', 'LOCAL_SPILL_MB', 'REMOTE_SPILL_MB', 'QUERY_TEXT']].head(10).to_string()

                prompt = """You are a Snowflake performance expert analyzing query history data.
Analyze these queries with memory spilling issues.

RECOMMENDATIONS SHOULD INCLUDE:
1. Warehouse sizing recommendations based on spill patterns
2. Query optimizations to reduce memory pressure
3. Identify patterns like DISTINCT, ORDER BY, GROUP BY on high-cardinality columns
4. Suggest breaking up complex queries or using intermediate tables
5. Consider multi-cluster warehouses for concurrent workloads"""

                data_context = f"WAREHOUSE SPILL STATISTICS:\n{wh_stats}\n\nTOP SPILLING QUERIES:\n{query_samples}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

elif selected == ":material/cached: Cache Utilization":
    st.subheader("Result Cache & Local Storage Cache")

    cache_df = get_cache_data(where_clause, source)

    if not cache_df.empty:
        cache_plot = to_pandas_native(cache_df)
        col1, col2 = st.columns(2)
        with col1:
            fig = px.line(cache_plot, x='DAY', y='AVG_CACHE_HIT', title="Daily Average Cache Hit Rate")
            fig.update_yaxes(tickformat='.0%')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            cache_melted = cache_plot.melt(id_vars=['DAY'], value_vars=['CACHED_GB', 'TOTAL_SCANNED_GB'],
                                           var_name='Type', value_name='GB')
            fig = px.bar(cache_melted, x='DAY', y='GB', color='Type',
                        title="Data Scanned: Cached vs Total", barmode='overlay')
            st.plotly_chart(fig, use_container_width=True)

        cache_by_wh = execute_query(f"""
            SELECT 
                warehouse_name,
                COUNT(*) as query_count,
                AVG(percentage_scanned_from_cache) as avg_cache_hit,
                SUM(bytes_scanned)/1e9 as total_scanned_gb
            FROM {source}
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            GROUP BY warehouse_name
            ORDER BY total_scanned_gb DESC
        """)

        st.markdown("**Cache hit rate by warehouse**")
        st.dataframe(cache_by_wh, use_container_width=True)

        if st.button(":material/psychology: Get AI optimization suggestions", key="cache_ai"):
            with st.spinner("Analyzing cache patterns..."):
                cache_stats = cache_by_wh.to_string() if not cache_by_wh.empty else "No cache stats"

                prompt = """You are a Snowflake performance expert analyzing query history data.
Analyze these cache utilization patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Identify warehouses with low cache hit rates and potential causes
2. Suggest query patterns that could improve cache utilization
3. Recommend warehouse auto-suspend settings to balance cache warmth vs cost
4. Identify if data freshness requirements allow for result cache usage
5. Suggest clustering or materialized views for frequently accessed patterns"""

                data_context = f"CACHE BY WAREHOUSE:\n{cache_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

elif selected == ":material/schedule: Time Breakdown":
    st.subheader("Query Time Breakdown")
    st.caption("Understanding where time is spent: compilation, queueing, execution.")

    time_df = get_time_breakdown(where_clause, source)

    if not time_df.empty:
        def _nz(x):
            return 0.0 if x is None else float(x)

        time_data = {
            'Category': ['Compilation', 'Queue (Provisioning)', 'Queue (Overload)', 'Queue (Repair)', 'Execution', 'Blocked'],
            'Hours': [
                _nz(time_df['COMPILATION_HOURS'].iloc[0]),
                _nz(time_df['QUEUE_PROVISIONING_HOURS'].iloc[0]),
                _nz(time_df['QUEUE_OVERLOAD_HOURS'].iloc[0]),
                _nz(time_df['QUEUE_REPAIR_HOURS'].iloc[0]),
                _nz(time_df['EXECUTION_HOURS'].iloc[0]),
                _nz(time_df['BLOCKED_HOURS'].iloc[0])
            ]
        }
        time_chart_df = pd.DataFrame(time_data)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(time_chart_df, values='Hours', names='Category', title="Total Time Distribution")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.dataframe(time_chart_df, use_container_width=True)
            total_queue = _nz(time_df['QUEUE_PROVISIONING_HOURS'].iloc[0]) + _nz(time_df['QUEUE_OVERLOAD_HOURS'].iloc[0])
            if total_queue > 1:
                st.warning(f"{total_queue:.1f} hours spent in queues - consider warehouse sizing or auto-scaling.", icon=":material/warning:")

        st.markdown("**Queue time by warehouse**")
        queue_by_wh = execute_query(f"""
            SELECT 
                warehouse_name,
                warehouse_size,
                COUNT(*) as query_count,
                SUM(queued_provisioning_time)/1000/60 as queue_prov_min,
                SUM(queued_overload_time)/1000/60 as queue_overload_min,
                AVG(queued_provisioning_time + queued_overload_time)/1000 as avg_queue_sec
            FROM {source}
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            GROUP BY warehouse_name, warehouse_size
            HAVING SUM(queued_provisioning_time + queued_overload_time) > 0
            ORDER BY queue_prov_min + queue_overload_min DESC
        """)
        st.dataframe(queue_by_wh, use_container_width=True)

        if st.button(":material/psychology: Get AI optimization suggestions", key="queue_ai"):
            with st.spinner("Analyzing queue patterns..."):
                queue_stats = queue_by_wh.to_string() if not queue_by_wh.empty else "No queue stats"
                time_breakdown = time_chart_df.to_string()

                prompt = """You are a Snowflake performance expert analyzing query history data.
Analyze these query queueing and time breakdown patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. For high provisioning queue time: adjust auto-suspend/auto-resume settings
2. For high overload queue time: enable multi-cluster warehouses
3. For high compilation time: identify complex queries, suggest materialized views
4. For high blocked time: identify transaction contention
5. Recommend specific warehouse configurations"""

                data_context = f"TIME BREAKDOWN:\n{time_breakdown}\n\nQUEUE TIME BY WAREHOUSE:\n{queue_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)
