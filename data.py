import streamlit as st
import pandas as pd
from utils import execute_query


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


# --- Query Performance ---

@st.cache_data(ttl=300)
def get_pruning_data(where):
    return execute_query(f"""
        SELECT 
            query_id,
            query_text,
            database_name,
            schema_name,
            warehouse_name,
            warehouse_size,
            partitions_scanned,
            partitions_total,
            CASE WHEN partitions_total = 0 THEN NULL ELSE partitions_scanned::float / partitions_total END as pruning_ratio,
            execution_time,
            total_elapsed_time,
            bytes_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND partitions_total > 0
        AND execution_time > 1000
        ORDER BY pruning_ratio DESC NULLS LAST
        LIMIT 500
    """)


@st.cache_data(ttl=300)
def get_spilling_data(where):
    return execute_query(f"""
        WITH stats AS (
            SELECT 
                COUNT(*) as total_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) as spilled_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 THEN total_elapsed_time ELSE 0 END) as spilled_time,
                SUM(total_elapsed_time) as total_time,
                SUM(bytes_spilled_to_local_storage)/1e9 as local_spill_gb,
                SUM(bytes_spilled_to_remote_storage)/1e9 as remote_spill_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where}
            AND execution_status = 'SUCCESS'
        )
        SELECT 
            total_queries,
            spilled_queries,
            spilled_queries * 100.0 / NULLIF(total_queries, 0) as pct_queries_spilled,
            spilled_time * 100.0 / NULLIF(total_time, 0) as pct_time_spilled,
            local_spill_gb,
            remote_spill_gb
        FROM stats
    """)


@st.cache_data(ttl=300)
def get_cache_data(where):
    return execute_query(f"""
        SELECT 
            DATE_TRUNC('day', start_time) as day,
            AVG(percentage_scanned_from_cache) as avg_cache_hit,
            SUM(bytes_scanned)/1e9 as total_scanned_gb,
            SUM(bytes_scanned * percentage_scanned_from_cache)/1e9 as cached_gb
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        GROUP BY DATE_TRUNC('day', start_time)
        ORDER BY day
    """)


@st.cache_data(ttl=300)
def get_time_breakdown(where):
    return execute_query(f"""
        SELECT 
            COALESCE(SUM(compilation_time), 0)/1000/60/60 as compilation_hours,
            COALESCE(SUM(queued_provisioning_time), 0)/1000/60/60 as queue_provisioning_hours,
            COALESCE(SUM(queued_overload_time), 0)/1000/60/60 as queue_overload_hours,
            COALESCE(SUM(queued_repair_time), 0)/1000/60/60 as queue_repair_hours,
            COALESCE(SUM(execution_time), 0)/1000/60/60 as execution_hours,
            COALESCE(SUM(transaction_blocked_time), 0)/1000/60/60 as blocked_hours
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
    """)


# --- Warehouse Analysis ---

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


# --- Users & Features ---

@st.cache_data(ttl=300)
def get_user_stats(where):
    return execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as query_count,
            COUNT(DISTINCT DATE_TRUNC('day', start_time)) as active_days,
            SUM(total_elapsed_time)/1000/60/60 as total_hours,
            AVG(execution_time)/1000 as avg_exec_sec,
            SUM(bytes_scanned)/1e12 as tb_scanned,
            COUNT(DISTINCT warehouse_name) as warehouses_used,
            COUNT(DISTINCT role_name) as roles_used,
            SUM(bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage)/1e9 as spill_gb
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        GROUP BY user_name
        ORDER BY total_hours DESC
    """)


@st.cache_data(ttl=300)
def get_role_stats(where):
    return execute_query(f"""
        SELECT 
            role_name,
            COUNT(*) as query_count,
            COUNT(DISTINCT user_name) as unique_users,
            SUM(total_elapsed_time)/1000/60/60 as total_hours,
            COUNT(DISTINCT warehouse_name) as warehouses_used,
            COUNT(DISTINCT database_name) as databases_accessed,
            SUM(bytes_scanned)/1e12 as tb_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        GROUP BY role_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_query_types(where):
    return execute_query(f"""
        SELECT 
            query_type,
            COUNT(*) as query_count,
            SUM(total_elapsed_time)/1000/60/60 as total_hours,
            AVG(execution_time)/1000 as avg_exec_sec,
            SUM(bytes_scanned)/1e12 as tb_scanned,
            SUM(rows_inserted + rows_updated + rows_deleted) as rows_modified
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        GROUP BY query_type
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_feature_adoption(where):
    return execute_query(f"""
        SELECT 
            'External Functions' as feature,
            COUNT(CASE WHEN external_function_total_invocations > 0 THEN 1 END) as queries_using,
            COUNT(*) as total_queries,
            SUM(external_function_total_invocations) as total_invocations
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        
        UNION ALL
        
        SELECT 
            'Data Transfer (Outbound)' as feature,
            COUNT(CASE WHEN outbound_data_transfer_bytes > 0 THEN 1 END) as queries_using,
            COUNT(*) as total_queries,
            SUM(outbound_data_transfer_bytes)/1e9 as total_invocations
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        
        UNION ALL
        
        SELECT 
            'Data Transfer (Inbound)' as feature,
            COUNT(CASE WHEN inbound_data_transfer_bytes > 0 THEN 1 END) as queries_using,
            COUNT(*) as total_queries,
            SUM(inbound_data_transfer_bytes)/1e9 as total_invocations
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        
        UNION ALL
        
        SELECT 
            'Query Tags' as feature,
            COUNT(CASE WHEN query_tag IS NOT NULL AND query_tag != '' THEN 1 END) as queries_using,
            COUNT(*) as total_queries,
            COUNT(DISTINCT query_tag) as total_invocations
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
    """)


# --- Errors & Recommendations ---

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


# --- Cortex AI Usage ---

@st.cache_data(ttl=300)
def get_llm_usage(where):
    return execute_query(f"""
        SELECT 
            query_type,
            user_name,
            COUNT(*) as query_count,
            SUM(total_elapsed_time)/1000/60 as total_minutes,
            AVG(execution_time)/1000 as avg_exec_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND (
            UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.COMPLETE%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.SUMMARIZE%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.TRANSLATE%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.SENTIMENT%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.EXTRACT_ANSWER%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE.ML.%'
        )
        GROUP BY query_type, user_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_search_usage(where):
    return execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as query_count,
            AVG(execution_time)/1000 as avg_exec_sec,
            SUM(execution_time)/1000/60 as total_minutes
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND UPPER(query_text) LIKE '%CORTEX_SEARCH%'
        GROUP BY user_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_analyst_usage(where):
    return execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as query_count,
            AVG(execution_time)/1000 as avg_exec_sec,
            SUM(execution_time)/1000/60 as total_minutes
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND (
            UPPER(query_text) LIKE '%CORTEX.ANALYST%'
            OR UPPER(query_text) LIKE '%SEMANTIC_MODEL%'
        )
        GROUP BY user_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_docai_usage(where):
    return execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as query_count,
            AVG(execution_time)/1000 as avg_exec_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND (
            UPPER(query_text) LIKE '%DOCUMENT_AI%'
            OR UPPER(query_text) LIKE '%PARSE_DOCUMENT%'
            OR UPPER(query_text) LIKE '%CORTEX.EXTRACT%'
        )
        GROUP BY user_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_agent_usage(where):
    return execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as query_count,
            AVG(execution_time)/1000 as avg_exec_sec,
            SUM(execution_time)/1000/60 as total_minutes
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND (
            UPPER(query_text) LIKE '%CORTEX.AGENT%'
            OR UPPER(query_text) LIKE '%CORTEX_AGENT%'
            OR UPPER(query_text) LIKE '%CREATE%AGENT%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.INVOKE_AGENT%'
        )
        GROUP BY user_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_intelligence_usage(where):
    return execute_query(f"""
        SELECT 
            user_name,
            COUNT(*) as query_count,
            AVG(execution_time)/1000 as avg_exec_sec,
            SUM(execution_time)/1000/60 as total_minutes
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND (
            UPPER(query_text) LIKE '%SNOWFLAKE_INTELLIGENCE%'
            OR UPPER(query_text) LIKE '%ASK_COPILOT%'
            OR UPPER(query_text) LIKE '%COPILOT%'
            OR query_tag LIKE '%intelligence%'
            OR query_tag LIKE '%copilot%'
            OR UPPER(query_text) LIKE '%CORTEX.ASK%'
        )
        GROUP BY user_name
        ORDER BY query_count DESC
    """)


@st.cache_data(ttl=300)
def get_ai_summary(where):
    return execute_query(f"""
        SELECT 
            DATE_TRUNC('day', start_time) as day,
            CASE 
                WHEN UPPER(query_text) LIKE '%CORTEX.COMPLETE%' THEN 'LLM_COMPLETE'
                WHEN UPPER(query_text) LIKE '%CORTEX.SUMMARIZE%' THEN 'LLM_SUMMARIZE'
                WHEN UPPER(query_text) LIKE '%CORTEX.TRANSLATE%' THEN 'LLM_TRANSLATE'
                WHEN UPPER(query_text) LIKE '%CORTEX.SENTIMENT%' THEN 'LLM_SENTIMENT'
                WHEN UPPER(query_text) LIKE '%CORTEX_SEARCH%' THEN 'CORTEX_SEARCH'
                WHEN UPPER(query_text) LIKE '%CORTEX.ANALYST%' OR UPPER(query_text) LIKE '%SEMANTIC_MODEL%' THEN 'CORTEX_ANALYST'
                WHEN UPPER(query_text) LIKE '%DOCUMENT_AI%' OR UPPER(query_text) LIKE '%PARSE_DOCUMENT%' THEN 'DOCUMENT_AI'
                WHEN UPPER(query_text) LIKE '%CORTEX.AGENT%' OR UPPER(query_text) LIKE '%INVOKE_AGENT%' THEN 'CORTEX_AGENTS'
                WHEN UPPER(query_text) LIKE '%COPILOT%' OR UPPER(query_text) LIKE '%SNOWFLAKE_INTELLIGENCE%' THEN 'SNOWFLAKE_INTELLIGENCE'
                ELSE 'OTHER_AI'
            END as ai_feature,
            COUNT(*) as query_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where}
        AND execution_status = 'SUCCESS'
        AND (
            UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.%'
            OR UPPER(query_text) LIKE '%CORTEX_SEARCH%'
            OR UPPER(query_text) LIKE '%SEMANTIC_MODEL%'
            OR UPPER(query_text) LIKE '%DOCUMENT_AI%'
            OR UPPER(query_text) LIKE '%PARSE_DOCUMENT%'
            OR UPPER(query_text) LIKE '%CORTEX.AGENT%'
            OR UPPER(query_text) LIKE '%INVOKE_AGENT%'
            OR UPPER(query_text) LIKE '%COPILOT%'
            OR UPPER(query_text) LIKE '%SNOWFLAKE_INTELLIGENCE%'
        )
        GROUP BY day, ai_feature
        ORDER BY day
    """)
