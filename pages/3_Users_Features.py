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

st.title(":material/group: Users, Roles & Feature Adoption")

tab1, tab2, tab3, tab4 = st.tabs([
    ":material/person: User Activity", 
    ":material/admin_panel_settings: Role Analysis", 
    ":material/code: Query Types", 
    ":material/extension: Feature Adoption"
])

with tab1:
    st.subheader("User Activity Analysis")
    
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
    
    user_df = get_user_stats(where_clause)
    
    if not user_df.empty:
        user_plot = to_pandas_native(user_df)
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Users", len(user_plot))
        col2.metric("Avg Queries/User", f"{user_plot['QUERY_COUNT'].mean():,.0f}")
        col3.metric("Most Active User", user_plot.iloc[0]['USER_NAME'])
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(user_plot.head(15), x='USER_NAME', y='TOTAL_HOURS',
                        title="Top 15 Users by Query Hours")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            user_plot['TOTAL_HOURS'] = user_plot['TOTAL_HOURS'].fillna(0).astype(float)
            fig = px.scatter(user_plot, x='QUERY_COUNT', y='TB_SCANNED', 
                           hover_name='USER_NAME', size='TOTAL_HOURS',
                           title="User Query Volume vs Data Scanned")
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("**User details**")
        st.dataframe(user_df, use_container_width=True)
        
        st.markdown("**User activity over time**")
        user_daily = execute_query(f"""
            SELECT 
                DATE_TRUNC('day', start_time) as day,
                COUNT(DISTINCT user_name) as active_users,
                COUNT(*) as query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            GROUP BY day
            ORDER BY day
        """)
        
        if not user_daily.empty:
            fig = px.line(user_daily, x='DAY', y='ACTIVE_USERS', title="Daily Active Users")
            st.plotly_chart(fig, use_container_width=True)
        
        if st.button(":material/psychology: Get AI user analysis", key="user_ai"):
            with st.spinner("Analyzing user patterns..."):
                user_stats = user_plot.head(20).to_string()
                
                prompt = """You are a Snowflake usage optimization expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these user activity patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Identify power users who may benefit from dedicated warehouses or optimizations
2. Users with high spill ratios whose queries need optimization
3. Opportunities for user training based on usage patterns
4. Governance recommendations (users accessing too many resources)
5. Cost attribution opportunities and chargeback recommendations"""
                
                data_context = f"USER ACTIVITY:\n{user_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab2:
    st.subheader("Role Usage Analysis")
    
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
    
    role_df = get_role_stats(where_clause)
    
    if not role_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(role_df.head(10), values='QUERY_COUNT', names='ROLE_NAME',
                        title="Query Distribution by Role")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(role_df.head(15), x='ROLE_NAME', y='UNIQUE_USERS',
                        title="Users per Role")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(role_df, use_container_width=True)
        
        st.markdown("**Role-warehouse matrix**")
        role_wh = execute_query(f"""
            SELECT 
                role_name,
                warehouse_name,
                COUNT(*) as query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND role_name IS NOT NULL
            AND warehouse_name IS NOT NULL
            GROUP BY role_name, warehouse_name
            ORDER BY query_count DESC
            LIMIT 100
        """)
        
        if not role_wh.empty:
            pivot = role_wh.pivot(index='ROLE_NAME', columns='WAREHOUSE_NAME', values='QUERY_COUNT').fillna(0)
            fig = px.imshow(pivot, title="Role-Warehouse Usage Matrix",
                          color_continuous_scale='Blues')
            st.plotly_chart(fig, use_container_width=True)
        
        if st.button(":material/psychology: Get AI role analysis", key="role_ai"):
            with st.spinner("Analyzing role patterns..."):
                role_stats = role_df.to_string()
                
                prompt = """You are a Snowflake governance and RBAC expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these role usage patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Role consolidation opportunities (similar access patterns, overlapping privileges)
2. Identify overly permissive roles (accessing many databases/warehouses)
3. Suggest role hierarchy improvements
4. Warehouse access governance (too many roles using expensive warehouses)
5. Least privilege principle violations and remediation steps"""
                
                data_context = f"ROLE USAGE:\n{role_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab3:
    st.subheader("Query Type Distribution")
    
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
    
    qt_df = get_query_types(where_clause)
    
    if not qt_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(qt_df, values='QUERY_COUNT', names='QUERY_TYPE',
                        title="Query Count by Type")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(qt_df, values='TOTAL_HOURS', names='QUERY_TYPE',
                        title="Time Spent by Query Type")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(qt_df, use_container_width=True)
        
        st.markdown("**Query type trends**")
        qt_trend = execute_query(f"""
            SELECT 
                DATE_TRUNC('week', start_time) as week,
                query_type,
                COUNT(*) as query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            GROUP BY week, query_type
            ORDER BY week
        """)
        
        if not qt_trend.empty:
            top_types = qt_df.head(8)['QUERY_TYPE'].tolist()
            qt_trend_top = qt_trend[qt_trend['QUERY_TYPE'].isin(top_types)]
            fig = px.area(qt_trend_top, x='WEEK', y='QUERY_COUNT', color='QUERY_TYPE',
                         title="Weekly Query Type Trends")
            st.plotly_chart(fig, use_container_width=True)
        
        if st.button(":material/psychology: Get AI query type analysis", key="qt_ai"):
            with st.spinner("Analyzing query patterns..."):
                qt_stats = qt_df.to_string()
                
                prompt = """You are a Snowflake workload optimization expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these query type patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Balance between read (SELECT) and write (INSERT/UPDATE/DELETE) workloads
2. High DML activity that could benefit from MERGE or bulk operations
3. ETL patterns that could use streams/tasks or dynamic tables
4. Query workloads that could benefit from materialized views
5. Opportunities to shift query types to off-peak hours"""
                
                data_context = f"QUERY TYPE DISTRIBUTION:\n{qt_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)

with tab4:
    st.subheader("Feature Adoption Analysis")
    
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
    
    feature_df = get_feature_adoption(where_clause)
    
    if not feature_df.empty:
        feature_df['ADOPTION_PCT'] = feature_df.apply(
            lambda row: (row['QUERIES_USING'] or 0) * 100.0 / (row['TOTAL_QUERIES'] or 1), axis=1
        )
        
        fig = px.bar(feature_df, x='FEATURE', y='ADOPTION_PCT',
                    title="Feature Adoption Rate (%)")
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(feature_df, use_container_width=True)
    
    st.markdown("**Query tags analysis**")
    tags_df = execute_query(f"""
        SELECT 
            query_tag,
            COUNT(*) as query_count,
            SUM(total_elapsed_time)/1000/60/60 as total_hours,
            COUNT(DISTINCT user_name) as unique_users,
            AVG(execution_time)/1000 as avg_exec_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where_clause}
        AND execution_status = 'SUCCESS'
        AND query_tag IS NOT NULL AND query_tag != ''
        GROUP BY query_tag
        ORDER BY query_count DESC
        LIMIT 50
    """)
    
    if not tags_df.empty:
        st.info(f"Found {len(tags_df)} distinct query tags", icon=":material/info:")
        fig = px.bar(tags_df.head(20), x='QUERY_TAG', y='QUERY_COUNT',
                    title="Top 20 Query Tags")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(tags_df, use_container_width=True)
    else:
        st.warning("No query tags found - consider recommending query tagging for workload attribution")
    
    st.markdown("**Database usage**")
    db_usage = execute_query(f"""
        SELECT 
            database_name,
            COUNT(*) as query_count,
            COUNT(DISTINCT schema_name) as schemas_used,
            COUNT(DISTINCT user_name) as unique_users,
            SUM(bytes_scanned)/1e12 as tb_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {where_clause}
        AND execution_status = 'SUCCESS'
        AND database_name IS NOT NULL
        GROUP BY database_name
        ORDER BY query_count DESC
    """)
    
    if not db_usage.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(db_usage.head(10), values='QUERY_COUNT', names='DATABASE_NAME',
                        title="Query Distribution by Database")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.dataframe(db_usage, use_container_width=True)
        
        if st.button(":material/psychology: Get AI feature adoption suggestions", key="feature_ai"):
            with st.spinner("Analyzing feature adoption..."):
                feature_stats = feature_df.to_string() if not feature_df.empty else "No feature data"
                db_stats = db_usage.to_string()
                tags_stats = tags_df.to_string() if not tags_df.empty else "No query tags in use"
                
                prompt = """You are a Snowflake feature adoption expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these feature usage patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Query tagging recommendations for better workload attribution and chargeback
2. Features that could improve the workload (streams, tasks, dynamic tables)
3. External function usage patterns and alternatives
4. Data sharing/data transfer optimization opportunities
5. Governance improvements (resource monitors, access policies)"""
                
                data_context = f"FEATURE ADOPTION:\n{feature_stats}\n\nDATABASE USAGE:\n{db_stats}\n\nQUERY TAGS:\n{tags_stats}"
                suggestions = get_ai_suggestions(prompt, data_context)
                st.markdown("### :material/lightbulb: AI Recommendations")
                st.markdown(suggestions)
