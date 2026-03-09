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

Provide specific, actionable recommendations based on this data. Be concise and focus on the highest-impact insights."""
    
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

st.title(":material/smart_toy: Cortex AI & LLM Usage")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    ":material/chat: LLM Functions", 
    ":material/search: Cortex Search", 
    ":material/query_stats: Cortex Analyst", 
    ":material/description: Document AI",
    ":material/smart_toy: Cortex Agents",
    ":material/psychology: Snowflake Intelligence"
])

with tab1:
    st.subheader("Cortex LLM Function Usage")
    st.caption("COMPLETE, SUMMARIZE, TRANSLATE, SENTIMENT, and other LLM functions")
    
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
    
    llm_df = get_llm_usage(where_clause)
    
    if not llm_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total LLM queries", f"{llm_df['QUERY_COUNT'].sum():,}")
        col2.metric("Unique users", llm_df['USER_NAME'].nunique())
        col3.metric("Total minutes", f"{llm_df['TOTAL_MINUTES'].sum():.1f}")
        
        col1, col2 = st.columns(2)
        with col1:
            by_user = llm_df.groupby('USER_NAME')['QUERY_COUNT'].sum().reset_index()
            fig = px.pie(by_user, values='QUERY_COUNT', names='USER_NAME', title="LLM usage by user")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(llm_df.head(15), x='USER_NAME', y='QUERY_COUNT', color='QUERY_TYPE',
                        title="LLM queries by user & type")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(llm_df, use_container_width=True)
        
        st.markdown("**LLM function details**")
        llm_details = execute_query(f"""
            SELECT 
                CASE 
                    WHEN UPPER(query_text) LIKE '%CORTEX.COMPLETE%' THEN 'COMPLETE'
                    WHEN UPPER(query_text) LIKE '%CORTEX.SUMMARIZE%' THEN 'SUMMARIZE'
                    WHEN UPPER(query_text) LIKE '%CORTEX.TRANSLATE%' THEN 'TRANSLATE'
                    WHEN UPPER(query_text) LIKE '%CORTEX.SENTIMENT%' THEN 'SENTIMENT'
                    WHEN UPPER(query_text) LIKE '%CORTEX.EXTRACT_ANSWER%' THEN 'EXTRACT_ANSWER'
                    WHEN UPPER(query_text) LIKE '%SNOWFLAKE.ML%' THEN 'ML_FUNCTIONS'
                    ELSE 'OTHER'
                END as function_type,
                COUNT(*) as call_count,
                AVG(execution_time)/1000 as avg_exec_sec,
                SUM(execution_time)/1000/60 as total_minutes
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND (
                UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.%'
                OR UPPER(query_text) LIKE '%SNOWFLAKE.ML.%'
            )
            GROUP BY function_type
            ORDER BY call_count DESC
        """)
        
        if not llm_details.empty:
            fig = px.bar(llm_details, x='FUNCTION_TYPE', y='CALL_COUNT', 
                        title="Calls by Cortex function type")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(llm_details, use_container_width=True)
    else:
        st.info("No Cortex LLM function usage found in this period.", icon=":material/info:")

with tab2:
    st.subheader("Cortex Search Usage")
    
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
    
    search_df = get_search_usage(where_clause)
    
    if not search_df.empty:
        col1, col2 = st.columns(2)
        col1.metric("Total search queries", f"{search_df['QUERY_COUNT'].sum():,}")
        col2.metric("Avg response time", f"{search_df['AVG_EXEC_SEC'].mean():.2f}s")
        
        fig = px.bar(search_df, x='USER_NAME', y='QUERY_COUNT', title="Cortex Search usage by user")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(search_df, use_container_width=True)
    else:
        st.info("No Cortex Search usage found in this period.", icon=":material/info:")

with tab3:
    st.subheader("Cortex Analyst Usage")
    st.caption("Text-to-SQL semantic model queries")
    
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
    
    analyst_df = get_analyst_usage(where_clause)
    
    if not analyst_df.empty:
        col1, col2 = st.columns(2)
        col1.metric("Total Analyst queries", f"{analyst_df['QUERY_COUNT'].sum():,}")
        col2.metric("Unique users", analyst_df['USER_NAME'].nunique())
        
        fig = px.bar(analyst_df, x='USER_NAME', y='QUERY_COUNT', title="Cortex Analyst usage by user")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(analyst_df, use_container_width=True)
    else:
        st.info("No Cortex Analyst usage found in this period.", icon=":material/info:")

with tab4:
    st.subheader("Document AI & Extraction")
    
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
    
    docai_df = get_docai_usage(where_clause)
    
    if not docai_df.empty:
        col1, col2 = st.columns(2)
        col1.metric("Total Doc AI queries", f"{docai_df['QUERY_COUNT'].sum():,}")
        col2.metric("Unique users", docai_df['USER_NAME'].nunique())
        
        st.dataframe(docai_df, use_container_width=True)
    else:
        st.info("No Document AI usage found in this period.", icon=":material/info:")

with tab5:
    st.subheader("Cortex Agents Usage")
    st.caption("Agentic AI workflows using Cortex Agents")
    
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
    
    agent_df = get_agent_usage(where_clause)
    
    if not agent_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Agent queries", f"{agent_df['QUERY_COUNT'].sum():,}")
        col2.metric("Unique users", agent_df['USER_NAME'].nunique())
        col3.metric("Total minutes", f"{agent_df['TOTAL_MINUTES'].sum():.1f}")
        
        fig = px.bar(agent_df, x='USER_NAME', y='QUERY_COUNT', title="Cortex Agent usage by user")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(agent_df, use_container_width=True)
        
        agent_details = execute_query(f"""
            SELECT 
                DATE_TRUNC('day', start_time) as day,
                COUNT(*) as calls,
                AVG(execution_time)/1000 as avg_sec
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND (
                UPPER(query_text) LIKE '%CORTEX.AGENT%'
                OR UPPER(query_text) LIKE '%CORTEX_AGENT%'
                OR UPPER(query_text) LIKE '%SNOWFLAKE.CORTEX.INVOKE_AGENT%'
            )
            GROUP BY day
            ORDER BY day
        """)
        
        if not agent_details.empty:
            fig = px.line(agent_details, x='DAY', y='CALLS', title="Agent usage over time")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No Cortex Agent usage found in this period.", icon=":material/info:")
        st.markdown("""
        **Cortex Agents** enable agentic AI workflows that can:
        - :material/psychology: Execute multi-step reasoning tasks
        - :material/build: Use tools and functions autonomously
        - :material/database: Query databases and APIs
        - :material/account_tree: Orchestrate complex workflows
        """)

with tab6:
    st.subheader("Snowflake Intelligence Usage")
    st.caption("Natural language queries via Snowflake Intelligence (Ask Copilot)")
    
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
    
    intel_df = get_intelligence_usage(where_clause)
    
    if not intel_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Intelligence queries", f"{intel_df['QUERY_COUNT'].sum():,}")
        col2.metric("Unique users", intel_df['USER_NAME'].nunique())
        col3.metric("Total minutes", f"{intel_df['TOTAL_MINUTES'].sum():.1f}")
        
        fig = px.bar(intel_df, x='USER_NAME', y='QUERY_COUNT', title="Snowflake Intelligence usage by user")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(intel_df, use_container_width=True)
        
        intel_trend = execute_query(f"""
            SELECT 
                DATE_TRUNC('day', start_time) as day,
                COUNT(*) as queries,
                COUNT(DISTINCT user_name) as unique_users
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE {where_clause}
            AND execution_status = 'SUCCESS'
            AND (
                UPPER(query_text) LIKE '%SNOWFLAKE_INTELLIGENCE%'
                OR UPPER(query_text) LIKE '%ASK_COPILOT%'
                OR UPPER(query_text) LIKE '%COPILOT%'
                OR query_tag LIKE '%intelligence%'
                OR UPPER(query_text) LIKE '%CORTEX.ASK%'
            )
            GROUP BY day
            ORDER BY day
        """)
        
        if not intel_trend.empty:
            fig = px.line(intel_trend, x='DAY', y='QUERIES', title="Intelligence usage over time")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No Snowflake Intelligence usage found in this period.", icon=":material/info:")
        st.markdown("""
        **Snowflake Intelligence** (Ask Copilot) enables:
        - :material/chat: Natural language data exploration
        - :material/code: AI-assisted SQL generation
        - :material/analytics: Conversational analytics
        - :material/explore: Data discovery and insights
        """)

st.subheader("AI Usage Summary")

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

summary_df = get_ai_summary(where_clause)

if not summary_df.empty:
    fig = px.area(summary_df, x='DAY', y='QUERY_COUNT', color='AI_FEATURE',
                 title="AI feature usage over time")
    st.plotly_chart(fig, use_container_width=True)
    
    totals = summary_df.groupby('AI_FEATURE')['QUERY_COUNT'].sum().reset_index()
    fig = px.pie(totals, values='QUERY_COUNT', names='AI_FEATURE', title="AI usage by feature")
    st.plotly_chart(fig, use_container_width=True)
    
    if st.button(":material/psychology: Get AI usage insights", key="ai_usage_insights"):
        with st.spinner("Analyzing AI usage patterns..."):
            summary_stats = summary_df.groupby('AI_FEATURE')['QUERY_COUNT'].sum().to_string()
            
            prompt = """You are a Snowflake AI/ML adoption expert analyzing ACCOUNT_USAGE.QUERY_HISTORY data.
Analyze these Cortex AI feature usage patterns.

RECOMMENDATIONS SHOULD INCLUDE:
1. Which AI features are most/least adopted and why that might be
2. Opportunities to expand AI usage (e.g., if using COMPLETE but not SUMMARIZE)
3. Cost optimization for AI workloads (batching, caching, model selection)
4. Patterns suggesting good or suboptimal AI implementation
5. Recommendations for AI governance and monitoring"""
            
            data_context = f"AI FEATURE USAGE SUMMARY:\n{summary_stats}"
            suggestions = get_ai_suggestions(prompt, data_context)
            st.markdown("### :material/lightbulb: AI Recommendations")
            st.markdown(suggestions)
else:
    st.info("No Cortex AI usage detected in this period.", icon=":material/info:")
    
    with st.container(border=True):
        st.markdown("### Cortex AI Features to Look For")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(":material/chat: **CORTEX.COMPLETE** - LLM text generation")
            st.markdown(":material/summarize: **CORTEX.SUMMARIZE** - Text summarization")
            st.markdown(":material/translate: **CORTEX.TRANSLATE** - Language translation")
            st.markdown(":material/sentiment_satisfied: **CORTEX.SENTIMENT** - Sentiment analysis")
        with col2:
            st.markdown(":material/question_answer: **CORTEX.EXTRACT_ANSWER** - Question answering")
            st.markdown(":material/search: **Cortex Search** - Vector/semantic search")
            st.markdown(":material/query_stats: **Cortex Analyst** - Natural language to SQL")
            st.markdown(":material/description: **Document AI** - PDF/image extraction")
