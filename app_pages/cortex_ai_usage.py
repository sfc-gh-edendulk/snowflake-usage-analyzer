import streamlit as st
import pandas as pd
import plotly.express as px
from utils import execute_query, get_ai_suggestions, build_where_clause
from data import (get_llm_usage, get_search_usage, get_analyst_usage, get_docai_usage,
                  get_agent_usage, get_intelligence_usage, get_ai_summary)

where_clause = build_where_clause()

st.title(":material/smart_toy: Cortex AI & LLM Usage")

selected = st.segmented_control(
    "Analysis",
    [":material/chat: LLM Functions", ":material/search: Cortex Search",
     ":material/query_stats: Cortex Analyst", ":material/description: Document AI",
     ":material/smart_toy: Cortex Agents", ":material/psychology: Snowflake Intelligence"],
    default=":material/chat: LLM Functions",
)

if selected == ":material/chat: LLM Functions":
    st.subheader("Cortex LLM Function Usage")
    st.caption("COMPLETE, SUMMARIZE, TRANSLATE, SENTIMENT, and other LLM functions")

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

elif selected == ":material/search: Cortex Search":
    st.subheader("Cortex Search Usage")

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

elif selected == ":material/query_stats: Cortex Analyst":
    st.subheader("Cortex Analyst Usage")
    st.caption("Text-to-SQL semantic model queries")

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

elif selected == ":material/description: Document AI":
    st.subheader("Document AI & Extraction")

    docai_df = get_docai_usage(where_clause)

    if not docai_df.empty:
        col1, col2 = st.columns(2)
        col1.metric("Total Doc AI queries", f"{docai_df['QUERY_COUNT'].sum():,}")
        col2.metric("Unique users", docai_df['USER_NAME'].nunique())

        st.dataframe(docai_df, use_container_width=True)
    else:
        st.info("No Document AI usage found in this period.", icon=":material/info:")

elif selected == ":material/smart_toy: Cortex Agents":
    st.subheader("Cortex Agents Usage")
    st.caption("Agentic AI workflows using Cortex Agents")

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

elif selected == ":material/psychology: Snowflake Intelligence":
    st.subheader("Snowflake Intelligence Usage")
    st.caption("Natural language queries via Snowflake Intelligence (Ask Copilot)")

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

st.divider()
st.subheader("AI Usage Summary")

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
