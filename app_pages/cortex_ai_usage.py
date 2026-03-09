import streamlit as st
import plotly.express as px
from utils import get_ai_suggestions
from data import (
    get_ai_adoption_summary, get_cortex_llm_daily, get_cortex_llm_by_function,
    get_credit_breakdown
)

sfdc_id = st.session_state.get("salesforce_account_id")
customer = st.session_state.get("customer_name", "Unknown")

if not sfdc_id:
    st.error("No SFDC account ID available. Return to Overview and select a customer.")
    st.stop()

st.title(f":material/smart_toy: Cortex AI & ML Usage — {customer}")

selected = st.segmented_control(
    "Analysis",
    [":material/chat: LLM Functions", ":material/dashboard: AI Adoption Overview",
     ":material/payments: Credit Breakdown"],
    default=":material/chat: LLM Functions",
)

if selected == ":material/chat: LLM Functions":
    st.subheader("Cortex LLM Usage")

    llm_daily = get_cortex_llm_daily(sfdc_id)
    llm_funcs = get_cortex_llm_by_function(sfdc_id)

    if llm_daily is not None and not llm_daily.empty:
        total_credits = llm_daily['CORTEX_LLM_CREDITS'].sum()
        days_active = len(llm_daily[llm_daily['CORTEX_LLM_CREDITS'] > 0])

        col1, col2 = st.columns(2)
        col1.metric("Total LLM Credits (L90D)", f"{total_credits:,.2f}")
        col2.metric("Active Days", days_active)

        fig = px.area(llm_daily, x='GENERAL_DATE', y='CORTEX_LLM_CREDITS',
                      title="Daily Cortex LLM Credit Consumption")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No Cortex LLM usage found in the last 90 days.", icon=":material/info:")

    if llm_funcs is not None and not llm_funcs.empty:
        st.markdown("**Usage by LLM Function**")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(llm_funcs, values='CREDITS', names='LLM_FUNCTION',
                        title="Credits by LLM Function")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(llm_funcs, x='LLM_FUNCTION', y='CREDITS',
                        title="Credits per Function", color='DAYS_USED')
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(llm_funcs, use_container_width=True)

    if st.button(":material/psychology: Get AI adoption insights", key="llm_ai"):
        with st.spinner("Analyzing LLM usage patterns..."):
            funcs_str = llm_funcs.to_string() if llm_funcs is not None and not llm_funcs.empty else "No function data"
            daily_str = f"Total credits: {total_credits:.2f}, Active days: {days_active}" if llm_daily is not None and not llm_daily.empty else "No daily data"

            prompt = """You are a Snowflake AI/ML adoption expert analyzing a customer's Cortex LLM usage.

RECOMMENDATIONS SHOULD INCLUDE:
1. Which LLM functions are most/least used and what that indicates
2. Opportunities to expand AI usage (e.g., if using COMPLETE but not SUMMARIZE)
3. Cost optimization for AI workloads (batching, caching, model selection)
4. Patterns suggesting good or suboptimal AI implementation
5. Recommendations for next steps in AI adoption"""

            data_context = f"LLM FUNCTION BREAKDOWN:\n{funcs_str}\n\nOVERALL:\n{daily_str}"
            suggestions = get_ai_suggestions(prompt, data_context)
            st.markdown("### :material/lightbulb: AI Recommendations")
            st.markdown(suggestions)

elif selected == ":material/dashboard: AI Adoption Overview":
    st.subheader("AI/ML Feature Adoption Summary")

    ai_df = get_ai_adoption_summary(sfdc_id)
    if ai_df is not None and not ai_df.empty:
        active = ai_df[ai_df['IS_USING'] == 'Yes']
        inactive = ai_df[ai_df['IS_USING'] == 'No']

        col1, col2 = st.columns(2)
        col1.metric("Features in Use", len(active))
        col2.metric("Total AI Credits (L30D)", f"{ai_df['TOTAL_CREDITS'].sum():,.2f}")

        st.markdown("**Active Features**")
        if not active.empty:
            fig = px.bar(active, x='FEATURE', y='TOTAL_CREDITS',
                        title="AI Feature Credits (Last 30 Days)", color='FEATURE')
            st.plotly_chart(fig, use_container_width=True)

        for _, row in ai_df.iterrows():
            icon = ":material/check_circle:" if row['IS_USING'] == 'Yes' else ":material/cancel:"
            credits = f"{row['TOTAL_CREDITS']:,.2f} credits" if row['TOTAL_CREDITS'] > 0 else "No usage"
            st.markdown(f"{icon} **{row['FEATURE']}** — {credits}")

        st.divider()
        from data import get_platform_adoption_summary
        platform_df = get_platform_adoption_summary(sfdc_id)
        if platform_df is not None and not platform_df.empty:
            st.subheader("Platform Feature Adoption")
            for _, row in platform_df.iterrows():
                icon = ":material/check_circle:" if row['IS_USING'] == 'Yes' else ":material/cancel:"
                credits = f"{row['TOTAL_CREDITS']:,.2f} credits" if row['TOTAL_CREDITS'] > 0 else "No usage"
                st.markdown(f"{icon} **{row['FEATURE']}** — {credits}")
    else:
        st.info("No AI adoption data available.", icon=":material/info:")

elif selected == ":material/payments: Credit Breakdown":
    st.subheader("Monthly Credit Breakdown by Service")

    breakdown = get_credit_breakdown(sfdc_id)
    if breakdown is not None and not breakdown.empty:
        service_cols = [c for c in breakdown.columns if c not in ('MONTH', 'TOTAL')]
        fig = px.bar(
            breakdown, x='MONTH', y=service_cols,
            title="Monthly Credits by Service Type", barmode='stack'
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Monthly Totals**")
        st.dataframe(breakdown, use_container_width=True)

        if breakdown['AI_SERVICES'].sum() > 0:
            st.markdown("**AI Services Trend**")
            fig = px.line(breakdown, x='MONTH', y='AI_SERVICES',
                         title="AI Services Credits Over Time")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No credit breakdown data available.", icon=":material/info:")
