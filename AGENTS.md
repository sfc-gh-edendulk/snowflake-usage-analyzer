# AGENTS.md - Snowflake Usage Analyzer

## Project Overview

Multi-page Streamlit app that analyzes a Snowflake account's query history via `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Provides interactive dashboards with AI-powered optimization recommendations using `SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', ...)`.

**Target audience**: Any Snowflake user with IMPORTED PRIVILEGES on the SNOWFLAKE database who wants to understand and optimize their account usage.

## Architecture

### Connection
- Uses `st.connection("snowflake")` (Streamlit in Snowflake native connector)
- Sets `QUERY_TAG = 'USAGE_ANALYZER_APP'` on each session
- All queries target `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`

### Session State
- `filter_start` / `filter_end` - date range (default: last 30 days)
- `filter_warehouse` - optional warehouse filter (None = all)
- `filter_user` - optional user filter (None = all)
- All filters are set on the main page sidebar and shared across all pages

### Caching Strategy
- `@st.cache_resource` for the Snowflake connection (singleton)
- `@st.cache_data(ttl=300)` for all query results (5-minute TTL)
- Sidebar filter dropdowns use `ttl=600` (10 minutes)

### AI Recommendations
- Every page has "Get AI optimization suggestions" buttons
- Uses `SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', ...)` for LLM calls
- AI calls are never auto-triggered; always behind manual button clicks
- Prompts include domain-specific context (pruning expert, warehouse expert, etc.)
- Graceful failure: catches exceptions and shows error message

## File Structure

```
streamlit_app.py                    # Main page: overview metrics, date/filter sidebar
environment.yml                     # Dependencies: plotly, pandas (snowflake channel only)
setup.sql                           # Permission grants for ACCOUNT_USAGE access
README.md                           # User-facing documentation
pages/
  1_Query_Performance.py            # Pruning, spilling, cache, time breakdown
  2_Warehouse_Analysis.py           # Sizing, right-sizing, utilization, multi-cluster
  3_Users_Features.py               # User activity, roles, query types, feature adoption
  4_Errors_Recommendations.py       # Error analysis, long queries, optimization summary
  5_Cortex_AI_Usage.py              # LLM functions, Search, Analyst, DocAI, Agents, Intelligence
```

## Page Details

### streamlit_app.py (Home)
- Summary metrics: total queries, unique users, warehouses, databases
- Success rate, cache hit rate, total query hours, cloud services credits
- Spilling warning banner if > 10 GB local or > 1 GB remote
- Sidebar: date range presets (7/30/90/365 days + custom), warehouse filter, user filter
- `build_where_clause()` generates SQL WHERE from session state filters
- `execute_query()` and `get_connection()` are defined here and duplicated in each page

### 1_Query_Performance.py
**Tabs**: Pruning Efficiency | Spilling Analysis | Cache Utilization | Time Breakdown
- Pruning: histogram of pruning ratios, worst schemas, worst individual queries
- Spilling: % queries spilling, spill by warehouse (stacked bar), top spilling queries
- Cache: daily cache hit rate trend, cached vs total GB scanned, cache by warehouse
- Time: pie chart of compilation/queue/execution/blocked hours, queue time by warehouse

### 2_Warehouse_Analysis.py
**Tabs**: Overview | Right-sizing | Utilization Patterns | Multi-cluster
- Overview: query hours by warehouse, scatter of count vs avg exec time
- Right-sizing: auto-recommendations (UPSIZE/DOWNSIZE/REVIEW/OK), exec time distribution by size
- Utilization: heatmap (hour x day-of-week), daily volume for top 5 warehouses
- Multi-cluster: cluster distribution, candidates based on queue overload time > 60s

### 3_Users_Features.py
**Tabs**: User Activity | Role Analysis | Query Types | Feature Adoption
- Users: top users by hours, scatter of query count vs data scanned, daily active users
- Roles: pie chart of query distribution, users per role, role-warehouse heatmap matrix
- Query types: pie charts (count and time), weekly trends (area chart)
- Features: adoption rates for external functions, data transfer, query tags; database usage pie

### 4_Errors_Recommendations.py
**Tabs**: Error Analysis | Long Running Queries | Optimization Summary
- Errors: failure rate, top error codes bar chart, error trends (area), failed queries by user
- Long queries: configurable threshold slider (1-60 min), box plot by WH size, scatter of data scanned vs time
- Optimization: auto-generated recommendations engine checking spill (>10GB), queue (>30min), pruning (>70%), cache (<30%), query tags (<20%)

### 5_Cortex_AI_Usage.py
**Tabs**: LLM Functions | Cortex Search | Cortex Analyst | Document AI | Cortex Agents | Snowflake Intelligence
- Detects AI usage via `LIKE` patterns on `query_text` (e.g., `%SNOWFLAKE.CORTEX.COMPLETE%`)
- Summary: area chart of all AI features over time, pie chart of feature distribution
- Each tab shows user breakdown, query counts, and trends

## Patterns & Conventions

### Common Functions (duplicated across pages)
Each page file independently defines:
- `get_connection()` - cached Snowflake connection
- `execute_query(query)` - runs SQL via connection
- `get_ai_suggestions(prompt, data_context)` - Cortex COMPLETE call
- `build_where_clause(start, end)` - WHERE clause from session state
- `to_pandas_native(df)` - `pd.DataFrame(df.to_dict())` conversion

### SQL Patterns
- All queries use `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- WHERE clauses always include date range; optionally warehouse/user filters
- `execution_status = 'SUCCESS'` for most analysis queries
- Results typically `LIMIT 50-500` for display
- Metrics use `/1e9` (GB), `/1e12` (TB), `/1e15` (PB) conversions
- Time conversions: `/1000` (ms to sec), `/1000/60` (to min), `/1000/60/60` (to hours)

### UI Patterns
- Material icons throughout: `:material/icon_name:`
- `st.tabs()` for sub-navigation within pages
- `st.columns()` for metric rows and side-by-side charts
- `st.expander()` for detailed data tables
- `st.button()` with spinner for AI recommendations (never auto-triggered)
- `st.dataframe()` with `use_container_width=True` for all tables
- Plotly Express (`px`) for all charts: bar, line, pie, scatter, area, histogram, imshow

### Dependencies
- `plotly` - all visualizations
- `pandas` - data manipulation
- Both from `snowflake` conda channel only (no conda-forge)

## Deployment

### Snowflake (Streamlit in Snowflake)
```bash
snow streamlit deploy --database APPS --schema STREAMLIT
```
Or via SQL in `setup.sql`.

### Prerequisites
- Role with `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`
- Cortex LLM access for AI recommendations (optional, degrades gracefully)
- ACCOUNT_USAGE has up to 45 min latency, 365 days retention

## Development Notes

### Known Limitations
- No `st.set_page_config()` call (SiS incompatible)
- Helper functions are duplicated across pages rather than shared via a utils module
- AI suggestions use string interpolation with `.replace("'", "''")`; not parameterized
- Filter state depends on visiting the main page first (`st.stop()` on pages if missing)

### Potential Improvements
- Extract shared functions to a `utils.py` module
- Add warehouse credit consumption analysis (requires `WAREHOUSE_METERING_HISTORY`)
- Add storage analysis page (requires `STORAGE_USAGE`)
- Parameterize SQL queries to prevent injection edge cases
- Add data export functionality (CSV download)
