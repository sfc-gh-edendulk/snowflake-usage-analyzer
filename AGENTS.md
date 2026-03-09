# AGENTS.md - Snowflake Usage Analyzer

## Project Overview

Multi-page Streamlit app that analyzes a Snowflake account's query history via `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Provides interactive dashboards with AI-powered optimization recommendations using `SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', ...)`.

**Target audience**: Any Snowflake user with IMPORTED PRIVILEGES on the SNOWFLAKE database who wants to understand and optimize their account usage.

## Architecture

### Navigation
- Uses `st.navigation()` / `st.Page()` (modern API)
- `streamlit_app.py` is the single entry point that runs before every page
- Sidebar filters (date range, warehouse, user) are rendered in the entry point and shared via `st.session_state`
- Summary stats are pre-loaded into `st.session_state["summary_stats"]` before any page runs

### Shared Modules
- **`utils.py`** - Connection, query execution, AI suggestions, WHERE clause builder, pandas conversion
- **`data.py`** - All `@st.cache_data` query functions (shared cache across pages)

### Connection
- Uses `st.connection("snowflake")` (Streamlit in Snowflake native connector)
- Sets `QUERY_TAG = 'USAGE_ANALYZER_APP'` on each session
- All queries target `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`

### Session State
- `filter_start` / `filter_end` - date range (default: last 30 days)
- `filter_warehouse` - optional warehouse filter (None = all)
- `filter_user` - optional user filter (None = all)
- `summary_stats` - pre-loaded summary DataFrame (home page reads from this)
- All filters are initialized with `setdefault()` in the entry point, so no page ever sees missing state

### Caching Strategy
- `@st.cache_resource` for the Snowflake connection (singleton, in `utils.py`)
- `@st.cache_data(ttl=300)` for all query results (5-minute TTL, in `data.py`)
- Sidebar filter dropdowns use `ttl=600` (10 minutes)
- Cache key includes the `where` clause string, so same filters = same cache hit across pages

### AI Recommendations
- Every page has "Get AI optimization suggestions" buttons
- Uses `SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', ...)` for LLM calls
- AI calls are never auto-triggered; always behind manual button clicks
- Prompts include domain-specific context (pruning expert, warehouse expert, etc.)
- Graceful failure: catches exceptions and shows error message

## File Structure

```
streamlit_app.py                    # Entry point: sidebar filters + st.navigation() + summary pre-load
snowflake.yml                       # Snow CLI deployment config
utils.py                            # Shared: connection, execute_query, ai_suggestions, build_where_clause, to_pandas_native
data.py                             # Shared: all @st.cache_data query functions
environment.yml                     # Dependencies: plotly, pandas (snowflake channel only)
setup.sql                           # Permission grants for ACCOUNT_USAGE access
README.md                           # User-facing documentation
AGENTS.md                           # This file
app_pages/
  home.py                           # Overview metrics from pre-loaded summary
  query_performance.py              # Pruning, spilling, cache, time breakdown
  warehouse_analysis.py             # Sizing, right-sizing, utilization, multi-cluster
  users_features.py                 # User activity, roles, query types, feature adoption
  errors_recommendations.py         # Error analysis, long queries, optimization summary
  cortex_ai_usage.py                # LLM functions, Search, Analyst, DocAI, Agents, Intelligence
```

## Page Details

### streamlit_app.py (Entry Point)
- Initializes all session state defaults
- Renders shared sidebar: date range presets (7/30/90/365 days + custom), warehouse filter, user filter
- Calls `load_summary_stats()` and stores result in `st.session_state["summary_stats"]`
- Defines `st.navigation()` with all 6 pages, calls `page.run()`

### app_pages/home.py (Overview)
- Reads from `st.session_state["summary_stats"]` (no additional queries)
- Summary metrics: total queries, unique users, warehouses, databases
- Success rate, cache hit rate, total query hours, cloud services credits
- Spilling warning banner if > 10 GB local or > 1 GB remote

### app_pages/query_performance.py
**Segmented control**: Pruning Efficiency | Spilling Analysis | Cache Utilization | Time Breakdown
- Only the selected section's queries execute (performance optimization)
- Pruning: histogram of pruning ratios, worst schemas, worst individual queries
- Spilling: % queries spilling, spill by warehouse (stacked bar), top spilling queries
- Cache: daily cache hit rate trend, cached vs total GB scanned, cache by warehouse
- Time: pie chart of compilation/queue/execution/blocked hours, queue time by warehouse

### app_pages/warehouse_analysis.py
**Segmented control**: Overview | Right-sizing | Utilization Patterns | Multi-cluster
- Overview: query hours by warehouse, scatter of count vs avg exec time
- Right-sizing: auto-recommendations (UPSIZE/DOWNSIZE/REVIEW/OK), exec time distribution by size
- Utilization: heatmap (hour x day-of-week), daily volume for top 5 warehouses
- Multi-cluster: cluster distribution, candidates based on queue overload time > 60s

### app_pages/users_features.py
**Segmented control**: User Activity | Role Analysis | Query Types | Feature Adoption
- Users: top users by hours, scatter of query count vs data scanned, daily active users
- Roles: pie chart of query distribution, users per role, role-warehouse heatmap matrix
- Query types: pie charts (count and time), weekly trends (area chart)
- Features: adoption rates for external functions, data transfer, query tags; database usage pie

### app_pages/errors_recommendations.py
**Segmented control**: Error Analysis | Long Running Queries | Optimization Summary
- Errors: failure rate, top error codes bar chart, error trends (area), failed queries by user
- Long queries: configurable threshold slider (1-60 min), box plot by WH size, scatter of data scanned vs time
- Optimization: auto-generated recommendations engine checking spill (>10GB), queue (>30min), pruning (>70%), cache (<30%), query tags (<20%)

### app_pages/cortex_ai_usage.py
**Segmented control**: LLM Functions | Cortex Search | Cortex Analyst | Document AI | Cortex Agents | Snowflake Intelligence
- Detects AI usage via `LIKE` patterns on `query_text` (e.g., `%SNOWFLAKE.CORTEX.COMPLETE%`)
- Summary section (always visible): area chart of all AI features over time, pie chart of feature distribution
- Each section shows user breakdown, query counts, and trends

## Patterns & Conventions

### Shared Functions (in utils.py)
- `get_connection()` - `@st.cache_resource`, returns `st.connection("snowflake")`
- `execute_query(query)` - runs SQL via cached connection
- `get_ai_suggestions(prompt, data_context)` - Cortex COMPLETE call with graceful error handling
- `build_where_clause()` - no parameters, reads directly from `st.session_state`
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
- `st.segmented_control()` for sub-navigation within pages (only selected section renders/queries)
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

### After Every Significant Change
Always commit, push to GitHub, and redeploy to Snowflake:

```bash
# 1. Commit and push
git add -A && git commit -m "description of changes" && git push

# 2. Deploy to Snowflake (recreates the Streamlit app)
snow streamlit deploy --replace --connection SFCOGSOPS_EDENDULK
```

### Snowflake App Location
- **Database**: `TEMP`
- **Schema**: `EDENDULK`
- **App name**: `CUSTOMER_USAGE_APP`
- **Warehouse**: `SE_WH`
- **Config**: `snowflake.yml` (Snow CLI v2 definition)

### Prerequisites
- Role with `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`
- Cortex LLM access for AI recommendations (optional, degrades gracefully)
- ACCOUNT_USAGE has up to 45 min latency, 365 days retention

## Development Notes

### Known Limitations
- AI suggestions use string interpolation with `.replace("'", "''")`; not parameterized
- `st.set_page_config()` is called in entry point (works locally, may need removal for SiS)

### Potential Improvements
- Add warehouse credit consumption analysis (requires `WAREHOUSE_METERING_HISTORY`)
- Add storage analysis page (requires `STORAGE_USAGE`)
- Parameterize SQL queries to prevent injection edge cases
- Add data export functionality (CSV download)
