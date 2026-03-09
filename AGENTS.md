# AGENTS.md - Snowflake Customer Usage Analyzer

## Project Overview

Multi-page Streamlit app deployed on **Snowhouse** (SFCOGSOPS-SNOWHOUSE_AWS_US_WEST_2) that analyzes **customer** Snowflake accounts. An SE selects a customer, then the app queries internal Snowhouse data to show query performance, warehouse analysis, AI/ML adoption, consumption trends, and optimization recommendations.

**Target audience**: Snowflake Sales Engineers analyzing customer accounts.
**Deployed at**: `TEMP.EDENDULK.CUSTOMER_USAGE_APP`

## App Flow

1. **Intro/Customer Selection Page** — User enters or selects a customer name. No data is queried until a customer is chosen.
2. **Customer is resolved** — SFDC name → `SALESFORCE_ACCOUNT_ID` → Snowflake account(s) → deployment(s). Customer-specific query history views in `TEMP.EDENDULK` are discovered.
3. **Account Selection (multi-account customers)** — If customer has >1 Snowflake account, user selects which to include (checkboxes, all selected by default). Accounts come from `SALES.SE_REPORTING.ACCOUNT_TOOL_CREDITS_MONTHLY` which provides the internal `SNOWFLAKE_ACCOUNT_ID` (numeric, used in JOB_ETL_V) and `DEPLOYMENT`.
4. **View creation (if needed)** — If no `{CUSTOMER}_QUERY_HISTORY_V` exists, Overview page offers a "Create View" button. Uses selected accounts' `SNOWFLAKE_ACCOUNT_ID` + `DEPLOYMENT` to generate a `CREATE VIEW` over `SNOWHOUSE_IMPORT.<DEPLOYMENT>.JOB_ETL_V`.
5. **Dashboard pages load** — All pages query using the resolved customer context.

## Data Sources

### Tier 1: Per-Customer Query History Views (TEMP.EDENDULK)

Pre-created views that expose a customer's query-level data. These mimic `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` but read from internal `SNOWHOUSE_IMPORT.<DEPLOYMENT>` DPO tables.

**Naming convention**: `{CUSTOMER}_QUERY_HISTORY_V` (and optionally `_WAREHOUSE_METERING_HISTORY_V`, `_STORAGE_USAGE_V`, `_SESSIONS_V`, `_WAREHOUSE_EVENTS_HISTORY_V`)

**Columns** (57 columns, matching ACCOUNT_USAGE.QUERY_HISTORY subset):
`QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, WAREHOUSE_NAME, WAREHOUSE_SIZE, WAREHOUSE_TYPE, CLUSTER_NUMBER, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, BYTES_READ_FROM_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, BYTES_DELETED, PARTITIONS_SCANNED, PARTITIONS_TOTAL, BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE, BYTES_SENT_OVER_THE_NETWORK, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME, QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_CLOUD, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES, INBOUND_DATA_TRANSFER_CLOUD, INBOUND_DATA_TRANSFER_REGION, INBOUND_DATA_TRANSFER_BYTES, LIST_EXTERNAL_FILES_TIME, CREDITS_USED_CLOUD_SERVICES, RELEASE_VERSION, EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, QUERY_LOAD_PERCENT, IS_CLIENT_GENERATED_STATEMENT`

**Used by pages**: Query Performance, Warehouse Analysis, Users & Features, Errors & Recommendations

**Important**: Not all customers have all view types. Discover what exists at runtime:
```sql
SELECT TABLE_NAME FROM TEMP.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'EDENDULK' AND TABLE_NAME LIKE '{CUSTOMER}%'
ORDER BY TABLE_NAME
```

### Tier 2: Account 360 / Finance Tables (consumption, revenue, product adoption)

These are Snowhouse-wide tables keyed by `SALESFORCE_ACCOUNT_ID`. They provide credit/revenue data and per-product adoption metrics.

#### Customer Lookup
```sql
-- Find SALESFORCE_ACCOUNT_ID from customer name
SELECT ID as SALESFORCE_ACCOUNT_ID, NAME, INDUSTRY, TYPE, BILLING_COUNTRY
FROM FIVETRAN.SALESFORCE.ACCOUNT
WHERE LOWER(NAME) LIKE LOWER('%<search_term>%') AND IS_DELETED = FALSE
```

#### Snowflake Accounts Under SFDC Account
```sql
-- Map SFDC → Snowflake accounts with INTERNAL account_id + deployment
-- SNOWFLAKE_ACCOUNT_ID here is the INTERNAL numeric ID (e.g. 655618) used in JOB_ETL_V.account_id
-- DEPLOYMENT is lowercase; UPPER(DEPLOYMENT) = schema name in SNOWHOUSE_IMPORT
SELECT SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_ACCOUNT_ID, UPPER(DEPLOYMENT) as DEPLOYMENT,
       SUM(CREDITS) as TOTAL_CREDITS_L90D
FROM SALES.SE_REPORTING.ACCOUNT_TOOL_CREDITS_MONTHLY
WHERE SALESFORCE_ACCOUNT_ID = '<id>'
  AND USAGE_MONTH >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE()))
GROUP BY SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_ACCOUNT_ID, DEPLOYMENT
ORDER BY TOTAL_CREDITS_L90D DESC
```

**Important mapping note**: `FINANCE.CUSTOMER.CONTRACT_REVENUE` does NOT contain the internal numeric `SNOWFLAKE_ACCOUNT_ID` needed for JOB_ETL_V. Use `SALES.SE_REPORTING.ACCOUNT_TOOL_CREDITS_MONTHLY` instead.

#### Revenue & Consumption

| Table | Key Columns | Use For |
|-------|-------------|---------|
| `FINANCE.CUSTOMER.CONTRACT_REVENUE` | `GENERAL_DATE, SALESFORCE_ACCOUNT_ID, COMPUTE_CREDITS, CLOUD_SERVICES_CREDITS, AI_SERVICES_CREDITS, TOTAL_CREDITS, GAAP_REVENUE` | Revenue trends, credit breakdown by service type. Filter `IS_FUTURE = FALSE`. |
| `FINANCE.SALES_FINANCE.WORKLOAD_AND_FEATURES` | `MONTH, SALESFORCE_ACCOUNT_ID, DATA_LAKE_CREDITS, DATA_WAREHOUSE_CREDITS, DATA_ENGINEERING_CREDITS, DATA_SCIENCE_CREDITS` | Workload distribution |
| `FINANCE.CUSTOMER.USAGE_DAILY` | `USAGE_DATE, SNOWFLAKE_ACCOUNT_ID, SNOWFLAKE_DEPLOYMENT, COMPUTE_CREDITS, AI_SERVICES_CREDITS, TOTAL_CREDITS` | Daily per-account usage |
| `FINANCE.CUSTOMER.WAREHOUSE_COMPUTE` | `USAGE_DATE, WAREHOUSE_NAME, CREDITS` | Per-warehouse credit consumption |

#### AI/ML Product Adoption (FINANCE.CUSTOMER.PRODUCT_*_ACCOUNT_REVENUE)

All keyed by `SALESFORCE_ACCOUNT_ID` + `GENERAL_DATE`. Each has `IS_<PRODUCT>_USAGE` (boolean) and `<PRODUCT>_CREDITS` (number).

| Table | Product | Credits Column |
|-------|---------|----------------|
| `PRODUCT_CORTEX_LLM_ACCOUNT_REVENUE` | Cortex LLM | `CORTEX_LLM_CREDITS` |
| `PRODUCT_CORTEX_LLM_FUNCTION_ACCOUNT_REVENUE` | Cortex LLM by Function | `LLM_FUNCTION_CREDITS`, `LLM_FUNCTION` |
| `PRODUCT_CORTEX_ML_ACCOUNT_REVENUE` | Cortex ML | `CORTEX_ML_CREDITS` |
| `PRODUCT_CORTEX_ANALYST_ACCOUNT_REVENUE` | Cortex Analyst | `CORTEX_ANALYST_CREDITS` |
| `PRODUCT_CORTEX_SEARCH_ACCOUNT_REVENUE` | Cortex Search | `CORTEX_SEARCH_CREDITS` |
| `PRODUCT_DOCUMENT_AI_ACCOUNT_REVENUE` | Document AI | `DOCUMENT_AI_CREDITS` |
| `PRODUCT_SPCS_ACCOUNT_REVENUE` | SPCS | `SPCS_GPU_CREDITS`, `SPCS_NON_GPU_CREDITS` |
| `PRODUCT_SNOWML_ACCOUNT_REVENUE` | SnowML | `SNOWML_CREDITS` |
| `PRODUCT_SNOWPARK_ACCOUNT_REVENUE` | Snowpark | `SNOWPARK_CREDITS` |
| `PRODUCT_DYNAMIC_TABLES_ACCOUNT_REVENUE` | Dynamic Tables | `DYNAMIC_TABLES_CREDITS` |
| `PRODUCT_ICEBERG_ACCOUNT_REVENUE` | Iceberg | `ICEBERG_TOTAL_RATED_CONSUMPTION` |
| `PRODUCT_NOTEBOOK_ACCOUNT_REVENUE` | Notebooks | `NOTEBOOK_CREDITS` |
| `PRODUCT_SIS_ACCOUNT_REVENUE` | Streamlit | `SIS_CREDITS` |
| `PRODUCT_CONNECTORS_ACCOUNT_REVENUE` | Connectors | `CONNECTOR_CREDITS` |
| `PRODUCT_DATA_CLEAN_ROOMS_ACCOUNT_REVENUE` | Data Clean Rooms | `DATA_CLEAN_ROOM_CREDITS` |
| `PRODUCT_SNOWFLAKE_INTELLIGENCE_ACCOUNT_REVENUE` | Snowflake Intelligence | (check columns) |
| `PRODUCT_CORTEX_LLM_FINE_TUNING_ACCOUNT_REVENUE` | LLM Fine Tuning | (check columns) |

#### Tools & Connectors

| Table | Key Columns | Use For |
|-------|-------------|---------|
| `SALES.SE_REPORTING.ACCOUNT_TOOL_CREDITS_MONTHLY` | `SALESFORCE_ACCOUNT_ID, TOOL, CLIENT, TOOL_CATEGORY, CREDITS` | Monthly tool/connector credits |
| `SNOWSCIENCE.JOB_ANALYTICS.JOB_CREDITS_DAY_TOOL_FACT` | `ACCOUNT_ID, TOOL, CLIENT, TOTAL_CREDITS, DS` | Daily tool credits (uses Snowflake ACCOUNT_ID, not SFDC ID — map via CONTRACT_REVENUE) |

### Tier 3: Snowhouse Import (raw DPO, deployment-specific)

Low-level internal tables. Used when customer query history views don't exist or for warehouse-level deep dives.

| Table | Use For |
|-------|---------|
| `SNOWHOUSE_IMPORT.<DEPLOYMENT>.JOB_ETL_V` | Raw job/query data with DPO stats (spilling, pruning, cache, CQE breakdown) |
| `SNOWHOUSE_IMPORT.<DEPLOYMENT>.WAREHOUSE_ETL_V` | Warehouse configurations (size, cluster, auto-suspend, QAS) |
| `SNOWHOUSE_IMPORT.<DEPLOYMENT>.WAREHOUSE_LOG_ETL_V` | Suspend/resume events |
| `SNOWHOUSE_IMPORT.<DEPLOYMENT>.JOB_RAW_V` | Lowest-level job data (QAS eligibility analysis) |
| `SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V` | Account metadata |

**Note**: `<DEPLOYMENT>` must be resolved per customer (e.g., `PROD1`, `VA`, `EUFRANKFURT`). Get it from `SALES.SE_REPORTING.ACCOUNT_TOOL_CREDITS_MONTHLY.DEPLOYMENT` (lowercase; UPPER() = schema name). The `SNOWFLAKE_ACCOUNT_ID` from that table is the internal numeric ID used in `JOB_ETL_V.account_id`.

#### View Creation Template
The query history view is generated from `utils.build_query_history_view_sql()`. Key parameters:
- `deployment` → `SNOWHOUSE_IMPORT.<DEPLOYMENT>.JOB_ETL_V`
- `account_id` → `JOB_ETL_V.account_id = <SNOWFLAKE_ACCOUNT_ID>`
- `date_start/date_end` → `created_on >= '<start>' AND created_on <= '<end>'`
- Multi-account: inner subqueries are UNION ALL'd before the column transformations
- View is created in `TEMP.EDENDULK.{VIEW_NAME}`

### Tier 4: Support & Sales

| Table | Use For |
|-------|---------|
| `SUPPORT.OUTBOUND_SHARING.COR_SFDC_CASE_CONSOLIDATED` | Support cases (JSON: `CASE_STRUCTURED:metadata:case:<field>`) |
| `FINANCE.SALES_FINANCE.SALESFORCE_OPPORTUNITIES` | Opportunity pipeline |
| `SNOWHOUSE.SALES.SALESFORCE_ACCOUNT_OWNERSHIP` | Account owner, RD, RVP, GVP, territory |

## Architecture

### Navigation
- Uses `st.navigation()` / `st.Page()` (modern API)
- `streamlit_app.py` is the single entry point that runs before every page
- Sidebar filters (date range, warehouse, user) are rendered in the entry point and shared via `st.session_state`
- Summary stats are pre-loaded into `st.session_state["summary_stats"]` before any page runs

### Shared Modules
- **`utils.py`** — Connection, query execution, AI suggestions, WHERE clause builder, pandas conversion
- **`data.py`** — All `@st.cache_data` query functions (shared cache across pages)

### Session State
- `customer_name` — Selected customer (SFDC name)
- `salesforce_account_id` — Resolved SFDC account ID
- `snowflake_accounts` — DataFrame of all Snowflake accounts under SFDC (from ACCOUNT_TOOL_CREDITS_MONTHLY)
- `selected_accounts` — DataFrame of user-selected accounts (subset of snowflake_accounts)
- `accounts_confirmed` — Whether user has confirmed account selection (or it was auto-confirmed)
- `query_history_view` — Fully qualified name of the customer's query history view (e.g., `TEMP.EDENDULK.AVIV_QUERY_HISTORY_V`)
- `available_views` — Dict of which view types exist for this customer
- `filter_start` / `filter_end` — Date range (default: last 30 days)
- `filter_warehouse` — Optional warehouse filter
- `filter_user` — Optional user filter
- `summary_stats` — Pre-loaded summary DataFrame

### Caching Strategy
- `@st.cache_resource` for the Snowflake connection (singleton, in `utils.py`)
- `@st.cache_data(ttl=300)` for all query results (5-minute TTL, in `data.py`)
- Sidebar filter dropdowns use `ttl=600` (10 minutes)
- Cache key includes the `where` clause string and customer context

### AI Recommendations
- Every page has "Get AI optimization suggestions" buttons
- Uses `SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', ...)` for LLM calls
- AI calls are never auto-triggered; always behind manual button clicks
- Graceful failure: catches exceptions and shows error message

## File Structure

```
streamlit_app.py                    # Entry point: customer selection gate, sidebar filters, st.navigation(), summary pre-load
snowflake.yml                       # Snow CLI deployment config
utils.py                            # Shared: connection, execute_query, ai_suggestions, build_where_clause, to_pandas_native
data.py                             # Shared: all @st.cache_data query functions
environment.yml                     # Dependencies: plotly, pandas (snowflake channel only)
setup.sql                           # Permission grants
README.md                           # User-facing documentation
AGENTS.md                           # This file
app_pages/
  home.py                           # Overview metrics + consumption summary
  query_performance.py              # Pruning, spilling, cache, time breakdown (from query_history view)
  warehouse_analysis.py             # Sizing, right-sizing, utilization, multi-cluster (from query_history view)
  users_features.py                 # User activity, roles, query types, feature adoption (from query_history view)
  errors_recommendations.py         # Error analysis, long queries, optimization summary (from query_history view)
  cortex_ai_usage.py                # AI/ML product adoption, credits, trends (from FINANCE.CUSTOMER.PRODUCT_* tables)
```

## Page Data Source Mapping

| Page | Primary Data Source | Fallback |
|------|-------------------|----------|
| Home / Overview | `CONTRACT_REVENUE` (consumption) + `QUERY_HISTORY_V` (summary stats) | — |
| Query Performance | `{CUSTOMER}_QUERY_HISTORY_V` | None (page disabled if view missing) |
| Warehouse Analysis | `{CUSTOMER}_QUERY_HISTORY_V` + `{CUSTOMER}_WAREHOUSE_METERING_HISTORY_V` | Query history only |
| Users & Features | `{CUSTOMER}_QUERY_HISTORY_V` | None |
| Errors & Recommendations | `{CUSTOMER}_QUERY_HISTORY_V` | None |
| Cortex AI Usage | `FINANCE.CUSTOMER.PRODUCT_CORTEX_*` tables | Query history LIKE patterns as supplement |

## Patterns & Conventions

### Shared Functions (in utils.py)
- `get_connection()` — `@st.cache_resource`, returns `st.connection("snowflake")`
- `execute_query(query)` — Runs SQL via cached connection
- `get_ai_suggestions(prompt, data_context)` — Cortex COMPLETE call with graceful error handling
- `build_where_clause()` — Reads from `st.session_state` (dates, warehouse, user filters)
- `to_pandas_native(df)` — `pd.DataFrame(df.to_dict())` conversion
- `build_query_history_view_sql(view_name, accounts_df, date_start, date_end)` — Generates CREATE VIEW SQL from JOB_ETL_V template. Supports multi-account/multi-deployment via UNION ALL.
- `create_query_history_view(view_name, accounts_df, date_start, date_end)` — Executes the CREATE VIEW. Returns `(success, error)` tuple.

### SQL Patterns
- Query history pages: swap `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` → `{st.session_state.query_history_view}`
- AI/ML pages: query `FINANCE.CUSTOMER.PRODUCT_*` tables filtered by `SALESFORCE_ACCOUNT_ID`
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
- `plotly` — All visualizations
- `pandas` — Data manipulation
- Both from `snowflake` conda channel only (no conda-forge)

## Deployment

### After Every Significant Change
Always commit, push to GitHub, and redeploy to Snowflake:

```bash
# 1. Commit and push
git add -A && git commit -m "description of changes" && git push

# 2. Upload files to stage (snow CLI deploy fails at CREATE step due to v3.3.0 bug)
snow streamlit deploy --replace --connection SFCOGSOPS_EDENDULK

# 3. If step 2 fails at CREATE STREAMLIT, files are already on stage. Manually recreate:
#    (run in Snowflake with SALES_ENGINEER role)
#    CREATE OR REPLACE STREAMLIT TEMP.EDENDULK.CUSTOMER_USAGE_APP
#      FROM '@TEMP.EDENDULK.streamlit/CUSTOMER_USAGE_APP'
#      MAIN_FILE = 'streamlit_app.py';
#    ALTER STREAMLIT TEMP.EDENDULK.CUSTOMER_USAGE_APP SET QUERY_WAREHOUSE = 'SE_WH' TITLE = 'Snowflake Usage Analyzer';
```

**Note**: `snow streamlit deploy` may also put `app_pages/` under `pages/` on stage. If so, manually upload:
```bash
for f in app_pages/*.py; do
  snow stage copy "$f" "@TEMP.EDENDULK.streamlit/CUSTOMER_USAGE_APP/app_pages/" --connection SFCOGSOPS_EDENDULK --database TEMP --schema EDENDULK --overwrite
done
```

### Snowflake App Location
- **Database**: `TEMP`
- **Schema**: `EDENDULK`
- **App name**: `CUSTOMER_USAGE_APP`
- **Warehouse**: `SE_WH`
- **Role**: `SALES_ENGINEER`
- **Config**: `snowflake.yml` (Snow CLI definition_version 2, use `artifacts` not `additional_source_files`)
- **Stage**: `@TEMP.EDENDULK.streamlit/CUSTOMER_USAGE_APP/`
- **Connection**: `SFCOGSOPS_EDENDULK`
- **GitHub**: https://github.com/sfc-gh-edendulk/snowflake-usage-analyzer

### Existing Customer Views (TEMP.EDENDULK)
Customer views follow pattern `{CUSTOMER}_{VIEW_TYPE}`. Current customers with views:
ALGOLIA, AMERSPORTS, AVIV, AVIVGROUP, BIG_FISH_GAME, C10, DATA, DCDATACLOUD, DIXSTONE, GY84918, KRYS_GROUP, MIRAE_ASSET_GLOBAL_INVESTMENTS, NORD_SECURITY, PERENCO_GROUP, PRI_MED, PROD, ROADRUNNER, SALOMON, SNCF_RETAIL, SOURCES_ALMA, TRANSATEL_CAP

Not all have all view types. Most have only `QUERY_HISTORY_V`. Some (AMERSPORTS, AVIV, DCDATACLOUD, DIXSTONE, PERENCO_GROUP, PROD, SALOMON) have additional warehouse/storage/sessions views.

## Development Notes

### Known Limitations
- AI suggestions use string interpolation with `.replace("'", "''")`; not parameterized
- `st.set_page_config()` is called in entry point (works locally, may need removal for SiS)
- Snow CLI 3.3.0 has a bug where `CREATE STREAMLIT` syntax fails — must use manual SQL workaround

### Data Strategy (Decided 2026-03-09)
**Current: Hybrid approach**
- Performance pages (query perf, warehouse, users, errors) → Use existing per-customer `{CUSTOMER}_QUERY_HISTORY_V` views
- AI/ML & consumption pages (cortex AI, home overview) → Direct queries against `FINANCE.CUSTOMER.PRODUCT_*` tables filtered by `SALESFORCE_ACCOUNT_ID`
- No new objects needed

**Future: Per-customer AI/ML views**
If direct queries against PRODUCT_* tables become too slow:
1. Create `{CUSTOMER}_AI_USAGE_V` views wrapping PRODUCT_* tables filtered by SFDC ID
2. Or create dynamic tables materializing common aggregations (daily AI feature summary per customer)
3. Script to automate view creation when onboarding a new customer

### Skills Reference
- **account-360**: Revenue, consumption, product adoption, tools, support cases, opportunities. All keyed by `SALESFORCE_ACCOUNT_ID`.
- **customer-health-check**: Warehouse deep dives, spilling/cache/pruning/QAS analysis via `SNOWHOUSE_IMPORT.<DEPLOYMENT>.JOB_ETL_V`. Requires deployment + account_id.
