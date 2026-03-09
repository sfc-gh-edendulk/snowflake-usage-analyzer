# Snowflake Usage Analyzer

Analyze your Snowflake account's query history with interactive dashboards and AI-powered optimization recommendations.

## Features

- **Query Performance Analysis** - Partition pruning efficiency, memory spilling, cache utilization, and time breakdown
- **Warehouse Optimization** - Right-sizing recommendations, utilization patterns, multi-cluster analysis
- **User & Role Analytics** - Activity patterns, role distribution, query type trends
- **Error Analysis** - Failed query patterns, error trends, troubleshooting guidance
- **Cortex AI Usage** - Track adoption of LLM functions, Search, Analyst, Agents, and Intelligence
- **AI Recommendations** - Cortex-powered optimization suggestions on every page

## Prerequisites

1. **Snowflake Account** with access to ACCOUNT_USAGE views
2. **Role with IMPORTED PRIVILEGES** on the SNOWFLAKE database
3. **Cortex LLM Functions** enabled (for AI recommendations - optional but recommended)

## Setup

### 1. Grant ACCOUNT_USAGE Access

Run the commands in `setup.sql` to grant the necessary permissions:

```sql
-- Option A: Use existing role with ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;

-- Option B: Create a dedicated role
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS USAGE_ANALYZER_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE USAGE_ANALYZER_ROLE;
GRANT ROLE USAGE_ANALYZER_ROLE TO USER <your_user>;
```

### 2. Deploy the Streamlit App

```sql
-- Create a database and schema for the app (if needed)
CREATE DATABASE IF NOT EXISTS APPS;
CREATE SCHEMA IF NOT EXISTS APPS.STREAMLIT;

-- Create a stage for the app files
CREATE STAGE IF NOT EXISTS APPS.STREAMLIT.USAGE_ANALYZER_STAGE;

-- Upload files to the stage (run from terminal)
-- PUT file:///path/to/streamlit_app.py @APPS.STREAMLIT.USAGE_ANALYZER_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/environment.yml @APPS.STREAMLIT.USAGE_ANALYZER_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/pages/*.py @APPS.STREAMLIT.USAGE_ANALYZER_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT APPS.STREAMLIT.USAGE_ANALYZER
  FROM @APPS.STREAMLIT.USAGE_ANALYZER_STAGE
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = <your_warehouse>;
```

Or use Snowflake CLI:

```bash
snow streamlit deploy --database APPS --schema STREAMLIT
```

### 3. Access the App

Open Snowsight and navigate to **Streamlit** to find your deployed app.

## Important Notes

- **Data Latency**: ACCOUNT_USAGE views have up to 45 minutes of latency
- **Historical Data**: Query history is available for the last 365 days
- **Cortex AI**: AI recommendation features require Cortex LLM access; they will gracefully fail if unavailable

## File Structure

```
public_customer_usage_app/
├── README.md                    # This file
├── streamlit_app.py             # Main application
├── environment.yml              # Dependencies
├── setup.sql                    # Permission grants
└── pages/
    ├── 1_Query_Performance.py   # Pruning, spilling, caching analysis
    ├── 2_Warehouse_Analysis.py  # Sizing and utilization
    ├── 3_Users_Features.py      # User activity and feature adoption
    ├── 4_Errors_Recommendations.py  # Error analysis
    └── 5_Cortex_AI_Usage.py     # AI feature tracking
```

## License

MIT License - feel free to modify and adapt for your needs.
