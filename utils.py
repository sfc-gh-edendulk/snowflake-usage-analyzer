import streamlit as st
import pandas as pd


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


def build_where_clause():
    start = st.session_state.get("filter_start")
    end = st.session_state.get("filter_end")
    clauses = [f"start_time >= '{start}'", f"start_time <= '{end}'"]
    if st.session_state.get("filter_warehouse"):
        clauses.append(f"warehouse_name = '{st.session_state.filter_warehouse}'")
    if st.session_state.get("filter_user"):
        clauses.append(f"user_name = '{st.session_state.filter_user}'")
    return " AND ".join(clauses)


def get_query_source():
    return st.session_state.get("query_history_view", "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY")


def to_pandas_native(df):
    return pd.DataFrame(df.to_dict())


@st.cache_data(ttl=600)
def resolve_customer(search_term):
    safe_term = search_term.replace("'", "''")
    try:
        return execute_query(f"""
            SELECT DISTINCT
                ID as SALESFORCE_ACCOUNT_ID,
                NAME,
                INDUSTRY,
                TYPE as TYPE,
                BILLING_COUNTRY
            FROM FIVETRAN.SALESFORCE.ACCOUNT
            WHERE LOWER(NAME) LIKE LOWER('%{safe_term}%')
              AND IS_DELETED = FALSE
            ORDER BY NAME
            LIMIT 10
        """)
    except:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def get_snowflake_accounts_for_sfdc(sfdc_id):
    safe_id = sfdc_id.replace("'", "''")
    try:
        return execute_query(f"""
            SELECT
                SNOWFLAKE_ACCOUNT_NAME,
                SNOWFLAKE_ACCOUNT_ID,
                UPPER(DEPLOYMENT) as DEPLOYMENT,
                SUM(CREDITS) as TOTAL_CREDITS_L90D
            FROM SALES.SE_REPORTING.ACCOUNT_TOOL_CREDITS_MONTHLY
            WHERE SALESFORCE_ACCOUNT_ID = '{safe_id}'
              AND USAGE_MONTH >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE()))
            GROUP BY SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_ACCOUNT_ID, DEPLOYMENT
            ORDER BY TOTAL_CREDITS_L90D DESC
        """)
    except:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def discover_customer_views(customer_name):
    safe_name = customer_name.replace("'", "''").replace(" ", "_").upper()
    try:
        result = execute_query(f"""
            SELECT TABLE_NAME
            FROM TEMP.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'EDENDULK'
              AND TABLE_NAME LIKE '{safe_name}%'
            ORDER BY TABLE_NAME
        """)
        views = {}
        if result is not None and not result.empty:
            for tname in result['TABLE_NAME'].tolist():
                suffix = tname.replace(safe_name + "_", "")
                views[suffix] = f"TEMP.EDENDULK.{tname}"
        return views
    except:
        return {}


def _build_job_etl_subquery(deployment, account_id, date_start, date_end):
    return f"""
      select job_id
        , dpo:"JobDPO:primary" as job_dpo_primary
        , dpo:"JobDPO:stats" as job_dpo_stats
        , dpo:"JobDPO:description" as job_dpo_desc
        , parse_json(coalesce(strip_null_value(dpo:"JobDPO:stats".statesDuration)
                         , strip_null_value(dpo:"JobDPO:primary".statesDuration))) as job_statsDuration
        , parse_json(coalesce(strip_null_value(dpo:"JobDPO:stats".stats)
                       , strip_null_value(dpo:"JobDPO:primary".stats))) as job_stats
         , DUR_GS_EXECUTING
         , DUR_XP_EXECUTING
         , DUR_ABORTING
         , DUR_FAILED_EXECUTION
         , DUR_WAIT_FILE_DELETION_GATEWAY
         , DUR_GS_POSTEXECUTING
         , total_duration
      from snowhouse_import.{deployment}.job_etl_v job_view
      where not (strip_null_value(job_dpo_primary:userName)::string = 'SYSTEM'
      and strip_null_value(job_dpo_primary:roleName)::string = 'APPADMIN'
      and contains(upper(strip_null_value(job_dpo_desc:description)::string), 'WORKSHEETS_APP'))
      and created_on >= '{date_start}'
      and created_on <= '{date_end}'
      and account_id = {account_id}
    """


def build_query_history_view_sql(view_name, accounts, date_start, date_end):
    subqueries = []
    for _, row in accounts.iterrows():
        subqueries.append(_build_job_etl_subquery(
            row['DEPLOYMENT'], row['SNOWFLAKE_ACCOUNT_ID'], date_start, date_end
        ))

    union_subquery = "\n    UNION ALL\n    ".join(subqueries)

    return f"""
CREATE OR REPLACE VIEW TEMP.EDENDULK.{view_name} AS (
    with instance_type_map as (
        select value:id as id
          , value:"externalName"::string as name
        from table(flatten(parse_json(system$dump_enum('ServerTypes'))))
    )
    , statement_type as (
        select key::int as id
          , value::string statement_type
        from table(flatten(input => select parse_json(system$dump_enum('StatementType')) as statement_type))
      )
    , cloud_region as (
        select value:id::int as id
          , value:cloud::string as cloud
          , value:region::string as region
        from table(flatten(input => select parse_json(system$dump_enum('CloudRegions'))))
      )
    select
      strip_null_value(job_dpo_primary:uuid)::string as query_id
      , iff(
          bitand(job_dpo_stats:flags::int, 34359738368) = 0
          , strip_null_value(job_dpo_desc:description)::string
          , ''
        ) as query_text
      , strip_null_value(job_dpo_primary:databaseName)::string as database_name
      , strip_null_value(job_dpo_primary:schemaName)::string as schema_name
      , statement_type.statement_type as query_type
      , strip_null_value(job_dpo_primary:sessionId)::int as session_id
      , strip_null_value(job_dpo_primary:userName)::string as user_name
      , strip_null_value(job_dpo_primary:roleName)::string as role_name
      , strip_null_value(job_dpo_primary:warehouseName)::string as warehouse_name
      , decode (job_stats:stats:warehouseSize::int
          , 1,'X-Small'
          , 2,'Small'
          , 4,'Medium'
          , 8,'Large'
          , 16,'X-Large'
          , 32,'2X-Large'
          , 64,'3X-Large'
          , 128,'4X-Large'
          , 20,'5X-Large'
          , 40,'6X-Large'
          , null
        ) as warehouse_size
      , instance_type_map.name as warehouse_type
      , nullif(job_dpo_stats:latestClusterNumber::int, -1) + 1 as cluster_number
      , strip_null_value(job_dpo_primary:tag)::string as query_tag
      , decode(coalesce(strip_null_value(job_dpo_stats:currentStateId), strip_null_value(job_dpo_primary:currentStateId))::int
        , 15, 'FAIL'
        , 16, 'INCIDENT'
        , 17, 'SUCCESS'
        ) as execution_status
      , strip_null_value(coalesce(strip_null_value(job_dpo_stats:errorCode), strip_null_value(job_dpo_primary:errorCode)))::string as error_code
      , iff(
          bitand(job_dpo_stats:flags::int, 34359738368) = 0
          , strip_null_value(coalesce(strip_null_value(job_dpo_stats:errorMessage),
              strip_null_value(job_dpo_primary:errorname)))::string
          , ''
        ) as error_message
      , (job_dpo_primary:createdOn::int/1000)::timestamp_ltz as start_time
      , least(strip_null_value(job_dpo_stats:endTime)::int/1000, 4102444800)::timestamp_ltz as end_time
      , least(strip_null_value(job_dpo_stats:endTime)::int, 4102444800000) - job_dpo_primary:createdOn::int as total_elapsed_time
      , (coalesce(job_stats:stats:ioLocalFdnReadBytes::double, 0) + coalesce(job_stats:stats:ioRemoteFdnReadBytes::double, 0))::int as bytes_scanned
      , div0(nvl(job_stats:stats:ioLocalFdnReadBytes::double, 0), nvl(job_stats:stats:ioLocalFdnReadBytes::double, 0) + nvl(job_stats:stats:ioRemoteFdnReadBytes::double, 0)) as percentage_scanned_from_cache
      , coalesce(job_stats:stats:ioRemoteFdnWriteBytes::number, 0) as bytes_written
      , coalesce(job_stats:stats:ioRemoteResultWriteBytes::number, 0) as bytes_written_to_result
      , coalesce(job_stats:stats:ioRemoteResultReadBytes::number, 0) as bytes_read_from_result
      , job_stats:stats:producedRows::int as rows_produced
      , coalesce(job_stats:stats:numRowsInserted::int, 0) as rows_inserted
      , coalesce(job_stats:stats:numRowsUpdated::int, 0) as rows_updated
      , coalesce(job_stats:stats:numRowsDeleted::int, 0) as rows_deleted
      , coalesce(job_stats:stats:numRowsUnloaded::int, 0) as rows_unloaded
      , coalesce(job_stats:stats:numBytesUnloaded::int, 0) as bytes_deleted
      , coalesce(job_stats:stats:scanFiles::number, 0) as partitions_scanned
      , coalesce(job_stats:stats:scanOriginalFiles::number, 0) as partitions_total
      , coalesce(job_stats:stats:ioLocalTempWriteBytes::number, 0) as bytes_spilled_to_local_storage
      , coalesce(job_stats:stats:ioRemoteTempWriteBytes::number, 0) as bytes_spilled_to_remote_storage
      , coalesce(job_stats:stats:netSentBytes::number, 0) as bytes_sent_over_the_network
      , coalesce(job_statsDuration[0], 0)::number
        + coalesce(job_statsDuration[6],  0)::number
        + coalesce(job_statsDuration[12], 0)::number
        + coalesce(job_statsDuration[13], 0)::number
        + coalesce(job_statsDuration[16], 0)::number
        + coalesce(job_statsDuration[19], 0)::number
        + coalesce(job_statsDuration[21], 0)::number
        as compilation_time
      , coalesce(job_statsDuration[2], 0)::number
        + coalesce(job_statsDuration[7], 0)::number
        + coalesce(job_statsDuration[8], 0)::number
        + coalesce(job_statsDuration[11], 0)::number
        + coalesce(job_statsDuration[15], 0)::number
        + coalesce(job_statsDuration[18], 0)::number
        as execution_time
      , coalesce(job_statsDuration[4], 0)::number as queued_provisioning_time
      , coalesce(job_statsDuration[5], 0)::number as queued_repair_time
      , coalesce(job_statsDuration[3], 0)::number as queued_overload_time
      , coalesce(job_statsDuration[1], 0)::number as transaction_blocked_time
      , outbound_cloud_region.cloud as outbound_data_transfer_cloud
      , outbound_cloud_region.region as outbound_data_transfer_region
      , coalesce(job_stats:stats:ioRemoteExternalWriteBytes::int, 0) as outbound_data_transfer_bytes
      , inbound_cloud_region.cloud as inbound_data_transfer_cloud
      , inbound_cloud_region.region as inbound_data_transfer_region
      , coalesce(job_stats:stats:ioRemoteExternalReadBytes::int, 0) as inbound_data_transfer_bytes
      , coalesce(job_statsDuration[10], 0)::number + coalesce(job_statsDuration[14], 0)::number as list_external_files_time
      , coalesce(job_stats:stats:gsBillingMicroCreditsInternal::double/1000000.00, 0) as credits_used_cloud_services
      , strip_null_value(job_dpo_primary:majorVersionNumber)::int || '.' || strip_null_value(job_dpo_primary:minorVersionNumber)::int || '.' || strip_null_value(job_dpo_primary:patchVersionNumber)::int as release_version
      , coalesce(job_stats:stats:extFuncTotalInvocations::number, 0) as external_function_total_invocations
      , coalesce(job_stats:stats:extFuncTotalSentRows::number, 0) as external_function_total_sent_rows
      , coalesce(job_stats:stats:extFuncTotalReceivedRows::number, 0) as external_function_total_received_rows
      , coalesce(job_stats:stats:extFuncTotalSentBytes::number, 0) as external_function_total_sent_bytes
      , coalesce(job_stats:stats:extFuncTotalReceivedBytes::number, 0) as external_function_total_received_bytes
      , iff(nvl(job_stats:stats:warehouseAvailableSize::int, job_stats:warehouseSize::int) = 0, null,
            least(round(nvl(job_stats:serverCount::int, 0)*100/nvl(job_stats:stats:warehouseAvailableSize::int, job_stats:warehouseSize::int)), 100)) as query_load_percent
      , bitand(job_dpo_stats:flags::int, 32) = 32 as is_client_generated_statement
    from
    (
    {union_subquery}
    ) as json_parsed
    left join instance_type_map
    on strip_null_value(job_dpo_primary:serverTypeId) = instance_type_map.id
    left join statement_type
    on greatest(coalesce(strip_null_value(job_dpo_primary:statementProperties)::int, 0), coalesce(strip_null_value(job_dpo_stats:statementProperties)::int, 0)) = statement_type.id
    left join cloud_region as outbound_cloud_region
    on job_dpo_stats:egressRegionId::int = outbound_cloud_region.id
    left join cloud_region as inbound_cloud_region
    on job_dpo_stats:ingressRegionId::int = inbound_cloud_region.id
    where 1=1
    and job_dpo_stats:currentStateId::int != 25
    and strip_null_value(job_dpo_primary:sessionId)::int != 0
    and bitand(job_dpo_stats:flags::int, 2080) != 2080
    and bitand(job_dpo_stats:flags::int, 1073741824) != 1073741824
)"""


def create_query_history_view(view_name, accounts, date_start, date_end):
    sql = build_query_history_view_sql(view_name, accounts, date_start, date_end)
    try:
        execute_query(sql)
        return True, None
    except Exception as e:
        return False, str(e)
