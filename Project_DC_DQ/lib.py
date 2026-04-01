import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement
if not hasattr(sa, "ColumnElement"):
    sa.ColumnElement = ColumnElement  # shim for GX
import os
import connection
from typing import Any, Dict, List
from dataclasses import dataclass
from datetime import datetime, timezone, time, date, timedelta
import json
import polars as pl
# from psycopg2.extras import Json
from decimal import Decimal
import numpy as np
from zoneinfo import ZoneInfo
from typing import Optional
from datahub_handler import report_result, stable_assertion_id, EXPECTATION_DESCRIPTION_MAP, default_description
import os
from odps import ODPS, options
import requests

now_wib = datetime.now(ZoneInfo("Asia/Jakarta"))
@dataclass
class ExpectationResult:
    success: bool
    observed_value: Any

@dataclass
class ValidationResult:
    row_count_expectation: ExpectationResult
    column_count_expectation: ExpectationResult
    mandatory_column_expectation: ExpectationResult
    pk_not_null_expectation: ExpectationResult
    pk_unique_expectation: ExpectationResult
    column_list: List[str]

SCHEMA_DICTIONARY = {
    'raw_data_dev_cloud': 'trino',
    'raw_data_prod_cloud': 'trino',
    'raw_data_prod': 'trino',
    'feature_engineering_dev' : 'trino',
    'wsbfi_3_dev': 'odps',
    'wsbfi_3': 'odps',
    'wsbfi_2_dev': 'odps',
    'wsbfi_2': 'odps',
    'wsro_dev': 'odps',
    'wsro': 'odps',
    'wspd_dev': 'odps',
    'wspd': 'odps',
    'bravo_dev': 'odps',
    'bravo_sit': 'postgres',
    'bravo_prod': 'postgres',
    'bravo_bau': 'postgres'
    }

DEV_SCHEMA_DICTIONARY = {
    'postgres': 'public',
    'trino': 'raw_data_prod_cloud',
    'odps': 'wsbfi_3_dev'
}
    
def get_primary_key(table_name: str) -> str | None:
    """
    Get table primary key via Hologres
    """
    try :
        query = """
            SELECT
                kcu.column_name AS primary_key
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND lower(kcu.table_name) = lower(%s)
            LIMIT 1
        """
        with connection.connection_manager("postgres") as con:
            data = con.execute_query(query, [table_name])

    # if polars DataFrame
        if data.is_empty():
            return None

        return str(data[0, 0]).strip().lower()
    except Exception:
        return None

# def prefix_table_name(table_name: str, stage: str):
#     if stage in ['holo', 'postgres']:
#         return table_name
#     elif stage == 'odps':
#         return f'{table_name}_odps'
#     elif stage == 'trino':
#         return table_name
    
def get_schema_holo(table_name: str):
    """
    Get table schema via Hologres
    """
    query = "select table_schema from information_schema.tables where table_name = %s"
    with connection.connection_manager("postgres") as con:
        data = con.execute_query(query, [table_name])

    if data.is_empty():
        return None
    
    return (data[0,0])
    
def get_information_schema(table_name: str, stage: str):
    """
    Get information schema

    Note:
        Not all table exist in the information schema
    """
    if stage == 'holo':
        schema = get_schema_holo(table_name)
        query = f"""SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table_name}'"""
        with connection.connection_manager('postgres') as con:
            data = con.execute_query(query)
        return data
    
    if stage == 'odps':
        query = f"select column_name, data_type FROM information_schema.COLUMNS WHERE table_name = '{table_name}'"
        with connection.connection_manager('odps') as con:
            data = con.execute_query(query)
        return data
    
    elif stage == 'trino':
        schema = os.getenv('schema_datalake')
        query = f"SELECT column_name, data_type FROM information_schema.COLUMNS WHERE table_schema = '{schema}' AND table_name = 'gx_{table_name}'"
        with connection.connection_manager('trino') as con:
            data = con.execute_query(query)
        return data
    
# def get_dwh_table_name(table_name: str):
#     """
#     Get DWH SqlServer table name by raw table name

#     Note: In some cases there are duplicated tables or false tables with faulty naming conventions
#     """
#     query = f"SELECT name FROM sys.tables WHERE name LIKE \'%_{table_name}\'"

#     with connection.connection_manager("dwh") as con:
#         data = con.execute_query(query)

#     return (data[0,0]) if not data.is_empty() else None
    
def create_view(table_name: str, schema: str, partition: str):
    """
    Create or replace a partition-filtered validation view for Great Expectations.

    Builds a temporary view prefixed with ``gx_`` that selects data from the
    source table and adds a normalized ``__dt_date`` column used for
    partition-based validation. The SQL generated depends on the underlying
    data platform derived from the schema.

    Supported platforms:
        - trino: parses ``dt`` column (YYYYMMDD) into DATE
        - odps:  converts ``pt`` partition column into DATE

    The view is created inside the development schema associated with the
    platform and only contains rows for the specified partition.

    Args:
        table_name (str):
            Source table name.

        schema (str):
            Source database/schema name used to determine platform.

        partition (str):
            Partition value (YYYYMMDD) used to filter rows.

    Returns:
        None

    Side Effects:
        - Executes DDL against the target database.
        - Replaces existing ``gx_<table_name>`` view if it exists.
        - Prints the generated SQL query.

    Raises:
        KeyError:
            If schema mapping to stage or dev schema is not configured.

        Exception:
            Propagates database execution errors.
    """
    stage = SCHEMA_DICTIONARY.get(schema)
    dev_schema = DEV_SCHEMA_DICTIONARY.get(stage)
    # cols = {c.lower() for c in get_column_list(table_name, schema)}  # normalize
    select_exprs = ["s.*"]
    
    if stage == 'trino':
        select_exprs.append("""
        CAST(
            TRY(date_parse(CAST(s.dt AS varchar), '%Y%m%d'))
            AS DATE
        ) AS __dt_date
        """)

        query = f"""
        CREATE OR REPLACE VIEW hive.{dev_schema}.gx_{table_name} AS
        SELECT {",".join(select_exprs)}
        FROM hive.{schema}.{table_name} s
        WHERE dt='{partition}'
        """
        
    if stage == 'odps':
        select_exprs.append("""
            TO_DATE(s.pt, 'yyyyMMdd') AS __dt_date
        """)

        query = f"""
            CREATE OR REPLACE VIEW {dev_schema}.gx_{table_name} AS
            SELECT
                {", ".join(select_exprs)}
            FROM {schema}.{table_name} 
        """
    # where pt = '{partition}'
    else :
        return
    
    print(query)
    with connection.connection_manager(stage) as con:
        con.execute_non_select_query(query)

def delete_view(table_name: str, schema: str):
    stage = SCHEMA_DICTIONARY.get(schema)
    dev_schema = DEV_SCHEMA_DICTIONARY.get(stage)
    query = f"""
            DROP VIEW IF EXISTS hive.{dev_schema}.gx_{table_name}
            """
    with connection.connection_manager(stage) as con:
        con.execute_non_select_query(query)

def get_overall_status_count_in_batch():
    dt = datetime.now(timezone.utc).strftime("%Y%m%d")

    query1 = f"select count(*) from data_analytics_sandbox.data_quality_summary_report where dt = '{dt}'"
    query2 = f"select count(*) from data_analytics_sandbox.data_quality_summary_report where dt = '{dt}' and overall_status = True"
    
    with connection.connection_manager('trino') as con:
        data1 = con.execute_query(query1)
        data2 = con.execute_query(query2)

    return int(data1[0,0]), int(data2[0,0])

def generate_id(name: str) -> str:
    time_id = now_wib.strftime("%Y%m%dT%H%M%S")
    return f"{time_id}_{name}"

def format_value(v):
    if v is None:
        return None
    if isinstance(v, (str, bool, int, float)):
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, np.generic):
        return np.asarray(v).item()
    if isinstance(v, (list, tuple)):
        return [format_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): format_value(vv) for k, vv in v.items()}
    # fallback
    return str(v)
        
# def delete_summary_report():
#     return

# def delete_expectation_result():
#     return



def _build_expectation_result_values(exp_row: Dict, run_id: str, stage: str, table_name: str) -> List:
    suite_name = exp_row.get("suite_name")
    expectation_name = exp_row.get("expectation_name")
    expectation_type = exp_row.get("expectation_type")
    severity = exp_row.get("severity")
    success = exp_row.get("success")
    observed = exp_row.get("observed_value")
    details = exp_row.get("details")
    safe_details = json.dumps(details, default=format_value)
    dimension = exp_row.get("dimension")
    exp_id = generate_id(expectation_name)
    expectation_config = exp_row.get("expectation_config")
    # expectation_config = (
    #     expectation_config_obj.to_json_dict() if expectation_config_obj is not None else None
    # )

    dt = now_wib.strftime("%Y%m%d")

    # UNCOMMENT FOR DATAHUB REPORTING
    # try:
    #     description =  EXPECTATION_DESCRIPTION_MAP[expectation_type](expectation_config)
    # except Exception:
    #     description = default_description(expectation_type, expectation_config)

    # props = {
    #         'severity': severity,
    #         'dt': dt,
    #         'unexpected_result': observed,
    #         'description': description,
    #         'id': exp_id
    #     }
    # status = 'SUCCESS' if success else 'FAILURE'
    # try:
    #     report_result(stage=stage, table_name=table_name, expectation_name=expectation_name, status=status, props=props)
    # except Exception as e:
    #     print(e)
    #     pass

    # if success == False:
    #     message_text = f"Table: {table_name}\n\
    #     Stage: {stage}\n\
    #     🚨*FAIL* on {expectation_name}\n\
    #     Severity: {severity.upper()}"

    #     send_gchat_alert(message_text=message_text)

    return [
        exp_id,               # consider including run_id/suite_name to avoid collisions
        run_id,
        suite_name,
        expectation_name,
        expectation_type,
        severity,
        success,
        json.dumps(observed) if observed is not None else None,
        safe_details,
        dimension,
        json.dumps(expectation_config) if expectation_config is not None else None,
        dt,
        stable_assertion_id(stage=stage, table_name=table_name, suite_name=suite_name, expectation_name=expectation_name)
        ]
        
def insert_expectation_results_for_table(
    stage: str,
    table_name: str,
    exp_rows: List[Dict],
    run_id: str,
    target_table: str = "data_analytics_sandbox.data_quality_expectation_result",
    chunk_size: int = 500,
    suite_name_filter: Optional[str] = None,  # pass table_name/suite_name if you want to ensure “per table”
):
    """
    Bulk insert expectation results (optionally for a single suite/table) using chunked multi-row VALUES.

    Works well when your connection layer doesn't support executemany.
    """

    columns = [
        "id",
        "run_id",
        "suite_name",
        "expectation_name",
        "expectation_type",
        "severity",
        "success",
        "observed_value",
        "details",
        "dimension",
        "expectation_config",
        "dt",
        "assertion_id"
    ]

    if suite_name_filter:
        exp_rows = [r for r in exp_rows if r.get("suite_name") == suite_name_filter]

    if not exp_rows:
        print("No expectation rows to insert.")
        return

    # Prebuild per-row placeholder block: "(?, ?, ?, ...)"
    row_placeholders = "(" + ", ".join(["?"] * len(columns)) + ")"

    with connection.connection_manager("trino") as con:
        total = 0

        for i in range(0, len(exp_rows), chunk_size):
            chunk = exp_rows[i : i + chunk_size]

            # Build VALUES part like: "(?,?...), (?,?...), ..."
            values_sql = ", ".join([row_placeholders] * len(chunk))

            insert_query = f"""
                INSERT INTO {target_table} (
                    {", ".join(columns)}
                )
                VALUES {values_sql}
            """

            # Flatten params: [row1col1, row1col2, ..., row2col1, ...]
            flat_params: List = []
            for exp_row in chunk:
                flat_params.extend(_build_expectation_result_values(exp_row, run_id, stage, table_name))

            con.execute_non_select_query(insert_query, flat_params)
            total += len(chunk)

    print(f"Successfully inserted {total} expectation results 🙂‍↕️")

def insert_summary_report(result: ValidationResult, table_name: str, stage: str, partition: str = None):
    overall_status = (
        result.mandatory_column_expectation.success is True
        and result.column_count_expectation.success is True 
        and result.row_count_expectation.success is True
        and result.pk_unique_expectation.success is True
        and result.pk_not_null_expectation.success is True
    )

    dt = now_wib.strftime("%Y%m%d")
    # batch_utc_midnight = datetime.combine(now_utc.date(), time.min)
    run_id = generate_id(table_name)

    pk_available = None
    if stage == "postgres":
        pk_available = True if get_primary_key(table_name) is not None else False

    # Build the values
    values = [
        run_id,
        table_name,
        stage,
        overall_status,
        result.row_count_expectation.success,
        result.column_count_expectation.success,
        result.mandatory_column_expectation.success,
        result.pk_not_null_expectation.success,
        result.pk_unique_expectation.success,
        int(result.row_count_expectation.observed_value)
        if result.row_count_expectation.observed_value is not None
        else None,
        int(result.column_count_expectation.observed_value)
        if result.column_count_expectation.observed_value is not None
        else None,
        json.dumps(result.mandatory_column_expectation.observed_value or None),
        json.dumps(result.pk_not_null_expectation.observed_value or None),
        json.dumps(result.pk_unique_expectation.observed_value or None),
        format_value(now_wib),
        json.dumps(getattr(result, "column_list", None)),
        True if get_primary_key(table_name) is not None else False,
        dt
    ]

    columns = [
        "run_id",
        "table_name",
        "stage",
        "overall_status",
        "row_count_status",
        "column_count_status",
        "mandatory_column_status",
        "pk_not_null_status",
        "pk_unique_status",
        "row_count_result",
        "column_count_result",
        "mandatory_column_result",
        "pk_not_null_result",
        "pk_unique_result",
        "created_at",
        "column_list",
        "pk_available",
        "dt"
    ]
    placeholders = ", ".join(["?"] * len(columns))
    with connection.connection_manager("trino") as con:
        insert_query = f"""
                INSERT INTO data_analytics_sandbox.data_quality_summary_report (
                    {", ".join(columns)}
                ) VALUES ({placeholders})
            """
        con.execute_non_select_query(insert_query, values)
    print(f"✅ Inserted new record for {table_name} (dt={dt})")

    return run_id

def get_column_list(table_name: str, schema: str):

    stage = SCHEMA_DICTIONARY.get(schema.lower())

    if stage is None:
        raise RuntimeError(f"Schema {schema} not mapped in SCHEMA_DICTIONARY")

    # ===============================
    # POSTGRES
    # ===============================
    if stage == "postgres":

        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        """

        with connection.connection_manager("postgres") as con:
            data = con.execute_query(query)

        if data.is_empty():
            return []

        return data["column_name"].to_list()

    # ===============================
    # ODPS
    # ===============================
    elif stage == "odps":

        options.allow_antique_date = True
        query = f"DESC {schema}.{table_name}"
        o = ODPS(
            os.getenv("user_odps"),
            os.getenv("password_odps"),
            os.getenv("project_odps"),
            endpoint=os.getenv("endpoint_odps")
        )

        table=o.get_table(table_name, schema=schema)

        # with connection.connection_manager("odps") as con:
        #     data = con.execute_query(query)

        # if data.is_empty():
        #     return []

        return [col.name.lower() for col in table.schema.columns]

    # ===============================
    # TRINO
    # ===============================
    elif stage == "trino":

        schema = os.getenv("schema_datalake")

        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        """

        with connection.connection_manager("trino") as con:
            data = con.execute_query(query)

        if data.is_empty():
            return []

        return data["column_name"].to_list()

# def execute_odps_query(query: str):
#     """
#     Execute query to ODPS and return Polars DataFrame
#     """
#     with connection.connection_manager("odps") as con:
#         data = con.execute_query(query)

#     return data

def compile_result(results, table_name, schema, column_list=None):

    formatted = {
        "row_count_expectation": ExpectationResult(False, None),
        "column_count_expectation": ExpectationResult(False, None),
        "mandatory_column_expectation": ExpectationResult(False, []),
        "pk_not_null_expectation": ExpectationResult(False, []),
        "pk_unique_expectation": ExpectationResult(False, []),
    }

    if column_list is None:
        column_list = get_column_list(table_name, schema)

    if not results or "results" not in results:
        validation_summary = ValidationResult(
            row_count_expectation=formatted["row_count_expectation"],
            column_count_expectation=formatted["column_count_expectation"],
            mandatory_column_expectation=formatted["mandatory_column_expectation"],
            pk_not_null_expectation=formatted["pk_not_null_expectation"],
            pk_unique_expectation=formatted["pk_unique_expectation"],
            column_list=column_list
        )
        return validation_summary, []

    exp_rows = []

    suite_meta = results.get("meta", {})
    suite_name = suite_meta.get("expectation_suite_name")

    for r in results["results"]:
        cfg_obj = r.get("expectation_config")
        if hasattr(cfg_obj, "to_json_dict"):
            cfg = cfg_obj.to_json_dict()
        else:
            cfg = cfg_obj or {}

        res = r.get("result") or {}
        meta = cfg.get("meta", {})
        etype = cfg.get("type") or cfg.get("expectation_type")
        success = bool(r.get("success"))
        name = meta.get("name") or etype
        dimension = meta.get("dimension")
        severity = meta.get("severity")
        column = cfg.get("kwargs", {}).get("column")

        if name == "mandatory_column_expectation":
            missing_columns = (
                res.get("missing_columns")
                or res.get("details", {}).get("missing_columns")
                or []
            )
            observed_value = missing_columns
        elif name in ["row_count_expectation", "column_count_expectation"]:
            observed_value = res.get("observed_value")
        else:
            observed_value = (
                res.get("observed_value")
                if res.get("observed_value") is not None
                else res.get("unexpected_count")
                if res.get("unexpected_count") is not None
                else res.get("unexpected_percent")
                if res.get("unexpected_percent") is not None
                else res.get("element_count")
            )

        observed_value = format_value(observed_value)

        details = {
            "element_count": res.get("element_count"),
            "unexpected_count": res.get("unexpected_count"),
            "unexpected_percent": res.get("unexpected_percent"),
            "success_percent": res.get("success_percent"),
            "partial_unexpected_list": res.get("partial_unexpected_list"),
            "unexpected_rows": res.get("unexpected_rows"),
            "column_list": column_list,
        }

        exp_rows.append({
            "suite_name": suite_name,
            "expectation_name": name,
            "expectation_type": etype,
            "severity": severity,
            "success": success,
            "observed_value": observed_value,
            "details": details,
            "dimension": dimension,
            "column": column,
            "element_count": res.get("element_count"),
            "unexpected_count": res.get("unexpected_count"),
            "unexpected_percent": res.get("unexpected_percent"),
            "success_percent": res.get("success_percent"),
            "expectation_config": cfg
        })

        if name in formatted:
            formatted[name] = ExpectationResult(success, observed_value)

    validation_summary = ValidationResult(
        row_count_expectation=formatted["row_count_expectation"],
        column_count_expectation=formatted["column_count_expectation"],
        mandatory_column_expectation=formatted["mandatory_column_expectation"],
        pk_not_null_expectation=formatted["pk_not_null_expectation"],
        pk_unique_expectation=formatted["pk_unique_expectation"],
        column_list=column_list
    )

    return validation_summary, exp_rows

def send_gchat_alert(message_text: str):
    webhook_url = os.getenv('gchat_webhook_url')

    message = {
        "text": message_text
    }

    try:
        response = requests.post(
            url=webhook_url,
            json=message  # ✅ FIX
        )
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f"Error sending message: {e}")
        if hasattr(e, "response") and e.response is not None:
            print("Response body:", e.response.text)