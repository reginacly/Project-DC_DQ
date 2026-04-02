import great_expectations as gx
import polars as pl
import pandas as pd
import connection
import requests
import json
import os

from dataclasses import asdict
from typing import Union, Type, Any, List, Dict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from great_expectations.core import ExpectationValidationResult, ExpectationSuiteValidationResult
from datetime import datetime, timezone
from datetime import timedelta
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

@dataclass
class SourceResult:
    record_count : int
    column_count : int
    numeric_process : Dict
    source_columns: List[str]


def prefix_table_name(stage: str, table_name: str):
    if stage == 'pg':
        parts = table_name.split('_', 1)
        if len(parts) > 1:
            return parts[1]
        else:
            return table_name
    elif stage == 'odps':
        return f"{table_name}_odps"
    else:
        return table_name

def get_numeric_columns(information_schema: pl.DataFrame, pk: List[str]) -> List[str]:
    numeric_datatypes = {
        "int", "int4", "int8", "integer", "bigint", "smallint", "decimal", "numeric", "real",
        "double precision", "float", "int64", "float64", "double"
    }

    filtered_schema = information_schema.with_columns(
        pl.col("data_type").str.to_lowercase()
    )

    if pk:
        filtered_schema = filtered_schema.filter(~pl.col("column_name").is_in(pk))

    numeric_cols = (
        filtered_schema
        .filter(pl.col("data_type").is_in(numeric_datatypes))
        .select("column_name")
    )

    return numeric_cols["column_name"].to_list()

def get_mean_sample(table_name: pl.DataFrame, information_schema: pl.DataFrame, pk: List[str]) -> Union[Dict[str, float], None]:
    numeric_columns = get_numeric_columns(information_schema, pk)
    
    if not numeric_columns:
        return None

    mean_results = [
        {"column": column, "mean": table_name.select(pl.col(column).mean())[0, 0]}
        for column in numeric_columns
    ]

    return mean_results

def validate_datatype_conversion(information_schema1 : pd.DataFrame, information_schema2 : pd.DataFrame) -> bool:
    return 

def check_column(information_schema: pl.DataFrame):
    columns = ['created_at', 'updated_at']
    return set(columns).issubset(
        set(information_schema["column_name"].to_list())
    )

def get_max_timestamp(source_stage: str, holo_dbname : str):
    config = {
        "endpoint": os.getenv('endpoint_holo'),
        "port": os.getenv('port_holo'),
        "dbname": os.getenv('holo_dbname'),
        "user": os.getenv('shared_user'),
        "password": os.getenv('shared_password')
    }

    def get_max_timestamp_from_holo():
        try:
            query = f'''
                SELECT MAX(val) AS overall_max
                FROM (
                    SELECT created_at AS val FROM bravo_sit.{holo_dbname}
                    UNION ALL
                    SELECT updated_at FROM bravo_sit.{holo_dbname}
                ) AS combined;
            '''
            with connection.connection_manager('holo', config=config, schema='bravo_sit') as con:
                data = con.execute_query(query)
            return data.iloc[0,0]

        except Exception as e:
            print(f"Could not get data from 'bravo_sit' (Reason: {e}). Falling back to 'bravo_bau'.")

            try:
                query = f'''
                    SELECT MAX(val) AS overall_max
                    FROM (
                        SELECT created_at AS val FROM bravo_bau.{holo_dbname}
                        UNION ALL
                        SELECT updated_at FROM bravo_bau.{holo_dbname}
                    ) AS combined;
                '''
                with connection.connection_manager('holo', config=config, schema="bravo_bau") as con:
                    data = con.execute_query(query)
                return data.iloc[0, 0]
            
            except Exception as e_bau:
                print(f"ERROR: Fallback to 'bravo_bau' also failed. Reason: {e_bau}")
                return None

    def get_max_timestamp_from_report():
        query = f'''
            SELECT max_timestamp
            FROM bravo_sit.uat_summary_report
            WHERE target_stage = 'holo'
              AND target_table_name = '{holo_dbname}';
        '''
        with connection.connection_manager('holo', config=config, schema='bravo_sit') as con:
            data = con.execute_query(query)
        return data.iloc[0, 0]

    if source_stage == 'pg':
        return get_max_timestamp_from_holo()

    try:
        result = get_max_timestamp_from_report()
        if pd.isna(result):
            return get_max_timestamp_from_holo()
        return result
    except:
        return get_max_timestamp_from_holo()

def get_df(stage: str, 
           table_name: str, 
           schema_name: str, 
           config: Dict[str, Any],
           df_type: Type[Union[pd.DataFrame, pl.DataFrame]] = pd.DataFrame,
          **kwargs) -> Union[pd.DataFrame, pl.DataFrame]:
    if stage == "pg":
        max_timestamp = (kwargs.get('max_timestamp') - timedelta(hours=7))
        query = (
            f"SELECT * FROM {schema_name}.{table_name} "
            f"WHERE created_at <= \'{max_timestamp}\' OR updated_at <= \'{max_timestamp}\' "
            # f"LIMIT 5"
        )
    else:
        query = f"SELECT * FROM {schema_name}.{table_name}"
    
    # print("Executing: ",query)
    with connection.connection_manager(stage, config=config, schema=schema_name) as con:
        data = con.execute_query(query)

    if df_type == pd.DataFrame:
        return data
    elif df_type == pl.DataFrame:
        return pl.from_pandas(data)
    else:
        raise ValueError(f"Unsupported DataFrame type: {df_type}")
    
def get_information_schema(stage: str, table_name: str, schema_name: str, config: Dict[str, Any])-> pl.DataFrame:
    if stage == 'odps' : 
        config['project'] = 'wsbfi_3_dev'
        query = f'select column_name, data_type FROM information_schema.COLUMNS WHERE table_name = \'{table_name}\';'
    else: query = f'SELECT column_name, data_type FROM information_schema.COLUMNS WHERE table_schema = \'{schema_name}\' AND table_name = \'{table_name}\''
    
    # print("Executing:", query)
    with connection.connection_manager(stage, config=config, schema=schema_name) as con:
        data = con.execute_query(query)

    return pl.from_pandas(data)

def has_year(column_list: List[str]):
    year_columns = [col for col in column_list if 'year' in col.lower()]
    return year_columns if year_columns else None

def auto_increment(config: Dict[str, Any]) -> int:
    query = 'SELECT MAX(id) FROM bravo_sit.uat_summary_report;'
    with connection.connection_manager("holo", config=config, dbname='los') as con:
        data = con.execute_query(query)
    current_id = data.iloc[0, 0]
    return 1 if current_id is None else int(current_id) + 1

def format_value(val):
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, str):
        return f"'{val}'"
    elif isinstance(val, datetime):
        return f"'{val.isoformat()}'"
    elif val is None:
        return "NULL"
    elif isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    elif isinstance(val, (dict, list)):
        return f"'{json.dumps(val)}'"
    else:
        return str(val)

def insert_report(source_stage: str, source_table_name: str,
                  target_stage: str, target_table_name: str,
                  validation_results: List[ExpectationValidationResult],
                  source_results: SourceResult,
                  source_information_schema: pl.DataFrame, target_information_schema: pl.DataFrame,
                  max_timestamp : datetime):

# def insert_report(*args, **kwargs):
#     print("⚠️ SKIP INSERT REPORT (TEST MODE)")
#     return "Pass", []

    MANDATORY_EXPECTATIONS = {
        "expect_table_row_count_to_equal",
        "expect_table_column_count_to_equal",
        "expect_column_mean_to_be_between",
        "expect_table_columns_to_match_set"
    }

    EXPECTATION_TO_COLUMN = {
        "expect_table_row_count_to_equal": "record_count",
        "expect_table_column_count_to_equal": "column_count",
        "expect_column_mean_to_be_between": "numeric_process",
        "expect_table_columns_to_match_set": "mandatory_columns",
        "expect_column_values_to_not_be_null": "pk_not_null",
        "expect_column_values_to_be_unique": "pk_unique",
        "expect_column_values_to_be_between": "year_check"
    }

    config = {
        "endpoint": os.getenv('endpoint_holo'),
        "port": os.getenv('port_holo'),
        "dbname": os.getenv('holo_dbname'),
        "user": os.getenv('shared_user'),
        "password": os.getenv('shared_password')
    }
    def extract_columns_with_types(schema_df: pl.DataFrame) -> dict:
        return {row["column_name"]: row["data_type"] for row in schema_df.to_dicts()}

    def parse_results_for_summary(results: List[ExpectationValidationResult]):
        summary = {}
        observed_values = {}
        failed_expectations = []

        record_count_value = 0
        column_count_value = 0

        mean_expectations_success = []
        mean_expectations_details = {}

        column_match_observed = []
        column_match_success = []

        pk_not_null_success = []
        pk_not_null_details = {}

        pk_unique_success = []
        pk_unique_details = {}

        year_check_success = []
        year_check_details = {}

        for entry in results:
            expectation_type = entry["expectation_config"]["type"]
            success = entry["success"]
            result_data = entry.get("result", {})
            observed_value = result_data.get("observed_value", "N/A")
            column = EXPECTATION_TO_COLUMN.get(expectation_type)
            entry_column = entry["expectation_config"].get("kwargs", {}).get("column")

            if expectation_type == "expect_column_values_to_not_be_null":
                pk_not_null_success.append(success)
                pk_not_null_details[entry_column] = success

            elif expectation_type == "expect_column_values_to_be_unique":
                pk_unique_success.append(success)
                pk_unique_details[entry_column] = success

            elif expectation_type == "expect_column_values_to_be_between":
                year_check_success.append(success)
                year_check_details[entry_column] = success

            elif expectation_type == "expect_table_row_count_to_equal":
                record_count_value = observed_value

            elif expectation_type == "expect_table_column_count_to_equal":
                column_count_value = observed_value

            elif expectation_type == "expect_column_mean_to_be_between":
                mean_expectations_success.append(success)
                mean_expectations_details[entry_column] = observed_value

            elif expectation_type == "expect_table_columns_to_match_set":
                if not column_match_observed:
                    column_match_observed = observed_value
                column_match_success.append(success)

            else:
                observed_values[expectation_type] = observed_value

            if column:
                if expectation_type == "expect_table_row_count_to_equal":
                    summary[column] = str(success)
                elif expectation_type == "expect_table_column_count_to_equal":
                    summary[column] = str(success)
                elif expectation_type == "expect_column_mean_to_be_between":
                    summary[column] = str(all(mean_expectations_success)) if mean_expectations_success else None
                elif expectation_type == "expect_table_columns_to_match_set":
                    summary[column] = str(all(column_match_success))
                elif expectation_type == "expect_column_values_to_not_be_null":
                    summary[column] = str(all(pk_not_null_success))
                elif expectation_type == "expect_table_columns_to_be_unique":
                    summary[column] = str(all(pk_unique_success))
                elif expectation_type == "expect_column_values_to_be_between":
                    summary[column] = str(all(year_check_success)) if year_check_success else None

            if not success:
                failed_expectations.append(expectation_type)

        observed_values["record_count"] = record_count_value
        observed_values["column_count"] = column_count_value
        observed_values["numeric_process"] = mean_expectations_details
        observed_values["mandatory_columns"] = column_match_observed
        observed_values["pk_not_null_details"] = pk_not_null_details
        observed_values["pk_unique_details"] = pk_unique_details
        observed_values["year_check_details"] = year_check_details

        return summary, failed_expectations, observed_values

    summary, failed_expectations, observed_values = parse_results_for_summary(validation_results)

    mandatory_pass = True
    for etype in MANDATORY_EXPECTATIONS:
        column = EXPECTATION_TO_COLUMN.get(etype)
        if not column:
            raise ValueError(f"Missing column mapping for mandatory expectation: {etype}")
        value = summary.get(column)
        if etype == "expect_column_mean_to_be_between":
            if value not in ("True", None):
                mandatory_pass = False
                break
        else:
            if value != "True":
                mandatory_pass = False
                break

    overall_status = "Pass" if mandatory_pass else "Fail"

    source_results_dict = asdict(source_results)
    if "numeric_process" in source_results_dict and isinstance(source_results_dict["numeric_process"], list):
        means: List[Dict] = source_results_dict["numeric_process"]
        source_results_dict["numeric_process"] = {
            item.get("column"): item.get("mean") for item in means
        }
    
    source_results_dict["source_columns"] = extract_columns_with_types(source_information_schema)
    target_results_dict = observed_values
    target_results_dict["mandatory_columns"] = extract_columns_with_types(target_information_schema)

    fixed_fields = {
        "id": auto_increment(config),
        "success": "success",
        "source_stage": source_stage,
        "target_stage": target_stage,
        "source_table_name": source_table_name,
        "target_table_name": target_table_name,
        "overall_status": overall_status,
        "source_results": json.dumps(source_results_dict, indent=4),
        "target_results": json.dumps(target_results_dict, indent=4),
        "max_timestamp": max_timestamp,
        "created_at": datetime.now(timezone.utc)
    }

    final_data = {**fixed_fields, **summary}

    columns = [
    "id",
    "success",
    "source_stage",
    "target_stage",
    "source_table_name",
    "target_table_name",
    "overall_status",
    "record_count",            # 1
    "column_count",            # 2
    "numeric_process",         # 3
    "mandatory_columns",       # 4
    "pk_not_null",             # 5
    "pk_unique",               # 6
    "year_check",              # 7
    "source_results",
    "target_results",
    "max_timestamp",
    "created_at"
]

    values = [final_data.get(col) for col in columns]
    values_str = ", ".join(format_value(v) for v in values)

    query = f"""
    INSERT INTO bravo_sit.uat_summary_report (
        {", ".join(columns)}
    ) VALUES ({values_str});
    """

    with connection.connection_manager("holo", config=config, dbname='los') as con:
        con.execute_non_select_query(query)

    return overall_status, failed_expectations

def insert_report_error(source_stage: str, source_table_name: str,
                  target_stage: str, target_table_name: str):
    config = {
        "endpoint": os.getenv('endpoint_holo'),
        "port": os.getenv('port_holo'),
        "dbname": os.getenv('holo_dbname'),
        "user": os.getenv('shared_user'),
        "password": os.getenv('shared_password')
    }
    id = auto_increment(config=config)

    columns = [
        "id",
        "success",
        "source_stage",
        "target_stage",
        "source_table_name",
        "target_table_name",
        "created_at"
    ]
    created_at = datetime.now(timezone.utc)
    values = [id, 'error', source_stage, target_stage, source_table_name, target_table_name, created_at]
    values_str = ", ".join(format_value(v) for v in values)
    
    query = f"""
    INSERT INTO bravo_sit.uat_summary_report (
        {", ".join(columns)}
    ) VALUES ({values_str});
    """
    # print("Executing:", query)
    with connection.connection_manager("holo", config=config, dbname='los') as con:
        con.execute_non_select_query(query)
    
def print_result(results: list[ExpectationValidationResult]):
    for result in results:
        expectation = result["expectation_config"]["type"]
        success = result["success"]
        observed_value = result.get("result", {}).get("observed_value", "N/A")
        print(f"{expectation}: {success}, {observed_value}")

# def extract_html_path(docs: Dict[str, str], validation_result: ExpectationSuiteValidationResult, data_source_name: str, data_asset_name):
#     run_time = validation_result.meta.get("run_id").run_time
#     run_time_str = run_time.strftime("%Y%m%dT%H%M%S.%fZ")
#     local_site_url = docs["local_site"]
#     parsed_url = urlparse(local_site_url)
#     docs_dir_base = Path(parsed_url.path).parent

#     html_path = (
#         docs_dir_base 
#         / "validations" 
#         / "suite"
#         / "__none__" 
#         / run_time_str 
#         / f"{data_source_name}-{data_asset_name}.html"
#     )
#     return html_path

# def send_slack_notif(message_text: str):
#     message = {
#         "text": message_text
#     }

#     webhook_url = ""

#     response = requests.post(
#         url=webhook_url,
#         data=json.dumps(message),
#         headers={'Content-Type': 'application/json'}
#     )

#     if response.status_code != 200:
#         raise ValueError(f"Slack notification failed: {response.status_code}, {response.text}")

#------------------------------------------GX------------------------------------------
def execute_gx(source: str, source_table : str, 
               target: str, target_table : str, target_df : pd.DataFrame, target_pk : List[str],
               source_information_schema: pl.DataFrame, target_information_schema: pl.DataFrame,
               source_result: SourceResult,
               **kwargs) :
    data_source_name = target
    data_asset_name = target_table
    batch_definition_name = (f"{data_source_name}_{data_asset_name}")

    context = gx.get_context()

    try:
        data_source = context.data_sources.get(data_source_name)  
    except Exception:
        data_source = context.data_sources.add_pandas(data_source_name)
   
    try:
        data_asset =  data_source.get_asset(data_asset_name)
    except Exception:
        data_asset =  data_source.add_dataframe_asset(name=data_asset_name)

    try:
        batch_definition = data_asset.get_batch_definition(batch_definition_name)
    except Exception:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
        
    suite = context.suites.add_or_update(gx.ExpectationSuite(name="suite"))
    #------------------Comparison(Mandatory)------------------
    mandatory_columns = ['created_at', 'updated_at', 'is_deleted', 'deleted_at']
    if source == "odps" and target == "datalake": 
        source_column_count = source_result.column_count + 1
        mandatory_columns.append('dt')
    elif source == "pg" and target == "holo": source_column_count = source_result.column_count + 5
    else: source_column_count = source_result.column_count

    suite.add_expectation(expectation=gx.expectations.ExpectTableRowCountToEqual(value=source_result.record_count))
    suite.add_expectation(expectation=gx.expectations.ExpectTableColumnCountToEqual(value=source_column_count))
    
    if source_result.numeric_process:
        for numeric in source_result.numeric_process:
            suite.add_expectation(expectation=gx.expectations.ExpectColumnMeanToBeBetween(column=numeric["column"], 
                                                                                          min_value=numeric["mean"], 
                                                                                          max_value=numeric["mean"]))
        
    suite.add_expectation(expectation=gx.expectations.ExpectTableColumnsToMatchSet(column_set=mandatory_columns, exact_match=False))
    suite.add_expectation(expectation=gx.expectations.ExpectTableColumnsToMatchSet(column_set=source_result.source_columns, exact_match=False))
    
    #------------------------Profiling------------------------
    if target_pk:
        for pk in target_pk:
            suite.add_expectation(expectation=gx.expectations.ExpectColumnValuesToNotBeNull(column=pk))
            suite.add_expectation(expectation=gx.expectations.ExpectColumnValuesToBeUnique(column=pk))

    year_columns = kwargs.get("year_columns")
    current_year = datetime.now().year
    if year_columns:
        for column in year_columns:
            suite.add_expectation(expectation=gx.expectations.ExpectColumnValuesToBeBetween(column=column, 
                                                                                            min_value=current_year-100, 
                                                                                            max_value=current_year+100))

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="validation_definition"
    )
    
    try:
        validation_definition = context.validation_definitions.get(name=validation_definition.name)
    except Exception:
        validation_definition = context.validation_definitions.add(validation_definition)

    validation_result = validation_definition.run(batch_parameters={"dataframe" : target_df})

    # html_path = Path(extract_html_path(docs=context.build_data_docs(), 
    #                               validation_result=validation_result, 
    #                               data_source_name=data_source_name, 
    #                               data_asset_name=data_asset_name))

    # print(f"HTML path: {html_path}")
    # print(f"HTML exist: {html_path.exists()}")
    # pdf_path = html_path.with_suffix('.pdf')
    # pdfkit.from_file(str(html_path), str(pdf_path))
    print_result(validation_result.results)

    print("⚠️ SKIP INSERT REPORT (TEST MODE)")
    overall_status = "TEST_PASS"
    failed_expectations = []

    # overall_status, failed_expectations = insert_report(source_stage=source, source_table_name=source_table,
    #                             target_stage=target, target_table_name=target_table,
    #                             validation_results=validation_result.results,
    #                             source_results=source_result,
    #                             source_information_schema=source_information_schema, 
    #                             target_information_schema=target_information_schema,
    #                             max_timestamp=kwargs.get("max_timestamp"))
    
    failed_section = ""
    if failed_expectations:
        failed_items = "\n".join(f"- `{rule}`" for rule in failed_expectations)
        failed_section = f"\n*🚨 Failed Rules:*\n{failed_items}"
    
    
    
    message_text = (
        f"*Source:* *`{source}`-`{source_table}`*\n"
        f"*Target:* *`{target}`-`{target_table}`*\n"
        f"*Status:* *{overall_status.upper()}*\n"
        f"{failed_section}\n"
    )

    return message_text
