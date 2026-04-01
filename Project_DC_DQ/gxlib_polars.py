import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement

if not hasattr(sa, "ColumnElement"):
    sa.ColumnElement = ColumnElement  # shim for GX

import great_expectations as gx
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.exceptions import DataContextError
from great_expectations.expectations.metrics import TableRowCount
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
import pandas as pd
import polars as pl

import os
import lib
import copy
import warnings

from abc import ABC, abstractmethod
from urllib.parse import quote_plus
from typing import Any

# from datetime import datetime, timezone, time, timedelta
import time

warnings.filterwarnings("ignore", category=DeprecationWarning, module="odps.utils")


# ============================================================
# CONTEXT SINGLETON
# ============================================================

class GXContextSingleton:
    """ 
    Singleton wrapper for obtaining a Great Expectations Data Context.
    """

    _instance = None

    @classmethod
    def get_context(cls):
        if cls._instance is None:
            cls._instance = gx.get_context(mode="file")
            try:
                cls._instance.sources.add_pandas(name="pandas_runtime")
            except Exception:
                pass
        return cls._instance


# ============================================================
# ENV HELPER
# ============================================================

def get_env(key, default=None):
    return os.getenv(key, default)

# ============================================================
# GET SUITE
# ============================================================

def get_suite(table_name: str, stage: str):
    context = GXContextSingleton.get_context()
    name = f'{stage}_{table_name}_suite'
    try:
        return context.suites.get(name)
    except DataContextError:
        print(f"Suite for {stage}_{table_name} not found, use add_or_update() to add suite")
        return None


# ============================================================
# DUPLICATE SUITE
# ============================================================

def duplicate_suite(source_suite_name: str, target_suite_name: str):

    context = GXContextSingleton.get_context()
    source_suite = context.suites.get(name=source_suite_name)

    new_suite = copy.deepcopy(source_suite)
    new_suite.name = target_suite_name

    new_suite.meta = {
        **(new_suite.meta or {}),
        "cloned_from": source_suite_name,
    }

    context.suites.add_or_update(new_suite)

    print(f"Suite duplicated: {source_suite_name} → {target_suite_name}")


# ============================================================
# BASE HANDLER
# ============================================================

class GXHandler(ABC):

    def __init__(self, raw_table_name: str, table_name : str):
        self.raw_table_name = raw_table_name
        self.table_name = table_name

    @abstractmethod
    def get_asset(self, **kwargs):
        pass

    @abstractmethod
    def validate(self, table_asset, stage):
        pass

# ============================================================
# HOLOGRES HANDLER
# ============================================================

class HologresHandler(GXHandler):

    def get_asset(self, **kwargs) -> TableAsset:

        context = GXContextSingleton.get_context()

        schema = kwargs.get("schema") or lib.get_schema_holo(self.raw_table_name)
        # if not schema :
        #     if self.raw_table_name == "customer_cif":
        #         schema = "bravo_sit" # Hardcode spesifik untuk tabel ini
        #     else:
        #         schema = lib.get_schema_holo(self.raw_table_name)
        datasource_name = f"my_holo_datasource_{schema}"

        try:
            datasource: SQLDatasource = context.data_sources.get(datasource_name)
        except KeyError:

            connection_string = (
                f"postgresql+psycopg2://{get_env('user_holo')}:{get_env('password_holo')}"
                f"@{get_env('endpoint_holo')}:{get_env('port_holo')}/{get_env('database_holo')}"
            )
            datasource: SQLDatasource = context.data_sources.add_sql(
                name=datasource_name,
                connection_string=connection_string
            )

        # now_utc = datetime.now(timezone.utc)
        # print(now_utc)

        # utc_midnight = datetime.combine(now_utc.date(), time.min)
        # previous_midnight = utc_midnight - timedelta(days=1)

        asset_name = f'holo_{schema}_{self.raw_table_name}'

        try:
            table_asset: TableAsset = datasource.get_asset(asset_name)

        except KeyError:
            table_asset = datasource.add_table_asset(
                name=asset_name,
                table_name=self.table_name,
                schema_name=schema
            )

        return table_asset
    def validate(self, table_asset, stage):

        # print("[POSTGRES] Using SQL GX validation")

        context = GXContextSingleton.get_context()

        suite = get_suite(self.raw_table_name, stage)

        if suite is None:
            print("Suite not found")
            return None

        batch_request = table_asset.build_batch_request()

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        results = validator.validate(
            result_format={
                "result_format": "COMPLETE",
                "return_unexpected_rows": True
            }
        )

        return results.to_json_dict()


# ============================================================
# ODPS HANDLER
# ============================================================

class ODPSHandler(GXHandler):

    def get_asset(self, **kwargs):
        return None

    def validate(self, table_asset=None, stage=None, target_dt=None, **kwargs):
        from odps import ODPS
        from datetime import datetime, timedelta

        # ============================================================
        # CONFIG
        # ============================================================
        schema = kwargs.get("schema") or get_env("schema_odps")
        partition = kwargs.get("partition") or target_dt
        partition_col = kwargs.get("partition_col", "pt")
        start_time = time.time()

        odps = ODPS(
            get_env("user_odps"),
            get_env("password_odps"),
            get_env("project_odps"),
            endpoint=get_env("endpoint_odps"),
        )

        # ============================================================
        # BUILD QUERY
        # ============================================================
        if partition:
            query = f"""
            SELECT *
            FROM {schema}.{self.table_name}
            WHERE {partition_col} = '{partition}'
            """
        else:
            query = f"""
            SELECT *
            FROM {schema}.{self.table_name}
            """

        print("ODPS query:")
        print(query)

        # ============================================================
        # EXECUTE QUERY
        # ============================================================
        instance = odps.execute_sql(query)
        instance.wait_for_success()

        with instance.open_reader() as reader:
            pdf = reader.to_pandas()

        # ============================================================
        # CONVERT TO POLARS
        # ============================================================
        pl_df = pl.from_pandas(pdf)

        print("Polars Data Loaded:", pl_df.shape)

        # ============================================================
        # CHECK PARTITION (EMPTY)
        # ============================================================
        if partition and pl_df.height == 0:
            print(f"⚠️ Partition {partition} empty → fallback")

            prev_dt = (
                datetime.strptime(partition, "%Y%m%d") - timedelta(days=1)
            ).strftime("%Y%m%d")

            fallback_query = f"""
            SELECT *
            FROM {schema}.{self.table_name}
            WHERE {partition_col} = '{prev_dt}'
            """

            instance = odps.execute_sql(fallback_query)
            instance.wait_for_success()

            with instance.open_reader() as reader:
                pdf = reader.to_pandas()

            pl_df = pl.from_pandas(pdf)

            lib.send_gchat_alert(
                f"⚠️ {self.table_name} partition {partition} empty. Fallback to {prev_dt}"
            )

            partition = prev_dt

        # ============================================================
        # FINAL EMPTY CHECK
        # ============================================================
        if pl_df.height == 0:
            print("❌ No data available")
            lib.send_gchat_alert(
                f"❌ {self.table_name} has no data to validate"
            )
            return None
        # ============================================================
        # CONVERT BACK TO PANDAS FOR GX
        # ============================================================
        df = pl_df.to_pandas()

        # df = df.convert_dtypes()

        # # Fix numeric
        # for col in df.columns:
        #     if any(x in col.lower() for x in ["manufacturingyear", "nextinstallmentnumber", "tenor", "graceperiodlatecharges",
        #                                       "installmentreminder", "pastduedays", "installmentstepdown", "realtenor"]):
        #         df[col] = pd.to_numeric(df[col], errors="coerce")


        # ============================================================
        # GET GX SUITE
        # ============================================================
        suite = get_suite(self.raw_table_name, stage)

        if suite is None:
            print("❌ Suite not found")
            return None

        # ============================================================
        # VALIDATE
        # ============================================================
        validator = build_validator_from_df(df, suite)
        results = validator.validate()

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        print(f"⏱️ Validation Time: {duration} seconds")

        # ============================================================
        # FORMAT RESULT
        # ============================================================
        summary = results.statistics
        success = results.success

        total = summary.get("evaluated_expectations", 0)
        success_count = summary.get("successful_expectations", 0)
        failed_count = summary.get("unsuccessful_expectations", 0)

        success_pct = round((success_count / total) * 100, 2) if total else 0
        failed_pct = round((failed_count / total) * 100, 2) if total else 0

        status_icon = "✅ PASSED" if success else "❌ FAILED"

        message_lines = [
            "📊 Data Quality Check",
            f"Table: {self.table_name}",
            f"Stage: {stage}",
        ]

        if partition:
            message_lines.append(f"Partition: {partition}")

        message_lines += [
            f"Status: {status_icon}",
            "",
            f"Total Expectations: {total}",
            f"Success: {success_count} ({success_pct}%)",
            f"Failed: {failed_count} ({failed_pct}%)",
            f"⏱️ Duration: {duration} sec"
        ]

        # ============================================================
        # FAILED DETAIL
        # ============================================================
        failed = []

        for res in results.results:
            if not res.success:
                exp_type = res.expectation_config.type
                column = res.expectation_config.kwargs.get("column", "table_level")

                sample = res.result.get("unexpected_list", [])
                sample = sample[:3] if sample else []

                if sample:
                    failed.append(f"- {column} → {exp_type} | sample: {sample}")
                else:
                    failed.append(f"- {column} → {exp_type}")

        if failed:
            message_lines.append("\n❗ Failed Checks:")
            message_lines.extend(failed[:20])  # limit biar ga 400 error

        message_text = "\n".join(message_lines)

        # limit panjang message
        if len(message_text) > 3500:
            message_text = message_text[:3500] + "\n... (truncated)"

        # ============================================================
        # SEND ALERT
        # ============================================================
        try:
            if not success:
                lib.send_gchat_alert(message_text)
        except Exception as e:
            print(f"⚠️ Failed to send GChat alert: {e}")

        # ============================================================
        # RETURN RESULT
        # ============================================================
        return results.to_json_dict()


# ============================================================
# TRINO HANDLER
# ============================================================

# class TrinoHandler(GXHandler):

#     def get_asset(self, **kwargs):

#         context = GXContextSingleton.get_context()

#         schema = kwargs.get('schema')
#         partition = kwargs.get('partition')

#         connection_string = (
#             f"trino://{get_env('user_datalake')}:"
#             f"{quote_plus(get_env('password_datalake'))}"
#             f"@{get_env('endpoint_datalake')}:"
#             f"{get_env('port_datalake')}/hive"
#         )

#         try:

#             datasource = context.data_sources.get(
#                 name=f'my_trino_datasource_{schema}'
#             )

#         except KeyError:

#             connect_args = {
#                 "http_scheme": 'https',
#                 "verify": False,
#             }

#             datasource: SQLDatasource = context.data_sources.add_sql(
#                 name=f'my_trino_datasource_{schema}',
#                 connection_string=connection_string,
#                 kwargs={"connect_args": connect_args}
#             )

#         asset_name = f"trino_{self.raw_table_name}"

#         try:

#             lib.create_view(self.raw_table_name, schema, partition)

#             table_asset = datasource.get_asset(asset_name)

#         except Exception:

#             lib.create_view(self.raw_table_name, schema, partition)

#             print(f"Adding new table asset")

#             table_asset = datasource.add_table_asset(
#                 name=asset_name,
#                 table_name=self.raw_table_name,
#                 schema_name='raw_data_dev_cloud'
#             )

#         return table_asset


#     def validate(
#         self,
#         table_asset: TableAsset,
#         stage: str,
#         schema: str,
#         target_dt: str | None = None,
#         bd_name: str = "by_dt_daily",
#     ):

#         context = GXContextSingleton.get_context()

#         suite = get_suite(
#             table_name=self.raw_table_name,
#             stage=stage
#         )

#         if suite is None:
#             return None

#         ymd = _ymd_from_target(target_dt)

#         try:

#             if ymd:

#                 try:
#                     table_asset.get_batch_definition(bd_name)

#                 except KeyError:
#                     table_asset.add_batch_definition_daily(
#                         name=bd_name,
#                         column="__dt_date"
#                     )

#                 y, m, d = ymd

#                 bd = table_asset.get_batch_definition(bd_name)

#                 try:

#                     batch = bd.get_batch(
#                         batch_parameters={
#                             "year": y,
#                             "month": m,
#                             "day": d
#                         }
#                     )

#                 except:

#                     message_text = f'{table_asset.name} does not have partition: {target_dt}\n'

#                     # Google Chat alert disabled
#                     # lib.send_gchat_alert(message_text=message_text)

#                     return None

#                 validator = context.get_validator(
#                     batch=batch,
#                     expectation_suite=suite
#                 )

#                 return validator.validate(
#                     result_format={
#                         "result_format" : "COMPLETE",
#                         "return_unexpected_rows" : True
#                     }
#                 )

#             batch_request = table_asset.build_batch_request()

#             validator = context.get_validator(
#                 batch_request=batch_request,
#                 expectation_suite=suite
#             )

#             return validator.validate(
#                 result_format={
#                     "result_format" : "COMPLETE",
#                     "return_unexpected_rows" : True
#                 }
#             )

#         finally:

#             try:
#                 lib.delete_view(self.raw_table_name, schema)

#             except Exception as e:
#                 print(f"View cleanup failed: {e}")


# ============================================================
# SQL SERVER HANDLER
# ============================================================

# class SqlServerHandler(GXHandler):

#     def get_asset(self, **kwargs):

#         context = GXContextSingleton.get_context()

#         server_dwh = get_env('server_dwh')
#         database_dwh = get_env('database_dwh')

#         def build_odbc_conn_str():

#             return (
#                 "DRIVER={ODBC Driver 18 for SQL Server};"
#                 f"SERVER={server_dwh};"
#                 f"DATABASE={database_dwh};"
#                 "Trusted_Connection=yes;"
#                 "TrustServerCertificate=yes;"
#             )

#         connection_string = (
#             "mssql+pyodbc:///?odbc_connect=" + quote_plus(build_odbc_conn_str())
#         )

#         try:

#             datasource = context.data_sources.get(
#                 name=f'my_dwh_datasource_{database_dwh}'
#             )

#         except:

#             datasource: SQLDatasource = context.data_sources.add_sql(
#                 name=f'my_dwh_datasource_{database_dwh}',
#                 connection_string=connection_string
#             )

#         asset_name = f'dwh_{self.raw_table_name}'

#         try:
#             table_asset = datasource.get_asset(asset_name)

#         except Exception:

#             print(f"Adding new table asset")

#             table_asset = datasource.add_table_asset(
#                 name=asset_name,
#                 table_name=f'{self.raw_table_name}'
#             )

#         return table_asset


# ============================================================
# HELPER
# ============================================================

def get_expectation_with_row_condition():

    expectation_name_list = []

    context = GXContextSingleton.get_context()

    suites = context.suites.all()

    for suite in suites:

        for exp in suite.expectations:

            jsonnn = exp._to_normalized_self_dict()

            if 'row_condition' in jsonnn:

                if jsonnn['row_condition'] is not None:
                    expectation_name_list.append(exp.meta['name'])

    print(expectation_name_list)


def _ymd_from_target(target_dt: str | None) -> tuple[int, int, int] | None:

    if not target_dt:
        return None

    return (
        int(target_dt[:4]),
        int(target_dt[4:6]),
        int(target_dt[6:8])
    )

def build_validator_from_df(df, suite):

    from great_expectations.validator.validator import Validator
    from great_expectations.execution_engine import PandasExecutionEngine
    from great_expectations.core.batch import Batch

    validator = Validator(
        execution_engine=PandasExecutionEngine(),
        batches=[Batch(data=df)],
        expectation_suite=suite,
    )

    return validator
# ============================================================
# HANDLER FACTORY
# ============================================================

HANDLER_REGISTRY = {
    "postgres": HologresHandler,
    "odps": ODPSHandler,
}


def create_gxhandler(stage, raw_table_name, table_name):
    handler_class = HANDLER_REGISTRY.get(stage)

    if handler_class is None:
        raise ValueError(f"Unsupported stage: {stage}")

    print("Creating handler:", handler_class.__name__)

    return handler_class(raw_table_name, table_name)