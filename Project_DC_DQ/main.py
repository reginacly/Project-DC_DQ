import lib
import gxlib
from exp_manager import add_or_update_suite
from dotenv import load_dotenv
import traceback
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import datahub_handler as dh
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

load_dotenv()

def main():

    raw_table_name = "fea_thm_debt_to_income_ratio"
    now_wib = datetime.now(ZoneInfo("Asia/Jakarta"))
    stage_list = ["trino"]
    # partition = now_wib.strftime("%Y%m%d") 
    partition = '20260330'
    dataset_urn = 'urn:li:dataset:(urn:li:dataPlatform:odps,bravo.default.dbo_collection__agreement_juris_masking,PROD)'

    print("\n========== START DATA QUALITY CHECK ==========\n")

    for stage in stage_list:

            print(f"\n===== VALIDATING {raw_table_name} ON {stage.upper()} =====")

            # =====================================
            # TABLE NAME MAPPING
            # =====================================

            if stage == "postgres":
                table_name = raw_table_name

            elif stage == "odps":
                table_name = f"{raw_table_name}"

            elif stage == "trino":
                table_name = f"{raw_table_name}"

            # elif stage == "dwh":
            #     table_name = lib.get_dwh_table_name(raw_table_name)

            else:
                raise RuntimeError(f"Unsupported stage {stage}")

            print("Table used:", table_name)

            # =====================================
            # SCHEMA MAPPING
            # =====================================

            if stage == "postgres":
                schema = lib.get_schema_holo(raw_table_name)

            elif stage == "odps":
                schema = os.getenv("schema_odps")

            elif stage == "trino":
                schema = os.getenv("schema_datalake")

            # elif stage == "dwh":
            #     schema = None

            print("Schema:", schema)

            # =====================================
            # COLUMN LIST
            # =====================================

            column_list = lib.get_column_list(table_name, schema)

            if not column_list:
                raise RuntimeError(f"No columns found for table {raw_table_name}")

            print("Column count:", len(column_list))

            # =====================================
            # CREATE SUITE
            # =====================================

            suite = add_or_update_suite(
                raw_table_name,
                stage,
                column_list
            )

            # =====================================
            # CREATE HANDLER
            # =====================================

            handler = gxlib.create_gxhandler(
            stage=stage,
            raw_table_name=raw_table_name,
            table_name=table_name
            )

            # =====================================
            # GET TABLE ASSET
            # =====================================

            table_asset = handler.get_asset(
                schema=schema,
                partition=partition
            )

            # =====================================
            # VALIDATE
            # =====================================

            print("\nRunning validation...\n")
            if stage == "postgres" :
                results = handler.validate(
                    table_asset,
                    stage)
            elif stage == "odps":
                results = handler.validate(
                    stage=stage,
                    schema=schema,
                    partition=partition,
                    partition_col="pt"   # ganti kalau nama partisinya bukan pt
                )
            elif stage == "trino":
                results = handler.validate(
                    table_asset=table_asset,
                    stage=stage,
                    schema=schema,
                    target_dt=partition
                )
            else:
                table_asset = handler.get_asset(schema=schema)
                results = handler.validate(
                    table_asset=table_asset,
                    stage=stage
                )

            # =====================================
            # COMPILE RESULT
            # =====================================

            validation_result, exp_rows = lib.compile_result(
                results,
                table_name,
                schema
            )

            # =====================================
            # INSERT SUMMARY
            # =====================================

            run_id = lib.insert_summary_report(
                validation_result,
                table_name,
                stage

            )

            # =====================================
            # INSERT EXPECTATION RESULTS
            # =====================================

            lib.insert_expectation_results_for_table(
                stage,
                table_name,
                exp_rows,
                run_id
            )

            # =====================================
            # PUSH TO DATAHUB
            # =====================================
            try:

                print("Dataset URN:", dataset_urn)

                # upsert assertion definitions from GX suite
                dh.upsert_assertion_for_suite(
                    stage=stage,
                    table_name=raw_table_name,
                    urn=dataset_urn
                )

                # report validation result per expectation
                for row in exp_rows:
                    expectation_name = (
                        row.get("expectation_name")
                        or row.get("expectation")
                        or row.get("name")
                    )

                    if not expectation_name:
                        print("⚠️ Skip DataHub result: expectation_name not found in exp_rows")
                        continue

                    success_value = row.get("success")

                    if success_value is True:
                        status = "SUCCESS"
                    elif success_value is False:
                        status = "FAILURE"
                    else:
                        status = "ERROR"

                    dh.report_result(
                        stage=stage,
                        table_name=raw_table_name,
                        expectation_name=expectation_name,
                        status=status,
                        props={
                            "run_id": str(run_id),
                            "table_name": table_name,
                            "raw_table_name": raw_table_name,
                            "schema": schema,
                            "stage": stage,
                            "dimension": row.get("dimension", ""),
                            "severity": row.get("severity", ""),
                            "column": row.get("column", ""),
                            "unexpected_count": row.get("unexpected_count", 0),
                            "unexpected_percent": row.get("unexpected_percent", 0),
                            "success_percent": row.get("success_percent", 0),
                            "partition": partition if stage == "odps" else "",
                        },
                    )
                print(f"✅ DataHub reporting finished for {table_name}")

            except Exception as e:
                print(f"⚠️ DataHub push failed for {table_name}: {e}")

    print("\n========== FINISHED ==========\n")

if __name__ == "__main__":
    main()