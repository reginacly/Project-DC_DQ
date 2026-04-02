import time
import traceback
import dqlib
import connection
import polars as pl
import pandas as pd
from dqlib import SourceResult
import csv
import gc
from pathlib import Path
from dotenv import load_dotenv
import psutil
import os
from typing import Dict
from report import generate_report_png

load_dotenv()

TEST_MODE = True  # 🔥 switch utama

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="odps.utils")

# =========================
# MEMORY MONITOR
# =========================
def print_memory_usage():
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"Memory usage: {memory_mb:.2f} MB")


# =========================
# PROCESS PER ROW
# =========================
def process_single_row(data: Dict, row_index, total_rows):
    source_df = None
    target_df = None
    source_information_schema = None
    target_information_schema = None
    
    try:
        print(f"\n🚀 Processing row {row_index}/{total_rows}")
        start_time = time.time()

        # =========================
        # CONNECTION
        # =========================
        source = connection.get_connection_info(data.get("source_stage"))
        target = connection.get_connection_info(data.get("target_stage"))

        SCHEMA_DEFAULTS = {
            'holo': 'bravo_bau',
            'odps': 'wsbfi_3_dev',
            'datalake': 'raw_data_prod_cloud',
            'dwh': 'dbo'
        }

        source_schema = data.get("source_schema") or SCHEMA_DEFAULTS.get(source["stage"])
        target_schema = data.get("target_schema") or SCHEMA_DEFAULTS.get(target["stage"])

        table_name = data.get("holo_table_name")
        if not table_name:
            raise ValueError("holo_table_name is required")

        source_table_name = dqlib.prefix_table_name(source["stage"], table_name)
        target_table_name = dqlib.prefix_table_name(target["stage"], table_name)

        pk_list = [col.strip() for col in data.get("pk", "").split(",") if col.strip()]

        print(f"📊 Table: {table_name}")

        # =========================
        # GET SCHEMA
        # =========================
        source_information_schema = dqlib.get_information_schema(
            source["stage"], source_table_name, source_schema, source
        )

        target_information_schema = dqlib.get_information_schema(
            target["stage"], target_table_name, target_schema, target
        )

        # normalize column names
        source_information_schema = source_information_schema.with_columns(
            pl.col("column_name").str.to_lowercase()
        )
        target_information_schema = target_information_schema.with_columns(
            pl.col("column_name").str.to_lowercase()
        )

        # =========================
        # LOAD SOURCE
        # =========================
        print("📥 Loading SOURCE...")
        source_df = dqlib.get_df(
            stage=source["stage"],
            table_name=source_table_name,
            schema_name=source_schema,
            config=source,
            df_type=pl.DataFrame
        )

        print(f"Source shape: {source_df.shape}")

        source_result = SourceResult(
            record_count=source_df.height,
            column_count=source_df.width,
            numeric_process=dqlib.get_mean_sample(
                table_name=source_df,
                information_schema=source_information_schema,
                pk=pk_list
            ),
            source_columns=source_information_schema["column_name"].to_list()
        )

        # cleanup source
        del source_df
        gc.collect()

        # =========================
        # LOAD TARGET
        # =========================
        print("📥 Loading TARGET...")
        target_df = dqlib.get_df(
            stage=target["stage"],
            table_name=target_table_name,
            schema_name=target_schema,
            config=target,
            df_type=pd.DataFrame
        )

        print(f"Target shape: {target_df.shape}")

        # =========================
        # MISSING COLUMN CHECK
        # =========================
        source_cols = set(source_information_schema["column_name"].to_list())
        target_cols = set([col.lower() for col in target_df.columns])

        missing_in_target = list(source_cols - target_cols)
        missing_in_source = list(target_cols - source_cols)

        print(f"Missing in target: {missing_in_target}")
        print(f"Missing in source: {missing_in_source}")

        # =========================
        # RUN GX (ONLY IF NOT TEST)
        # =========================
        if not TEST_MODE:
            print("🔍 Running Data Quality Check...")
            message_text = dqlib.execute_gx(
                source=source["stage"],
                source_table=source_table_name,
                target=target["stage"],
                target_table=target_table_name,
                target_df=target_df,
                target_pk=pk_list,
                source_result=source_result,
                source_information_schema=source_information_schema,
                target_information_schema=target_information_schema
            )
            print(message_text)

        # =========================
        # GENERATE PNG REPORT
        # =========================
        OUTPUT_DIR=Path("REPORT")
        output_file = OUTPUT_DIR/f"{table_name}.png"

        generate_report_png(
            source_result=source_result,
            target_df=target_df,
            source_information_schema=source_information_schema,
            target_information_schema=target_information_schema,
            output_path=output_file
        )

        print(f"🖼 PNG generated: {output_file}")

        # =========================
        # FINAL LOG
        # =========================
        elapsed = time.time() - start_time
        print(f"⏱ Runtime: {elapsed:.2f}s")

        print_memory_usage()

        return True, None

    except Exception as e:
        print(f"\n❌ ERROR Row {row_index}: {str(e)}")
        print(traceback.format_exc())
        return False, str(e)

    finally:
        try:
            del source_df
        except:
            pass
        try:
            del target_df
        except:
            pass
        try:
            del source_information_schema
        except:
            pass
        try:
            del target_information_schema
        except:
            pass

        gc.collect()
        print_memory_usage()


# =========================
# MAIN
# =========================
def main():
    try:
        print("🚀 START TEST MODE")
        print_memory_usage()

        with open(Path("C:/Users/5002684/Downloads/dwh_uat/uat_template.csv"), newline='') as file:
            reader = csv.DictReader(file, delimiter=';')
            payload = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]

        total_rows = len(payload)

        for idx, data in enumerate(payload, start=1):
            process_single_row(data, idx, total_rows)

        print("\n✅ TEST SELESAI")

    except Exception as e:
        print("❌ SETUP ERROR")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()