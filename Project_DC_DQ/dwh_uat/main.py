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
import contextlib
import psutil
import os
from typing import Dict
from report import generate_report_png

load_dotenv()

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="odps.utils")
warnings.filterwarnings("ignore", message=".*metric column.*is being registered with different metric_provider.*")

def print_memory_usage():
    """Print current memory usage of the process"""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"Current memory usage: {memory_mb:.2f} MB")

@contextlib.contextmanager
def cleanup_dataframes(*df_vars):
    """Context manager to ensure dataframes are properly cleaned up"""
    try:
        yield
    finally:
        for var_name in df_vars:
            if var_name in locals() or var_name in globals():
                try:
                    if var_name in locals():
                        del locals()[var_name]
                    if var_name in globals():
                        del globals()[var_name]
                except:
                    pass
        gc.collect()

def process_single_row(data: Dict, row_index, total_rows):
    """Process a single row with proper memory management"""
    source_df = None
    target_df = None
    source_information_schema = None
    missing_in_target=[]
    missing_in_source=[]
    
    try:
        print(f"Processing row {row_index}/{total_rows}...")
        
        start_time = time.time()
        
        source = connection.get_connection_info(data.get("source_stage"))
        target = connection.get_connection_info(data.get("target_stage"))
        
        SCHEMA_DEFAULTS = {
            'pg': 'public',
            'holo': 'bravo_sit',
            'odps': 'wsbfi_3_dev',
            'datalake': 'raw_data_dev_cloud'
        }

        source_schema = data.get("source_schema") or SCHEMA_DEFAULTS.get(source["stage"])
        target_schema = data.get("target_schema") or SCHEMA_DEFAULTS.get(target["stage"])

        table_name = data.get("holo_table_name")
        
        source_table_name = dqlib.prefix_table_name(stage=source["stage"], table_name=table_name)
        target_table_name = dqlib.prefix_table_name(stage=target["stage"], table_name=table_name)
        target_pk = data.get("pk")
        pk_list = [col.strip() for col in target_pk.split(",")] if target_pk else []

        source_information_schema = dqlib.get_information_schema(stage=source["stage"],
                                                                table_name=source_table_name,
                                                                schema_name=source_schema,
                                                                config=source)
        
        check_column = dqlib.check_column(source_information_schema)
        if not check_column:
            dqlib.insert_report_error(
                source_stage=source["stage"],
                target_stage=target["stage"],
                source_table_name=source_table_name,
                target_table_name=target_table_name
        )
        
        max_timestamp = dqlib.get_max_timestamp(source_stage=source["stage"],
                                               holo_dbname=table_name)
        
        source_df = dqlib.get_df(stage=source["stage"],
                               table_name=source_table_name,
                               schema_name=source_schema,
                               config=source,
                               df_type=pl.DataFrame,
                               max_timestamp=max_timestamp)
        
        
        
        target_information_schema = dqlib.get_information_schema(stage=target["stage"],
                                                                table_name=target_table_name,
                                                                schema_name=target_schema,
                                                                config=target)
        
        year_columns = dqlib.has_year(source_information_schema.select(pl.col("column_name")).to_series().to_list())
        
        source_result = SourceResult(record_count=source_df.height,
                                   column_count=source_df.width,
                                   numeric_process=dqlib.get_mean_sample(table_name=source_df,
                                                                       information_schema=source_information_schema,
                                                                       pk=pk_list),
                                    source_columns=source_information_schema["column_name"].to_list())
        
        target_df = dqlib.get_df(stage=target["stage"],
                               table_name=target_table_name,
                               schema_name=target_schema,
                               config=target,
                               df_type=pd.DataFrame,
                               max_timestamp=max_timestamp)
        if source_df is not None:
            del source_df
            source_df = None
            gc.collect()
            print_memory_usage()

        source_cols = set(source_information_schema["column_name"].to_list())

        target_cols = set([col.lower() for col in target_df.columns])

        missing_in_target = list(source_cols - target_cols)
        missing_in_source = list(target_cols - source_cols)

        message_text = dqlib.execute_gx(source=source["stage"], 
                                      source_table=source_table_name,
                                      target=target["stage"], 
                                      target_table=target_table_name,
                                      target_df=target_df,
                                      target_pk=pk_list,
                                      source_result=source_result,
                                      source_information_schema=source_information_schema,
                                      target_information_schema=target_information_schema,
                                      max_timestamp=max_timestamp,
                                      year_columns=year_columns)

        generate_report_png(
            source_result=source_result,
            target_df=target_df,
            source_information_schema=source_information_schema,
            target_information_schema=target_information_schema,
            output_path=f"{table_name}.png",
            missing_in_source=missing_in_source,
            missing_in_target=missing_in_target
        )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        notif = (f"{message_text}\n"
                f"Runtime: {elapsed_time:.4f}s")

        if hasattr(dqlib, "send_slack_notif"):
            dqlib.send_slack_notif(notif)
        else:
            print(notif)
        # print(f"✅ Row {row_index} completed successfully")
        print_memory_usage()
        
        return True, None

    except Exception as e:
        error_details = traceback.format_exc()
        
        try:
            source_stage = source["stage"] if 'source' in locals() and source else data.get("source_stage", "Unknown")
            source_table = source_table_name if 'source_table_name' in locals() else data.get("source_table_name", "Unknown")
            target_stage = target["stage"] if 'target' in locals() and target else data.get("target_stage", "Unknown")
            target_table = target_table_name if 'target_table_name' in locals() else data.get("target_table_name", "Unknown")
        except:
            source_stage = data.get("source_stage", "Unknown")
            source_table = data.get("source_table_name", "Unknown")
            target_stage = data.get("target_stage", "Unknown")
            target_table = data.get("target_table_name", "Unknown")
        
        error_notif = (f"🚨*ERROR in Row {row_index}*🚨\n"
                      f"*Source   :`{source_stage}`-`{source_table}`*\n"
                      f"*Target   :`{target_stage}`-`{target_table}`*\n"
                      f"*Exception:*{str(e)}\n"
                      f"{error_details}")
        
        if hasattr(dqlib, "send_slack_notif"):
            dqlib.send_slack_notif(error_notif)
        else:
            print(error_notif)
        print(f"❌ Row {row_index} failed: {str(e)}")
        
        try:
            dqlib.insert_report_error(source_stage=source_stage,
                                      target_stage=target_stage,
                                      source_table_name=source_table,
                                      target_table_name=target_table,
                                      success='error')
        except Exception as insert_error:
            print(f"Failed to insert error report: {insert_error}")
        
        return False, str(e)
    
    finally:
        cleanup_vars = [source_df, target_df, source_information_schema]
        for var in cleanup_vars:
            if var is not None:
                try:
                    del var
                except:
                    pass
        
        gc.collect()
        print_memory_usage()

def main():
    try:
        print_memory_usage()
        
        with open(Path("C:/Users/5002684/Downloads/dwh_uat/uat_template.csv"), mode='r', newline='') as file:
            reader = csv.DictReader(file, delimiter=';')
            payload = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]
        
        total_rows = len(payload)
        successful_rows = 0
        failed_rows = 0
        
        for row_index, data in enumerate(payload, start=1):
            success, error = process_single_row(data, row_index, total_rows)
            
            if success:
                successful_rows += 1
            else:
                failed_rows += 1
            
            if row_index % 10 == 0:
                gc.collect()
                # print(f"--- Periodic cleanup after row {row_index} ---")
                print_memory_usage()
        
        summary_notif = (f"📊 *Processing Summary* 📊\n"
                        f"Total rows processed: {total_rows}\n"
                        f"✅ Successful: {successful_rows}\n"
                        f"❌ Failed: {failed_rows}\n"
                        f"Success rate: {(successful_rows/total_rows)*100:.1f}%")
        
        if hasattr(dqlib, "send_slack_notif"):
            dqlib.send_slack_notif(summary_notif)
        else:
            print(summary_notif)
        print(f"\n{summary_notif}")
        print("--- Final Summary ---")
        print_memory_usage()

    except Exception as e:
        error_details = traceback.format_exc()
        setup_error_notif = (f"🚨*SETUP ERROR*🚨\n"
                            f"Failed to read CSV file or initialize processing\n"
                            f"*Exception:* {str(e)}\n"
                            f"{error_details}")
        
        try:
            dqlib.send_slack_notif(setup_error_notif)
        except:
            print("Failed to send Slack notification for setup error")
        
        print(f"Setup error: {str(e)}")

if __name__ == "__main__":
    main()