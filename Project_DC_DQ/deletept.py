from dotenv import load_dotenv
load_dotenv()

from connection import TrinoConnection

def delete_partition(dt: str):

    delete_summary_query = f"""
    DELETE FROM data_analytics_sandbox.data_quality_summary_report
    WHERE dt = '{dt}'
    """

    delete_expectation_query = f"""
    DELETE FROM data_analytics_sandbox.data_quality_expectation_result
    WHERE dt = '{dt}'
    """

    trino = TrinoConnection()

    try:

        trino.connect(schema_datalake="data_analytics_sandbox")

        print(f"\nDeleting summary_report partition dt={dt}...")
        trino.execute_non_select_query(delete_summary_query)

        print(f"\nDeleting expectation_result partition dt={dt}...")
        trino.execute_non_select_query(delete_expectation_query)

        print("\n✅ Partition deleted successfully")

    except Exception as e:
        print("❌ Failed deleting partition")
        print(str(e))
    
    finally:
        trino.close()


if __name__ == "__main__":

    # target partition
    dt_partition = "20260331"

    delete_partition(dt_partition)