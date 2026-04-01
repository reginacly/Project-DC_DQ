import os
import yaml
import great_expectations as gx
from odps import ODPS
from github import Github
from dotenv import load_dotenv

from GX.custom_exp.expect_column_values_to_not_contain_special_characters import ExpectColumnValuesToNotContainSpecialCharacters
from GX.custom_exp.expect_column_values_to_be_email import ExpectColumnValuesToBeEmail
from GX.custom_exp.expect_column_values_to_not_have_double_space import ExpectColumnValuesToNotHaveDoubleSpace
from GX.custom_exp.expect_column_values_to_be_indonesian_phone_number import ExpectColumnValuesToMatchIndonesianPhoneRules


from exp_manager import METRIC_REGISTRY

load_dotenv()

# =========================
# CONFIG
# =========================
TABLE_NAME = "customer_cif_odps"
SCHEMA = "wsbfi_3_dev"
CONTRACT_ID = "customer_cif_contract"

# =========================
# LOAD CONTRACT
# =========================
def load_contract():
    token = os.getenv("GITHUB_TOKEN")
    g = Github(token) if token else Github()

    repo = g.get_repo("reginacly/data-contracts")
    file = repo.get_contents(f"example/{CONTRACT_ID}.yaml")

    contract = yaml.safe_load(file.decoded_content.decode())

    print("✅ Contract loaded:", CONTRACT_ID)
    return contract


# =========================
# FETCH DATA FROM ODPS
# =========================
def fetch_data():
    odps = ODPS(
        os.getenv("user_odps"),
        os.getenv("password_odps"),
        os.getenv("project_odps"),
        endpoint=os.getenv("endpoint_odps"),
    )

    query = f"""
    SELECT *
    FROM {SCHEMA}.{TABLE_NAME}
    LIMIT 1000
    """

    print("\n📥 Fetching data from ODPS...")

    instance = odps.execute_sql(query)
    instance.wait_for_success()

    with instance.open_reader() as reader:
        df = reader.to_pandas()

    print("✅ Data loaded:", df.shape)
    return df


# =========================
# BUILD VALIDATOR + EXPECTATIONS
# =========================
def build_validator(contract, df):

    from great_expectations.validator.validator import Validator
    from great_expectations.execution_engine import PandasExecutionEngine
    from great_expectations.core.batch import Batch
    from great_expectations.core.expectation_suite import ExpectationSuite

    validator = Validator(
        execution_engine=PandasExecutionEngine(),
        batches=[Batch(data=df)],
        expectation_suite=ExpectationSuite(name="tmp_suite"),
    )

    print("\n⚙️ Building expectations from contract...\n")

    total = 0

    for table in contract.get("schema", []):
        for prop in table.get("properties", []):

            column = prop["name"]

            for rule in prop.get("quality", []):

                metric = rule["metric"]
                args = rule.get("arguments", {})

                if metric not in METRIC_REGISTRY:
                    print(f"⚠️ Metric not registered: {metric}")
                    continue

                try:
                    exp_func, params = METRIC_REGISTRY[metric](column, args)

                    # ✅ INI YANG BENAR
                    exp_func(validator, **params)

                    total += 1

                except Exception as e:
                    print(f"❌ Failed {metric} on {column}: {e}")

    print(f"\n✅ Total expectations built: {total}")
    return validator


# =========================
# RUN VALIDATION
# =========================
def run_validation(validator):

    print("\n🚀 Running validation...\n")

    results = validator.validate()

    return results


# =========================
# PRINT RESULTS
# =========================
def print_results(results):

    print("\n=========== VALIDATION RESULTS ===========\n")

    print("Success:", results["success"])
    print("Evaluated:", results["statistics"]["evaluated_expectations"])

    for r in results["results"]:

        cfg = r.get("expectation_config", {})
        res = r.get("result", {})

        column = cfg.get("kwargs", {}).get("column")
        exp = cfg.get("expectation_type")

        print({
            "column": column,
            "expectation": exp,
            "success": r.get("success"),
            "unexpected_count": res.get("unexpected_count"),
            "unexpected_percent": res.get("unexpected_percent"),
        })


# =========================
# MAIN
# =========================
def main():

    print("\n========= ODPS GX FINAL TEST =========\n")

    contract = load_contract()

    df = fetch_data()

    validator = build_validator(contract, df)

    results = run_validation(validator)

    print_results(results)


if __name__ == "__main__":
    main()