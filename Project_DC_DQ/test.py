import great_expectations as gx
import yaml
import json
import os
import lib
import gxlib

from github import Github
from dotenv import load_dotenv

import gxlib
import lib
from exp_manager import METRIC_REGISTRY

load_dotenv()

TABLE_NAME = "customer_cif"
STAGE = "trino"
CONTRACT_ID = "customer_cif_contract"
SCHEMA = "bravo_sit"


# =====================================================
# LOAD DATA CONTRACT
# =====================================================

def load_contract(contract_id):

    token = os.getenv("GITHUB_TOKEN")

    g = Github(token) if token else Github()

    repo = g.get_repo("reginacly/data-contracts")

    file = repo.get_contents(f"example/{contract_id}.yaml")

    return yaml.safe_load(file.decoded_content.decode())


# =====================================================
# BUILD GX SUITE
# =====================================================

def build_suite(contract):

    context = gx.get_context()

    suite_name = f"debug_{TABLE_NAME}_suite"

    suite = gx.ExpectationSuite(name=suite_name)

    DEFAULT_RESULT_FORMAT = {
        "result_format": "COMPLETE",
        "return_unexpected_rows": True
    }

    print("\nBUILDING EXPECTATION SUITE...\n")

    for table in contract.get("schema", []):

        for prop in table.get("properties", []):

            column = prop["name"]

            for rule in prop.get("quality", []):

                metric = rule["metric"]
                args = rule.get("arguments", {})

                severity = rule.get("severity", "warning")
                dimension = rule.get("dimension", "correctness")

                exp_cls, params = METRIC_REGISTRY[metric](column, args)

                print(f"ADD EXPECTATION → {metric} ({column})")

                suite.add_expectation(
                    exp_cls(
                        **params,
                        result_format=DEFAULT_RESULT_FORMAT,
                        meta={
                            "name": f"{column}-{exp_cls.__name__}",
                            "dimension": dimension
                        },
                        severity=severity
                    )
                )

    context.suites.add_or_update(suite)

    print("\nTOTAL EXPECTATIONS:", len(suite.expectations))

    return suite_name


# =====================================================
# RUN VALIDATION
# =====================================================

def run_validation(suite_name):

    column_list = lib.get_column_list(TABLE_NAME, SCHEMA)

    print("\nCOLUMNS IN TABLE:")
    print(column_list)

    handler = gxlib.create_gxhandler(
        stage=STAGE,
        table_name=TABLE_NAME
    )

    asset = handler.get_asset(schema=SCHEMA)

    context = gx.get_context()

    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
        expectation_suite_name=suite_name
    )

    print("\nRUNNING VALIDATION...\n")

    results = validator.validate()

    return results.to_json_dict()


# =====================================================
# DEBUG RESULT
# =====================================================

def print_results(results):

    print("\n=========== GX VALIDATION RESULTS ===========\n")

    gx_results = results.get("results", [])

    print("TOTAL RESULTS:", len(gx_results))

    for r in gx_results:

        cfg = r.get("expectation_config", {})
        res = r.get("result", {})

        column = cfg.get("kwargs", {}).get("column")
        exp = cfg.get("type")

        success = r.get("success")

        observed = (
            res.get("observed_value")
            or res.get("unexpected_percent_total")
            or res.get("unexpected_percent")
            or res.get("unexpected_count")
            or res.get("element_count")
        )

        print({
            "column": column,
            "expectation": exp,
            "success": success,
            "observed_value": observed,
            "unexpected_count": res.get("unexpected_count"),
            "unexpected_percent": res.get("unexpected_percent")
        })


# =====================================================
# PRINT RAW RESULT (OPTIONAL)
# =====================================================

def print_raw_sample(results):

    print("\n=========== RAW GX RESULT SAMPLE ===========\n")

    for r in results.get("results", [])[:2]:

        print(json.dumps(r, indent=2, default=str))


# =====================================================
# MAIN
# =====================================================

def main():

    print("\n===== TEST DATA CONTRACT PIPELINE =====\n")

    contract = load_contract(CONTRACT_ID)

    suite_name = build_suite(contract)

    results = run_validation(suite_name)

    print_results(results)

    # optional debug
    print_raw_sample(results)


if __name__ == "__main__":
    main()