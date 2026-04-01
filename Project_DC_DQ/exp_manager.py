import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement
if not hasattr(sa, "ColumnElement"):
    sa.ColumnElement = ColumnElement

import great_expectations.expectations as gxe
import great_expectations as gx
import warnings
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="odps.utils")

import os
import gxlib
import lib
from great_expectations.expectations.row_conditions import Column
from great_expectations.core.expectation_suite import ExpectationSuite
from github import Github, Auth
import yaml

# custom expectations
from GX.custom_exp.expect_column_values_to_not_contain_special_characters import ExpectColumnValuesToNotContainSpecialCharacters
from GX.custom_exp.expect_column_values_to_be_email import ExpectColumnValuesToBeEmail
from GX.custom_exp.expect_column_values_to_not_have_double_space import ExpectColumnValuesToNotHaveDoubleSpace
from GX.custom_exp.expect_column_values_to_be_indonesian_phone_number import ExpectColumnValuesToMatchIndonesianPhoneRules
from GX.custom_exp.expect_column_values_to_be_valid_json import ExpectColumnValuesToBeValidJson

# ============================================================
# GITHUB CONTRACT LOADER
# ============================================================

def load_contract_from_github(contract_id: str):

    token = os.getenv("github_token")
    
    g = Github(auth=Auth.Token(token))

    repo = g.get_repo("reginacly/data-contracts")

    file_path = f"auto-data-contracts/odps/bravo_dev/{contract_id}.yaml"

    file = repo.get_contents(file_path, ref="auto-generated")

    return yaml.safe_load(file.decoded_content.decode("utf-8"))


# ============================================================
# METRIC REGISTRY
# ============================================================

METRIC_REGISTRY = {

    "valuesNotContainDoubleSpace": lambda col, args: (
        ExpectColumnValuesToNotHaveDoubleSpace,
        {"column": col},
    ),

    "valueNotContainSpecialCharacter": lambda col, args: (
        ExpectColumnValuesToNotContainSpecialCharacters,
        {"column": col},
    ),

    "valueEmailFormatValid": lambda col, args: (
        ExpectColumnValuesToBeEmail,
        {"column": col},
    ),

    "valuePhoneNumberFormatValid": lambda col, args: (
        ExpectColumnValuesToMatchIndonesianPhoneRules,
        {"column": col},
    ),

    "regexMatch": lambda col, args: (
        gxe.ExpectColumnValuesToMatchRegex,
        {"column": col, "regex": args["pattern"]},
    ),

    "valueInSet": lambda col, args: (
        gxe.ExpectColumnValuesToBeInSet,
        {"column": col, "value_set": args["validValues"]},
    ),

    "valueOutOfRange": lambda col, args: (
        gxe.ExpectColumnValuesToBeBetween,
        {
            "column": col,
            **(
                {"min_value": args["minValue"]}
                if args.get("minValue") is not None else {}
            ),
            **(
                {"max_value": args["maxValue"]}
                if args.get("maxValue") is not None else {}
            ),
        }
    ),

    "columnGreaterThan": lambda col, args: (
        gxe.ExpectColumnPairValuesAToBeGreaterThanB,
        {"column_A": col, "column_B": args["compareColumn"]},
    ),

    "valuesNotNull": lambda col, args: (
        gxe.ExpectColumnValuesToNotBeNull,
        {"column": col},
    ),

    "valuesNotInSet": lambda col, args: (
        gxe.ExpectColumnValuesToNotBeInSet,
        {"column": col,
         "value_set": args["invalidValues"]})
}


# ============================================================
# APPLY YAML SCHEMA RULES
# ============================================================

def apply_schema_quality_rules(suite, contract, column_list):

    DEFAULT_RESULT_FORMAT = {
        "result_format": "COMPLETE",
        "return_unexpected_rows": True
    }

    if not column_list:
        print("⚠️ Warning: column_list kosong, tidak ada rule kolom yang diterapkan.")
        return

    for table in contract.get("schema", []):
        for prop in table.get("properties", []):

            column = prop["name"]
            
            if column not in column_list:
                print(f"Skipping column not in table: {column}")
                continue

            for rule in prop.get("quality", []):
                metric = rule["metric"]
                if metric not in METRIC_REGISTRY:
                    print(f"❌ Skipping: Metric '{metric}' not found in REGISTRY for column '{column}'")
                    continue
                
                args = rule.get("arguments", {})
                exp_cls, params = METRIC_REGISTRY[metric](column, args)

                suite.add_expectation(
                    exp_cls(
                        **params,
                        result_format=DEFAULT_RESULT_FORMAT,
                        meta={
                            "name": f"{column}-{exp_cls.__name__}",
                            "dimension": rule.get("dimension"),
                            "severity": rule.get("severity")
                        },
                    )
                )


# ============================================================
# GET SUITE
# ============================================================

# def get_suite(table_name: str, stage: str):

#     context = gxlib.GXContextSingleton.get_context()
#     name = f'{stage}_{table_name}_suite'

#     try:
#         suite = context.suites.get(name)
#         return suite
#     except:
#         print(f'Suite for {stage}_{table_name} not found')
#         return None


# ============================================================
# MAIN SUITE GENERATOR (COMPATIBLE WITH REKAN'S MAIN.PY)
# ============================================================

def add_or_update_suite(table_name: str, stage: str, column_list: list):

    context = gxlib.GXContextSingleton.get_context()
    suite_name = f'{stage}_{table_name}_suite'
    try :
        suite = context.suites.get(name=suite_name)
        suite.expectations.clear()
    except :
        suite = gx.ExpectationSuite(name=suite_name)

    # ============================================================
    # BASELINE RULES (TETAP SAMA SEPERTI REKAN)
    # ============================================================

    pk = lib.get_primary_key(table_name)

    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(
            min_value=0,
            strict_min=True,
            meta={"name": "row_count_expectation", "dimension": "completeness",  "severity" : 'critical'},
        )
    )

    suite.add_expectation(
        gxe.ExpectTableColumnCountToBeBetween(
            min_value=1,
            meta={"name": "column_count_expectation", "dimension": "schema", "severity" : 'critical'},
        )
    )

    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=['created_at', 'updated_at', 'is_deleted', 'inserted_date'],
            exact_match=False,
            result_format={"result_format": "COMPLETE"},
            meta={"name": "mandatory_column_expectation", "dimension": "schema", "severity":'critical'}
        )
    )

    if pk:
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(
                column=pk,
                meta={"name": "pk_not_null_expectation", "dimension": "completeness", "severity":'critical'},
            )
        )

        suite.add_expectation(
            gxe.ExpectColumnValuesToBeUnique(
                column=pk,
                meta={"name": "pk_unique_expectation", "dimension": "uniqueness", "severity":'critical'},
            )
        )

    # ============================================================
    # YAML CONTRACT RULES
    # ============================================================

    contract_id = f"{table_name}_contract"

    try :
        contract = load_contract_from_github(contract_id)
        if contract:
            apply_schema_quality_rules(suite, contract, column_list)
            print(f"✅ Contract rules applied for {table_name}")
        else:
            print(f"ℹ️ No contract found for {table_name}, using baseline only.")
    except Exception as e:
        print(f"⚠️ Warning loading contract: {str(e)}")

    context.suites.add_or_update(suite)
    return suite