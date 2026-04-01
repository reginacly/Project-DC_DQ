import hashlib
import time
import requests
import great_expectations as gx
import gxlib

from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os

load_dotenv()


now_wib = datetime.now(ZoneInfo("Asia/Jakarta"))

DOMAIN = os.getenv('domain_datahub')
TOKEN = os.getenv('token_datahub')
DATAHUB_GQL = f"{DOMAIN}api/graphql"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


# ============================================================
# HELPER
# ============================================================

def get_platform_by_stage(stage: str) -> str:
    """
    Map pipeline stage to DataHub platform name.
    Adjust this if your DataHub platform name is different.
    """
    mapping = {
        "postgres": "postgres",
        "odps": "odps",   # ganti ke "maxcompute" kalau di DataHub pakainya maxcompute
        "trino": "trino",
        "dwh": "mssql",
    }

    if stage not in mapping:
        raise ValueError(f"Unsupported stage for platform mapping: {stage}")

    return mapping[stage]


def gql(query: str, variables: dict | None = None):
    r = requests.post(
        DATAHUB_GQL,
        json={"query": query, "variables": variables or {}},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    out = r.json()

    if "errors" in out:
        raise RuntimeError(out["errors"])

    return out["data"]


def parse_dataset_urn(urn: str):
    inner = urn.split("(", 1)[1].rstrip(")")
    platform_urn, name, env = inner.split(",", 2)
    platform = platform_urn.split(":")[-1]
    return platform, name, env


def resolve_exact_dataset_urn(data: dict, platform: str, name: str, env: str) -> str:
    try:
        entities = data["search"]["searchResults"]
    except KeyError as e:
        raise KeyError("Invalid DataHub search response structure") from e

    for item in entities:
        urn = item["entity"]["urn"]
        p, n, e = parse_dataset_urn(urn)

        if p == platform and n == name and e == env:
            return urn

    raise ValueError(
        f"Dataset not found: platform={platform}, name={name}, env={env}"
    )


def get_dataset_urn(schema: str, table_name: str, platform: str, env: str = "PROD"):
    query = """
    query searchDataset($query: String!) {
      search(input: {
        type: DATASET,
        query: $query,
        start: 0,
        count: 20
      }) {
        searchResults {
          entity {
            urn
          }
        }
      }
    }
    """

    variables = {
        "query": f"{schema}.{table_name}"
    }

    resp = gql(query, variables)

    return resolve_exact_dataset_urn(
        resp,
        platform,
        f"production.{schema}.{table_name}",
        env
    )


def stable_assertion_id(stage: str, table_name: str, suite_name: str, expectation_name: str) -> str:
    key = f"{stage}:{table_name}:{suite_name}:{expectation_name}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def describe_range(cfg: dict, subject: str) -> str:
    kw = cfg.get("kwargs") or {}
    min_v = kw.get("min_value")
    max_v = kw.get("max_value")

    if min_v is not None and max_v is not None:
        return f"Ensure {subject} is between {min_v} and {max_v}."
    if min_v is not None:
        return f"Ensure {subject} is at least {min_v}."
    if max_v is not None:
        return f"Ensure {subject} is at most {max_v}."
    return f"Ensure {subject} is within the expected range."


def default_description(expectation_type: str, cfg: dict) -> str:
    kwargs = cfg.get("kwargs", {})

    column = kwargs.get("column")
    if column:
        return f"Ensure `{expectation_type}` holds for column `{column}`."

    column_set = kwargs.get("column_set")
    if column_set:
        return f"Ensure `{expectation_type}` holds for expected columns `{column_set}`."

    return f"Ensure `{expectation_type}` holds for dataset."


def map_expectation_to_dimension(expectation_type: str) -> str:
    mapping = {
        "expect_column_values_to_not_be_null": "COMPLETENESS",
        "expect_column_values_to_be_null": "COMPLETENESS",
        "expect_column_values_to_be_unique": "UNIQUENESS",
        "expect_column_values_to_be_in_set": "VALIDITY",
        "expect_column_values_to_not_be_in_set": "VALIDITY",
        "expect_column_values_to_match_regex": "VALIDITY",
        "expect_column_values_to_not_match_regex": "VALIDITY",
        "expect_column_values_to_be_between": "VALIDITY",
        "expect_column_value_lengths_to_be_between": "VALIDITY",
        "expect_table_row_count_to_be_between": "VOLUME",
        "expect_table_column_count_to_be_between": "SCHEMA",
        "expect_table_columns_to_match_set": "SCHEMA",
        "expect_ column_values_not_have_double_space": "CORRECTNESS"
    }

    return mapping.get(expectation_type, "VALIDITY")


EXPECTATION_DESCRIPTION_MAP = {
    "expect_column_values_to_not_be_null": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values are never null."
    ),
    "expect_column_values_to_be_null": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values are null."
    ),
    "expect_column_values_to_be_unique": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values are unique."
    ),
    "expect_column_values_to_be_in_set": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values are one of {cfg['kwargs']['value_set']}."
    ),
    "expect_column_values_to_not_be_in_set": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values are NOT one of {cfg['kwargs']['value_set']}."
    ),
    "expect_column_values_to_match_regex": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values match regex `{cfg['kwargs']['regex']}`."
    ),
    "expect_column_values_to_not_match_regex": lambda cfg: (
        f"Ensure column `{cfg['kwargs']['column']}` values do not match regex `{cfg['kwargs']['regex']}`."
    ),
    "expect_table_row_count_to_be_between": lambda cfg: (
        "Ensure table row count is within expected range."
    ),
    "expect_table_column_count_to_be_between": lambda cfg: (
        "Ensure table column count is within expected range."
    ),
    "expect_table_columns_to_match_set": lambda cfg: (
        f"Ensure table columns contain expected set {cfg['kwargs']['column_set']}."
    ),
    "expect_column_value_lengths_to_be_between": lambda cfg: describe_range(
        cfg, f"length of column `{(cfg.get('kwargs') or {}).get('column', '<unknown>')}`"
    ),
    "expect_column_values_to_be_between": lambda cfg: describe_range(
        cfg, f"value of column `{(cfg.get('kwargs') or {}).get('column', '<unknown>')}`"
    ),
    "expect_column_values_to_not_have_double_space": lambda cfg: describe_range(
        cfg, f"value of column `{(cfg.get('kwargs') or {}).get('column', '<unknown>')}`"
    ),
}


# ============================================================
# DATASET DESCRIPTION
# ============================================================

# def push_description(urn: str, description: str):
#     query = """
#     mutation updateDataset($urn: String!, $description: String!) {
#       updateDataset(
#         urn: $urn
#         input: {
#           editableProperties: {
#             description: $description
#           }
#         }
#       ) {
#         urn
#       }
#     }
#     """

#     gql(query, {"urn": urn, "description": description})


# ============================================================
# ASSERTION UPSERT
# ============================================================

def upsert_assertion(
    *,
    assertion_id: str,
    entity_urn: str,
    dimension: str,
    description: str,
    exp_name: str,
    platform_urn: str = "urn:li:dataPlatform:great-expectations",
    field_path: str | None = None,
    external_url: str | None = None,
    logic: str | None = None,
) -> str:
    query = """
    mutation upsertCustomAssertion($urn: String, $input: UpsertCustomAssertionInput!) {
      upsertCustomAssertion(urn: $urn, input: $input) {
        urn
      }
    }
    """

    assertion_urn = f"urn:li:assertion:{assertion_id}"

    input_obj = {
        "entityUrn": entity_urn,
        "type": dimension.upper(),
        "description": description,
        "platform": {"urn": platform_urn},
    }

    # if field_path:
    #     input_obj["fieldPath"] = field_path

    if external_url:
        input_obj["externalUrl"] = external_url

    if logic:
        input_obj["logic"] = logic

    gql(query, {"urn": assertion_urn, "input": input_obj})
    return assertion_urn


def upsert_assertion_for_suite(stage: str, table_name: str, urn: str):
    suite_name = f"{stage}_{table_name}_suite"
    context = gxlib.GXContextSingleton.get_context()
    suite = context.suites.get(suite_name)

    success_count = 0
    failed_count = 0

    for exp in suite.expectations:
        try:
            exp_name = None
            meta = getattr(exp, "meta", None) or {}
            exp_name = meta.get("name") or exp.expectation_type

            cfg_obj = getattr(exp, "configuration", None)
            if cfg_obj is not None and hasattr(cfg_obj, "to_json_dict"):
                cfg = cfg_obj.to_json_dict()
            elif cfg_obj is not None:
                cfg = dict(cfg_obj)
            else:
                cfg = {}

            expectation_type = exp.expectation_type
            kwargs = cfg.get("kwargs", {})
            field_path = kwargs.get("column")

            if not field_path:
                print(f"No column detected for {exp.expectation_type}")

            try:
                description = EXPECTATION_DESCRIPTION_MAP[expectation_type](cfg)
            except Exception:
                description = default_description(expectation_type, cfg)

            dimension = meta.get("dimension") or map_expectation_to_dimension(expectation_type)

            upsert_assertion(
                assertion_id=stable_assertion_id(
                    stage=stage,
                    table_name=table_name,
                    suite_name=suite_name,
                    expectation_name=exp_name,
                ),
                entity_urn=urn,
                dimension=dimension,
                description=description,
                exp_name=exp_name,
                field_path=field_path,
                logic=str(kwargs) if kwargs else None,
            )

            success_count += 1

        except Exception as e:
            failed_count += 1
            print(f"⚠️ Failed upsert assertion for {exp.expectation_type}: {e}")

    print(
        f"Successfully upsert assertions for {table_name}. "
        f"success={success_count}, failed={failed_count}"
    )


# ============================================================
# ASSERTION RESULT REPORT
# ============================================================

def report_result(
    *,
    stage: str,
    table_name: str,
    expectation_name: str,
    status: str,
    props: dict[str, str] | None = None,
    external_url: str | None = None,
    timestamp_ms: int | None = None,
):
    query = """
    mutation reportAssertionResult($urn: String!, $result: AssertionResultInput!) {
      reportAssertionResult(urn: $urn, result: $result)
    }
    """

    suite_name = f"{stage}_{table_name}_suite"
    assertion_id = stable_assertion_id(stage, table_name, suite_name, expectation_name)
    assertion_urn = f"urn:li:assertion:{assertion_id}"

    result_obj = {
        "timestampMillis": timestamp_ms or int(time.time() * 1000),
        "type": status,
        "properties": [
            {"key": str(k), "value": str(v)}
            for k, v in (props or {}).items()
        ],
    }

    if external_url:
        result_obj["externalUrl"] = external_url

    gql(query, {"urn": assertion_urn, "result": result_obj})


# ============================================================
# OPTIONAL TABLE LIST HELPER
# ============================================================

# def get_table_list_from_datahub(tag: str, platform: str):
#     query = """
#     query taggedPlatformDatasetUrns($input: ScrollAcrossEntitiesInput!) {
#       scrollAcrossEntities(input: $input) {
#         searchResults {
#           entity { urn }
#         }
#         nextScrollId
#       }
#     }
#     """

#     scroll_id = None
#     results = []

#     while True:
#         variables = {
#             "input": {
#                 "types": ["DATASET"],
#                 "query": "*",
#                 "count": 1000,
#                 "scrollId": scroll_id,
#                 "orFilters": [
#                     {
#                         "and": [
#                             {"field": "platform", "values": [f"urn:li:dataPlatform:{platform}"]},
#                             {"field": "tags", "values": [f"urn:li:tag:{tag}"]},
#                         ]
#                     }
#                 ],
#             }
#         }

#         resp = gql(query=query, variables=variables)
#         block = resp["scrollAcrossEntities"]
#         rows = block["searchResults"]

#         results.extend([r["entity"]["urn"] for r in rows])

#         scroll_id = block["nextScrollId"]
#         if not scroll_id or not rows:
#             break

#     return results


if __name__ == "__main__":
    schema = "data_analytics_sandbox"
    table_name = "data_quality_expectation_result"
    platform = "trino"

    dataset_urn = get_dataset_urn(
        schema=schema,
        table_name=table_name,
        platform=platform,
        env="PROD",
    )

    print("Dataset URN:", dataset_urn)