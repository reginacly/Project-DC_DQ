import os
import json
import hashlib
import time
import requests
import gxlib

from dotenv import load_dotenv

load_dotenv()

DOMAIN = os.getenv("domain_datahub")
TOKEN = os.getenv("token_datahub")
DATAHUB_GQL = f"{DOMAIN}api/graphql"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


# ============================================================
# CONFIG TEST
# ============================================================

STAGE = "odps"
RAW_TABLE_NAME = "dbo_collection__agreement_juris_masking"

# hardcode dataset urn yang mau dites
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:odps,bravo.default.dbo_collection__agreement_juris_masking,PROD)"

# kalau True -> query ke DataHub untuk cek dataset ini memang ada
CHECK_DATASET_EXISTENCE = True

# kalau True -> simpan hasil preview ke file json
WRITE_TO_FILE = True
OUTPUT_FILE = "datahub_preview_output.json"


# ============================================================
# HELPER
# ============================================================

def gql(query: str, variables: dict | None = None):
    if not TOKEN:
        raise ValueError("DATAHUB_TOKEN is not set")

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
}


def build_assertion_payload(stage: str, table_name: str, exp) -> dict:
    suite_name = f"{stage}_{table_name}_suite"

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

    try:
        description = EXPECTATION_DESCRIPTION_MAP[expectation_type](cfg)
    except Exception:
        description = default_description(expectation_type, cfg)

    dimension = meta.get("dimension") or map_expectation_to_dimension(expectation_type)

    assertion_id = stable_assertion_id(
        stage=stage,
        table_name=table_name,
        suite_name=suite_name,
        expectation_name=exp_name,
    )

    assertion_urn = f"urn:li:assertion:{assertion_id}"

    input_obj = {
        "entityUrn": DATASET_URN,
        "type": dimension,
        "description": description,
        "platform": {"urn": "urn:li:dataPlatform:great-expectations"},
    }

    if field_path:
        input_obj["fieldPath"] = field_path

    if kwargs:
        input_obj["logic"] = str(kwargs)

    return {
        "suite_name": suite_name,
        "expectation_name": exp_name,
        "expectation_type": expectation_type,
        "assertion_id": assertion_id,
        "assertion_urn": assertion_urn,
        "input_obj": input_obj,
    }


def build_result_payload(stage: str, table_name: str, expectation_name: str, status: str = "SUCCESS") -> dict:
    suite_name = f"{stage}_{table_name}_suite"

    assertion_id = stable_assertion_id(
        stage=stage,
        table_name=table_name,
        suite_name=suite_name,
        expectation_name=expectation_name,
    )
    assertion_urn = f"urn:li:assertion:{assertion_id}"

    result_obj = {
        "timestampMillis": int(time.time() * 1000),
        "type": status,
        "properties": [
            {"key": "test_mode", "value": "true"},
            {"key": "table_name", "value": table_name},
            {"key": "stage", "value": stage},
        ],
    }

    return {
        "expectation_name": expectation_name,
        "assertion_urn": assertion_urn,
        "result_obj": result_obj,
    }


def check_dataset_exists(dataset_urn: str):
    query = """
    query getEntity($urn: String!) {
      entity(urn: $urn) {
        urn
        type
      }
    }
    """
    return gql(query, {"urn": dataset_urn})


def main():
    print("=== DATAHUB PREVIEW TEST ===")
    print("Stage      :", STAGE)
    print("Table      :", RAW_TABLE_NAME)
    print("Dataset URN:", DATASET_URN)
    print()

    if CHECK_DATASET_EXISTENCE:
        try:
            resp = check_dataset_exists(DATASET_URN)
            print("✅ Dataset URN exists in DataHub")
            print(json.dumps(resp, indent=2))
            print()
        except Exception as e:
            print(f"❌ Dataset URN check failed: {e}")
            return

    context = gxlib.GXContextSingleton.get_context()
    suite_name = f"{STAGE}_{RAW_TABLE_NAME}_suite"

    try:
        suite = context.suites.get(suite_name)
    except Exception as e:
        print(f"❌ Suite not found: {suite_name}")
        print(e)
        return

    assertions_preview = []
    results_preview = []

    for exp in suite.expectations:
        try:
            assertion_payload = build_assertion_payload(
                stage=STAGE,
                table_name=RAW_TABLE_NAME,
                exp=exp,
            )
            assertions_preview.append(assertion_payload)

            result_payload = build_result_payload(
                stage=STAGE,
                table_name=RAW_TABLE_NAME,
                expectation_name=assertion_payload["expectation_name"],
                status="SUCCESS",
            )
            results_preview.append(result_payload)

        except Exception as e:
            print(f"⚠️ Failed building preview for {getattr(exp, 'expectation_type', 'unknown')}: {e}")

    print(f"Total expectations previewed: {len(assertions_preview)}")
    print()

    for i, item in enumerate(assertions_preview[:5], start=1):
        print(f"--- Assertion Preview #{i} ---")
        print(json.dumps(item, indent=2))
        print()

    for i, item in enumerate(results_preview[:3], start=1):
        print(f"--- Result Preview #{i} ---")
        print(json.dumps(item, indent=2))
        print()

    if WRITE_TO_FILE:
        output = {
            "dataset_urn": DATASET_URN,
            "stage": STAGE,
            "raw_table_name": RAW_TABLE_NAME,
            "suite_name": suite_name,
            "assertion_count": len(assertions_preview),
            "result_count": len(results_preview),
            "assertions_preview": assertions_preview,
            "results_preview": results_preview,
        }

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print(f"✅ Preview written to {OUTPUT_FILE}")

    print("=== FINISHED ===")


if __name__ == "__main__":
    main()