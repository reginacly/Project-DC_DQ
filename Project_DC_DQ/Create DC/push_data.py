import requests
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

# =====================================================
# CONFIG
# =====================================================

DOMAIN = "https://datahub.bfi.co.id/"
TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6Im1zby5qb3NlcGguc29saWNoaW5AYmZpLmNvLmlkIiwidHlwZSI6IlBFUlNPTkFMIiwidmVyc2lvbiI6IjIiLCJqdGkiOiJiMGYwOGIxNC00MjNlLTQ3NTYtYTVmNS0yM2NkOWExNzU2Y2EiLCJzdWIiOiJtc28uam9zZXBoLnNvbGljaGluQGJmaS5jby5pZCIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.Ik8d8TyUrDA5WeZXZ--CrAUQLcFwUafvI26QzAjdCkY"
DATAHUB_GQL = f"{DOMAIN}api/graphql"
DATAHUB_GMS = f"{DOMAIN}api/gms/aspects?action=ingestProposal"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:odps,bravo.default.dbo_collection__agreement_juris_masking,PROD)"

# =====================================================
# HELPER
# =====================================================

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

def gql(query: str, variables: dict = None):
    resp = requests.post(
        DATAHUB_GQL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if "errors" in result:
        raise RuntimeError(json.dumps(result["errors"], indent=2))
    return result["data"]

def ingest_proposal(entity_urn: str, aspect_name: str, aspect_value: dict):
    """Write an aspect via GMS ingestProposal REST API."""
    resp = requests.post(
        DATAHUB_GMS,
        headers=HEADERS,
        json={
            "proposal": {
                "entityType": "assertion",
                "entityUrn": entity_urn,
                "aspectName": aspect_name,
                "changeType": "UPSERT",
                "aspect": {
                    "contentType": "application/json",
                    "value": json.dumps(aspect_value),
                },
            }
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

# =====================================================
# 1. FETCH EXISTING ASSERTIONS
# =====================================================

GET_ASSERTIONS = """
query getAssertions($urn: String!) {
  dataset(urn: $urn) {
    assertions(start: 0, count: 1000) {
      total
      assertions {
        urn
        info {
          type
          description
          customAssertion {
            type
            entityUrn
            field { path }
          }
        }
      }
    }
  }
}
"""

print("=" * 60)
print("Step 1: Fetching dataset assertions")
print(f"Dataset: {DATASET_URN}")
print("=" * 60)

data = gql(GET_ASSERTIONS, {"urn": DATASET_URN})
all_assertions = data["dataset"]["assertions"]["assertions"]
total = data["dataset"]["assertions"]["total"]
print(f"Total assertions: {total}\n")

# =====================================================
# 2. RE-EMIT ASSERTIONS AS DATASET TYPE
#    (converts CUSTOM -> DATASET with nativeType = description)
# =====================================================

print("Step 2: Converting assertions to DATASET type (for Data Contract display)\n")

updated = 0
skipped = 0
errors = 0

for a in all_assertions:
    urn = a["urn"]
    info = a.get("info") or {}
    a_type = (info.get("type") or "").upper()
    desc = info.get("description") or ""
    custom = info.get("customAssertion") or {}

    if not desc:
        print(f"  SKIP (no desc) {urn}")
        skipped += 1
        continue

    # Determine scope based on field presence
    has_field = custom.get("field") and custom["field"].get("path")
    scope = "DATASET_COLUMN" if has_field else "DATASET_ROWS"

    # Build assertionInfo aspect with DATASET type
    assertion_info = {
        "description": desc,
        "type": "DATASET",
        "source": {"type": "EXTERNAL"},
        "datasetAssertion": {
            "dataset": custom.get("entityUrn") or DATASET_URN,
            "scope": scope,
            "operator": "EQUAL_TO",
            "nativeType": desc,
            "nativeParameters": {"description": desc},
        },
    }

    if has_field:
        assertion_info["datasetAssertion"]["fields"] = [
            {"path": custom["field"]["path"], "type": "STRING", "nativeType": "string"}
        ]

    print(f"  EMIT {urn}")
    print(f"    -> {desc[:70]}")

    try:
        ingest_proposal(urn, "assertionInfo", assertion_info)
        updated += 1
    except Exception as e:
        errors += 1
        print(f"    ERROR: {e}")

print(f"\nResult: {updated} updated, {skipped} skipped, {errors} errors")

# =====================================================
# 3. UPSERT DATA CONTRACT
# =====================================================

print("\n" + "=" * 60)
print("Step 3: Upserting Data Contract")
print("=" * 60)

UPSERT_CONTRACT = """
mutation UpsertDataContract($input: UpsertDataContractInput!) {
  upsertDataContract(input: $input) { urn }
}
"""

data_quality_rules = [{"assertionUrn": a["urn"]} for a in all_assertions]

result = gql(UPSERT_CONTRACT, {
    "input": {
        "entityUrn": DATASET_URN,
        "freshness": [],
        "dataQuality": data_quality_rules,
        "schema": [],
    }
})
print(f"Contract: {result['upsertDataContract']['urn']}")

# =====================================================
# 4. VERIFY
# =====================================================

print("\n" + "=" * 60)
print("Step 4: Verification")
print("=" * 60)

VERIFY = """
query ($urn: String!) {
  dataset(urn: $urn) {
    contract {
      properties {
        dataQuality {
          assertion {
            urn
            info {
              type
              description
              datasetAssertion { nativeType }
            }
          }
        }
      }
    }
  }
}
"""

verify = gql(VERIFY, {"urn": DATASET_URN})
dq = verify["dataset"]["contract"]["properties"]["dataQuality"] or []
print(f"Assertions in contract: {len(dq)}\n")

has_desc = 0
for item in dq:
    a = item.get("assertion", {})
    info = a.get("info") or {}
    a_type = info.get("type")
    native = (info.get("datasetAssertion") or {}).get("nativeType") or ""
    status = "OK" if native else "EMPTY"
    if native:
        has_desc += 1
    print(f"  [{status}] [{a_type}] {native[:70] if native else '(no nativeType)'}")

print(f"\n{'='*60}")
if has_desc == len(dq):
    print(f"ALL {has_desc}/{len(dq)} assertions have descriptions!")
else:
    print(f"WARNING: {has_desc}/{len(dq)} assertions have descriptions")
print(f"{'='*60}")