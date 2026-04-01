import os
import yaml
from datetime import datetime
from dotenv import load_dotenv
from trino import dbapi
from trino.auth import BasicAuthentication

# ============================================================
# 1. LOAD ENV
# ============================================================
load_dotenv()

# ============================================================
# 2. CONFIG
# ============================================================
TARGET_SCHEMA = os.getenv("schema_datalake")
TARGET_TABLE = "customer_cif"
OUTPUT_DIR = "auto-data-contracts/datalake"
SERVER_NAME = "datalake"

if not TARGET_SCHEMA:
    raise Exception("schema_datalake is not set")

CONTRACT_ID = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

# ============================================================
# 3. TRINO CONNECTION (BEST PRACTICE)
# ============================================================
def get_trino_connection(schema: str):
    return dbapi.connect(
        host=os.getenv("endpoint_datalake"),
        port=os.getenv("port_datalake"),
        catalog=os.getenv("catalog_datalake"),
        schema=schema,
        auth=BasicAuthentication(
            os.getenv("user_datalake"),
            os.getenv("password_datalake"),
        ),
        http_scheme="https",
        verify=False,
    )

# ============================================================
# 4. FETCH COLUMNS FROM INFORMATION_SCHEMA
# ============================================================
print(f"🔍 Scanning datalake table {TARGET_SCHEMA}.{TARGET_TABLE}")

conn = get_trino_connection(TARGET_SCHEMA)
cursor = conn.cursor()

cursor.execute("""
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = ?
      AND table_name = ?
    ORDER BY ordinal_position
""", (TARGET_SCHEMA, TARGET_TABLE))

columns = cursor.fetchall()

cursor.close()
conn.close()

# ============================================================
# 5. LOGICAL TYPE MAPPING
# ============================================================
def map_logical_type(physical_type: str) -> str:
    pt = physical_type.lower()

    if any(x in pt for x in ["char", "varchar", "string"]):
        return "string"
    if pt == "date":
        return "date"
    if "timestamp" in pt:
        return "timestamp"
    if any(x in pt for x in ["int", "bigint", "smallint", "tinyint"]):
        return "integer"
    if any(x in pt for x in ["decimal", "numeric", "double", "float"]):
        return "number"
    if "boolean" in pt:
        return "boolean"
    if any(x in pt for x in ["json", "map", "struct"]):
        return "object"
    if pt.startswith("array"):
        return "array"

    return "string"

# ============================================================
# 6. CDE (CRITICAL DATA ELEMENT) DETECTION
# ============================================================
def is_critical_data_element(column_name: str) -> bool:
    keywords = {
        "address", "address_detail", "address_phone", "assettypeid",
        "birth_date", "birth_place", "business_scale",
        "cif_emergency_id", "cif_other_business_id",
        "company_fax", "company_name", "company_phone", "company_type",
        "customer_type", "district", "email", "employment_since_year",
        "established_city", "established_district",
        "established_subdistrict", "gender", "home_location",
        "home_owner_name", "id_number", "id_number_expiration_date",
        "id_type", "industry_type", "job_title", "legal_name",
        "licenseplate", "manufacturingyear", "mobile_phone",
        "monthly_income", "mother_name", "name", "nationality",
        "npwp", "npwp_type", "num_dependent", "number_of_employees",
        "otr_price", "owneraddress", "relation_type", "rt", "rw",
        "serialno1", "serialno2", "share_percentage", "since_year",
        "spouse_birth_date", "spouse_birth_place",
        "spouse_id_number", "spouse_id_number_expiration_date",
        "spouse_legal_name", "spouse_mobile_phone", "spouse_name",
        "stay_since_year", "subdistrict", "suppliername", "type"
    }
    col = column_name.lower()
    return any(k in col for k in keywords)

# ============================================================
# 7. BUILD DATA CONTRACT STRUCTURE
# ============================================================
data_contract = {
    "apiVersion": "v3.1.0",
    "kind": "DataContract",
    "id": CONTRACT_ID,
    "version": "1.0.0",
    "status": "DRAFT",
    "description": {
        "purpose": "",
        "usage": "",
        "limitations": ""
    },
    "team": {
        "name": "",
        "role": ""
    },
    "servers": [{
        "server": SERVER_NAME,
        "type": "trino",
        "database": os.getenv("catalog_datalake"),
        "schema": TARGET_SCHEMA
    }],
    "schema": [{
        "name": TARGET_TABLE,
        "physicalType": "table",
        "description": "",
        "relationships": [],
        "properties": []
    }],
    "dataQuality": [],
    "slaProperties": [],
    "changePolicy": {
        "breakingChange": "requires new contract version"
    },
    "metadata": {
        "contractCreatedAt": datetime.utcnow().date().isoformat(),
        "contractGeneratedBy": "auto-datalake-script",
        "notes": ""
    }
}

# ============================================================
# 8. FILL COLUMNS
# ============================================================
for column_name, data_type, is_nullable in columns:
    data_contract["schema"][0]["properties"].append({
        "name": column_name,
        "logicalType": map_logical_type(data_type),
        "physicalType": data_type,
        "nullable": is_nullable == "YES",
        "primaryKey": False,   # Datalake does not enforce PK
        "unique": False,
        "criticalDataElement": is_critical_data_element(column_name),
        "description": "",
        "quality": []
    })

# ============================================================
# 9. WRITE YAML
# ============================================================
output_schema_dir = os.path.join(OUTPUT_DIR, TARGET_SCHEMA)
os.makedirs(output_schema_dir, exist_ok=True)

output_file = os.path.join(output_schema_dir, f"{TARGET_TABLE}.yaml")

with open(output_file, "w", encoding="utf-8") as f:
    yaml.dump(data_contract, f, sort_keys=False, allow_unicode=True)

print("✅ Data contract generated successfully")
print(f"📄 File saved at: {output_file}")