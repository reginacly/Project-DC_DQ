import os
import yaml
from datetime import datetime
from dotenv import load_dotenv
import trino
from trino.auth import BasicAuthentication
from trino.dbapi import Connection as TrinoConnection
import urllib3

# ============================================================
# DISABLE SSL WARNING (OPTIONAL)
# ============================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

TRINO_CONN = {
    "host": os.getenv("endpoint_datalake"),
    "port": int(os.getenv("port_datalake", 7778)),
    "user": os.getenv("user_datalake"),
    "catalog": os.getenv("catalog_datalake"),
    "schema": os.getenv("schema_datalake"),
}

TARGET_SCHEMA = TRINO_CONN["schema"]
TARGET_TABLE = "fea_thm_debt_to_income_ratio"   # <-- adjust sesuai kebutuhan
OUTPUT_DIR = "auto-data-contracts/trino"

if not TARGET_SCHEMA:
    raise Exception("❌ trino_schema is not set in .env")

CONTRACT_ID = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
SERVER_NAME = "trino-bravo-nonprod"

# ============================================================
# 🔥 FIX SSL HERE (IMPORTANT)
# ============================================================

from trino.auth import BasicAuthentication
import requests

session = requests.Session()
session.verify = False   # 🔥 FIX SSL DI SINI

# ============================================================
# CONNECT TRINO
# ============================================================

print(f"🔍 Scanning table {TARGET_SCHEMA}.{TARGET_TABLE} (Trino)")

conn = trino.dbapi.connect(
    host=TRINO_CONN["host"],
    port=TRINO_CONN["port"],
    user=TRINO_CONN["user"],
    catalog=TRINO_CONN["catalog"],
    schema=TRINO_CONN["schema"],
    http_scheme="https",
    auth=BasicAuthentication(
        os.getenv("user_datalake"),
        os.getenv("password_datalake")
    ),
    http_session=session
)

cursor = conn.cursor()

# ============================================================
# GET COLUMNS
# ============================================================

SQL_COLUMNS = f"""
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = '{TARGET_SCHEMA}'
  AND table_name = '{TARGET_TABLE}'
ORDER BY ordinal_position
"""

cursor.execute(SQL_COLUMNS)
columns = cursor.fetchall()

if not columns:
    raise Exception(f"❌ Table not found: {TARGET_SCHEMA}.{TARGET_TABLE}")

print(f"📊 Total columns: {len(columns)}")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def map_logical_type(physical_type: str) -> str:
    pt = physical_type.lower()

    if any(x in pt for x in ["char", "varchar"]):
        return "string"
    if pt == "date":
        return "date"
    if "timestamp" in pt:
        return "timestamp"
    if any(x in pt for x in ["bigint", "integer", "smallint", "int"]):
        return "integer"
    if any(x in pt for x in ["double", "float", "decimal", "real"]):
        return "number"
    if pt == "boolean":
        return "boolean"
    if pt in ["array", "map", "row", "json"]:
        return "object"

    return "string"


def is_critical_data_element(column_name: str) -> bool:
    keywords = {
        "address", "address_detail", "address_phone", "assettypeid",
         "business_scale","cif_emergency_id", "cif_other_business_id", "company_type",
        "customer_type", "district", "employment_since_year","established_city", 
        "established_district","established_subdistrict",  "home_location", "home_owner_name", 
        "id_number", "id_number_expiration_date", "id_type", "industry_type", "job_title", 
        "legal_name", "monthly_income", "nationality", "npwp", "npwp_type", "num_dependent", 
        "number_of_employees", "otr_price", "owneraddress", "relation_type", "rt", "rw",
        "serialno1", "serialno2", "share_percentage", "since_year", "spouse_birth_date", 
        "spouse_birth_place","spouse_id_number", "spouse_id_number_expiration_date",
        "spouse_legal_name", "stay_since_year", "subdistrict", "suppliername", "type",
        "maritalstatus", "spousename", "gender", "birthdate", "birthplace",
        "surgatemothername", "profession",  "companyname", "licenseplate", "color",
        "manufacturingyear", "tenor", "firstinstallment", "spousemobilephone", 
        "email", "mobilephone", "name", "legalphone1", "companyphone1", "residencephone1", 
        "companyfax", "legalfax", "residencefax", "emergencyphone1", "emergencyphone2",
        "emergencycontactname", "residencephone2", "legalphone2", "realtenor", "companyphone2"
    }
    col = column_name.lower()
    return any(k in col for k in keywords)

def generate_quality_rules(physical_type: str):

    pt = physical_type.lower()
    if any (x in pt for x in ["string", "varchar", "char"]):
        return[{
            "metric": "valuesNotContainDoubleSpace",
            "description" : "",
            "dimension": "correctness",
            "severity": "critical"
        },
        {
            "metric": "valuesNotNull",
            "description" : "",
            "dimension": "completeness",
            "severity": "critical"
        }]
    
    if any(x in pt for x in ["int", "bigint", "double", "float", "decimal", "smallint", "tinyint"]):
        return[{
            "metric": "valueOutOfRange",
            "description" : "",
            "arguments" : {
                "minValue" : 0
            },
            "dimension": "correctness",
            "severity": "warning"
        },
        {
            "metric": "valuesNotNull",
            "description" : "",
            "dimension": "completeness",
            "severity": "critical"
        }
        ]
    if pt == "date":
        return [
            {
                "metric": "valueOutOfRange",
                "description" : "",
                "arguments": {
                    "minValue": "1900-01-02"
                },
                "dimension": "validity",
                "severity": "warning"
                
            }
        ]
    if any(x in pt for x in ["timestamp", "datetime"]):
        return [
            {
                "metric": "valuesNotInSet",
                "description" : "",
                "arguments": {
                    "invalidValues": ["1970-01-01 00:00:00"]},
                "dimension": "correctness",
                "severity": "critical"
            },
            {
                "metric": "valueOutOfRange",
                "description" : "",
                "arguments": {
                    "minValue": "1900-01-02 00:00:00"},
                "dimension": "validity",
                "severity": "critical"
                
            }
        ]
    if "boolean" in pt or pt == "bool":
        return [
            {
                "metric": "valueInSet",
                "description": "",
                "dimension": "validity",
                "arguments": {
                    "validValues": [True, False]},
                "severity": "warning"
            }
        ]
    if any(x in pt for x in ["json", "array", "map", "struct", "binary", "interval"]):
        return [
            {
                "metric": "valuesNotNull",
                "description": "",
                "dimension": "completeness",
                "severity": "critical"
            }
        ]
    return []

# ============================================================
# BUILD DATA CONTRACT
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
        "catalog": TRINO_CONN["catalog"],
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
        "contractGeneratedBy": "auto-trino-script",
        "notes": ""
    }
}

# ============================================================
# POPULATE COLUMNS
# ============================================================

for column_name, data_type in columns:

    data_contract["schema"][0]["properties"].append({
        "name": column_name,
        "logicalType": map_logical_type(data_type),
        "physicalType": data_type,
        "nullable": True,
        "primaryKey": False,
        "unique": False,
        "criticalDataElement": is_critical_data_element(column_name),
        "description": "",
        "quality": generate_quality_rules(data_type)
    })

# ============================================================
# SAVE FILE
# ============================================================

output_schema_dir = os.path.join(OUTPUT_DIR, TARGET_SCHEMA)
os.makedirs(output_schema_dir, exist_ok=True)

output_file = os.path.join(output_schema_dir, f"{TARGET_TABLE}.yaml")

with open(output_file, "w", encoding="utf-8") as f:
    yaml.dump(data_contract, f, sort_keys=False, allow_unicode=True)

print("✅ Data contract generated successfully")
print(f"📄 File saved at: {output_file}")