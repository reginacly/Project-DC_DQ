import os
import psycopg2
import yaml
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

HOLO_CONN = {
    "host": os.getenv("endpoint_holo"),
    "port": os.getenv("port_holo"),
    "database": os.getenv("database_holo"),
    "user": os.getenv("user_holo"),
    "password": os.getenv("password_holo"),
}

TARGET_SCHEMA = os.getenv("schema_holo")
TARGET_TABLE = "doc_pending_agreement"
OUTPUT_DIR = "auto-data-contracts/holo"

if not TARGET_SCHEMA:
    raise Exception("schema_holo is not set")

CONTRACT_ID = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
SERVER_NAME = "holo-bravo-nonprod"

SQL_COLUMNS = """
    SELECT
        column_name,
        data_type,
        character_maximum_length,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position
"""

SQL_PK = """
    SELECT column_name
    FROM information_schema.key_column_usage
    WHERE table_schema = %s
      AND table_name = %s
"""

print(f"🔍 Scanning table {TARGET_SCHEMA}.{TARGET_TABLE}")

conn = psycopg2.connect(**HOLO_CONN)
cursor = conn.cursor()

cursor.execute(SQL_COLUMNS, (TARGET_SCHEMA, TARGET_TABLE))
columns = cursor.fetchall()

cursor.execute(SQL_PK, (TARGET_SCHEMA, TARGET_TABLE))
primary_keys = {row[0] for row in cursor.fetchall()}

print(f"🔑 PK candidates: {primary_keys}")

cursor.close()
conn.close()

def map_logical_type(physical_type: str) -> str:
    pt = physical_type.lower()

    if any(x in pt for x in ["char", "varchar", "text"]):
        return "string"

    if pt == "date":
        return "date"

    if "timestamp" in pt:
        return "timestamp"

    if any(x in pt for x in ["bigint", "integer", "smallint", "int"]):
        return "integer"

    if any(x in pt for x in ["numeric", "decimal", "double", "float"]):
        return "number"

    if "boolean" in pt or pt == "bool":
        return "boolean"

    if "json" in pt:
        return "object"

    return "string"

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

    return column_name.lower() in keywords

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
        "type": "postgres",
        "database": HOLO_CONN["database"],
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
        "contractGeneratedBy": "auto-holo-script",
        "notes": ""
    }
}

for column_name, data_type, char_len, is_nullable in columns:
    physical_type = f"{data_type}({char_len})" if char_len else data_type
    nullable = is_nullable == "YES"
    is_pk = column_name in primary_keys

    data_contract["schema"][0]["properties"].append({
        "name": column_name,
        "logicalType": map_logical_type(physical_type),
        "physicalType": physical_type,
        "nullable": nullable,
        "primaryKey": is_pk,
        "unique": is_pk, 
        "criticalDataElement": is_critical_data_element(column_name),
        "description": "",
        "quality": []
    })

output_schema_dir = os.path.join(OUTPUT_DIR, TARGET_SCHEMA)
os.makedirs(output_schema_dir, exist_ok=True)

output_file = os.path.join(output_schema_dir, f"{TARGET_TABLE}.yaml")

with open(output_file, "w", encoding="utf-8") as f:
    yaml.dump(data_contract, f, sort_keys=False, allow_unicode=True)

print("✅ Data contract generated successfully")
print(f"📄 File saved at: {output_file}")