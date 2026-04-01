import os
from odps import ODPS
import yaml
from datetime import datetime
from dotenv import load_dotenv

# ============================================================
# 1. LOAD ENV
# ============================================================
load_dotenv()

# ============================================================
# 2. CONFIG ODPS
# ============================================================
ODPS_CONN = {
    "access_id": os.getenv("user_odps"),
    "secret_access_key": os.getenv("password_odps"),
    "project": os.getenv("project_odps"),
    "endpoint": os.getenv("endpoint_odps"),
}

TARGET_SCHEMA = os.getenv("schema_odps")
TARGET_TABLE = "dbo_collection__amortization_juris"
OUTPUT_DIR = "auto-data-contracts/odps"
SERVER_NAME = "odps"

if not TARGET_SCHEMA:
    raise Exception("schema_odps is not set")

# ============================================================
# 3. CONNECT ODPS
# ============================================================
odps = ODPS(
    ODPS_CONN["access_id"],
    ODPS_CONN["secret_access_key"],
    ODPS_CONN["project"],
    endpoint=ODPS_CONN["endpoint"],
)

print(f"🔍 Scanning ODPS table {TARGET_SCHEMA}.{TARGET_TABLE}")

table = odps.get_table(TARGET_TABLE, schema=TARGET_SCHEMA)

# ============================================================
# 4. LOGICAL TYPE MAPPING (ODPS)
# ============================================================
def map_logical_type(physical_type: str) -> str:
    pt = physical_type.lower()

    # STRING TYPES
    if any(x in pt for x in ["char", "varchar", "string"]):
        return "string"

    # DATE & TIME
    if pt == "date":
        return "date"

    if "datetime" in pt:
        return "datetime"

    if any(x in pt for x in ["timestamp", "timestamp_ntz"]):
        return "timestamp"

    # INTEGER
    if any(x in pt for x in ["bigint", "smallint", "int", "tinyint"]):
        return "integer"

    # NUMBER / FLOAT / DECIMAL
    if any(x in pt for x in ["decimal", "double", "float"]):
        return "number"

    # BOOLEAN
    if "boolean" in pt or pt == "bool":
        return "boolean"

    # JSON / OBJECT
    if any(x in pt for x in ["json", "map", "struct"]):
        return "object"

    # COMPLEX TYPES
    if "array" in pt:
        return "array"

    # BINARY
    if "binary" in pt:
        return "binary"
    
    if "interval" in pt:
        return "interval"

    # DEFAULT FALLBACK
    return "string"

# ============================================================
# 5. CDE DETECTION
# ============================================================
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

# ============================================================
# 6. AUTO DATA QUALITY RULE GENERATOR
# ============================================================

def get_custom_rules(column_name: str):
    col = column_name.lower()

    if col == "email":
        return [
            {
                "metric" : "valueEmailFormatValid",
                "description" : "Email must follow a valid email address format",
                "dimension": "correctness",
                "severity": "warning"
            }
        ]
    
    elif col == "gender":
        return [
            {
                "metric" : "valueInSet",
                "arguments" :{
                    "validValues" :['F','M', 'Female',  'Male'] },
                "description": "All values must match one of the defined valid values.",
                "dimension": "correctness",
                "severity": "warning"
            }
        ]
    
    elif col == "firstinstallment":
        return [
            {
                "metric" : "valueInSet",
                "arguments" :{
                    "validValues" :['AR','AD', 'ARREAR', 'ADVANCE'] },
                "description": "All values must match one of the defined valid values.",
                "dimension": "correctness",
                "severity": "warning"
            }
        ]
    
    elif "name" in col:
        return [
            {
                "metric" : "valueNotContainSpecialCharacter",
                "description" : "name must not contain special character",
                "dimension": "correctness",
                "severity": "critical"
            }
        ]
    
    elif col == "maritalstatus":
        return [
            {
                "metric" : "valueInSet",
                "arguments" :{
                    "validValues" :['Single','Married', 'Widdow', 'Widdower', 'Divorce' ] },
                "description": "All values must match one of the defined valid values.",
                "dimension": "correctness",
                "severity": "warning"
            }
        ]
    
    elif any(x in col for x in ["areaphone"]):
        return []
    
    elif "phone" in col:
        return [
            {
                "metric" : "valuePhoneNumberFormatValid",
                "description" : "Phone number must follow a valid format",
                "dimension": "correctness",
                "severity": "warning"
            }
        ]
    
    elif col in ["companyfax", "legalfax", "residencefax"]: 
        return [
            {
                "metric" : "regexMatch",
                "description" : "company fax must be 14-digit numeric",
                "arguments" :{
                    "pattern" : '^[0-9]{14}$'},
                "dimension": "correctness",
                "severity": "critical"
            }
        ]
    
    elif col == "manufacturingyear":
        return [
            {
                "metric": "valueOutOfRange",
                "description": "Manufacturing year must be  after 1900",
                "arguments": {
                    "minValue": "1901"},
                "dimension": "correctness",
                "severity": "critical"
            }
        ]
    
    elif col == "licenseplate":
        return [
            {
                "metric": "regexMatch",
                "description": "License plate must follow Indonesian TNKB format (e.g., B 1234 XYZ)",
                "arguments": {
                    "pattern": r'^[A-Z]{1,2}\s?[0-9]{1,4}\s?[A-Z]{0,3}$'
                },
                "dimension": "correctness",
                "severity": "critical"
            }
        ]
    
    return []

# ============================================================
# 6. AUTO DATA QUALITY RULE GENERATOR
# ============================================================
def generate_quality_rules(physical_type: str):

    pt = physical_type.lower()
    if any (x in pt for x in ["string", "varchar", "char"]):
        return[{
            "metric": "valuesNotContainDoubleSpace",
            "description" : "Validates that no double spaces exist",
            "dimension": "correctness",
            "severity": "critical"
        },
        {
            "metric": "valuesNotNull",
            "description" : "Ensures the column contains no missing or null values",
            "dimension": "completeness",
            "severity": "critical"
        }]
    
    if any(x in pt for x in ["int", "bigint", "double", "float", "decimal", "smallint", "tinyint"]):
        return[{
            "metric": "valueOutOfRange",
            "description" : "Validates that the data is non-negative",
            "arguments" : {
                "minValue" : 0
            },
            "dimension": "correctness",
            "severity": "warning"
        },
        {
            "metric": "valuesNotNull",
            "description" : "Ensures the column contains no missing or null values",
            "dimension": "completeness",
            "severity": "critical"
        }
        ]
    if pt == "date":
        return [
            {
                "metric": "valueOutOfRange",
                "description" : "Ensures date values are more recent than the default system placeholder (1900-01-01)",
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
                "description" : "Ensures date values not default system 1970-01-01 00:00:00",
                "arguments": {
                    "invalidValues": ["1970-01-01 00:00:00"]},
                "dimension": "correctness",
                "severity": "critical"
            },
            {
                "metric": "valueOutOfRange",
                "description" : "Ensures date values are more recent than the default system placeholder 1970-01-01 00:00:00",
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
                "description": "Ensures the column contains no missing or null values",
                "dimension": "completeness",
                "severity": "critical"
            }
        ]
    return []
                              

# ============================================================
# 6. BUILD DATA CONTRACT
# ============================================================
data_contract = {
    "apiVersion": "v3.1.0",
    "kind": "DataContract",
    "id": f"{TARGET_SCHEMA}.{TARGET_TABLE}",
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
        "type": "odps",
        "database": ODPS_CONN["project"],
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
        "contractGeneratedBy": "auto-odps-script",
        "notes": ""
    }
}

# ============================================================
# 7. FILL COLUMNS (NO PK IN ODPS)
# ============================================================
for col in table.schema.columns:
    physical_type = str(col.type).lower()

    data_contract["schema"][0]["properties"].append({
        "name": col.name,
        "logicalType": map_logical_type(physical_type),
        "physicalType": physical_type,
        "nullable": True,          # ODPS does not enforce NOT NULL
        "primaryKey": False,       # ODPS has no PK
        "unique": False,
        "criticalDataElement": is_critical_data_element(col.name),
        "description": "",
        "quality": generate_quality_rules(physical_type)
        + get_custom_rules(col.name)
    })

print(table.schema.columns)

# ============================================================
# 8. WRITE YAML
# ============================================================
output_schema_dir = os.path.join(OUTPUT_DIR, TARGET_SCHEMA)
os.makedirs(output_schema_dir, exist_ok=True)

output_file = os.path.join(output_schema_dir, f"{TARGET_TABLE}.yaml")

with open(output_file, "w", encoding="utf-8") as f:
    yaml.dump(data_contract, f, sort_keys=False, allow_unicode=True)

print("✅ Data contract generated successfully")
print(f"📄 File saved at: {output_file}")


