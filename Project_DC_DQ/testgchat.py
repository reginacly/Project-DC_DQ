import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

webhook_url = os.getenv("gchat_webhook_url")

payload = {
    "text": "✅ Test alert dari data quality pipeline"
}

resp = requests.post(
    webhook_url,
    data=json.dumps(payload),
    headers={"Content-Type": "application/json"},
    timeout=30,
)

print(resp.status_code)
print(resp.text)
resp.raise_for_status()