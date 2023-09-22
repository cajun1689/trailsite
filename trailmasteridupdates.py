import requests
import json
import os
from pathlib import Path
from ratelimit import limits, sleep_and_retry

# Base API URL
API_URL = "https://app.smartsuite.com/api/v1/applications/"

# Auth file path
auth_path = Path(os.path.expanduser("~")) / "Desktop" / "auth.json"

# Load authorization headers from file
with open(auth_path) as f:
    HEADERS = json.load(f)

# Define rate limit: 10 requests per minute
ONE_MINUTE = 60
MAX_CALLS_PER_MINUTE = 10

@sleep_and_retry
@limits(calls=MAX_CALLS_PER_MINUTE, period=ONE_MINUTE)
def update_record(app_id, record_id, master_id):
    print(f"Attempting to update record {record_id} with master_id {master_id}")
    endpoint = f"{API_URL}{app_id}/records/{record_id}/"
    data = {"s0d1c07938": master_id}
    response = requests.patch(endpoint, headers=HEADERS, json=data)
    if response.status_code == 200:
        print(f"Successfully updated record {record_id}, Response: {response.json()}")
    else:
        print(f"Failed to update record {record_id}, Reason: {response.content}")

def fetch_records(app_id, fields):
    endpoint = f"{API_URL}{app_id}/records/list/"
    payload = {
        "sort": [],
        "filter": {}
    }
    response = requests.post(endpoint, headers=HEADERS, json=payload)
    if response.status_code == 200:
        return [{field: record.get(field) for field in fields} for record in response.json().get('items', [])]
    else:
        print(f"Failed to fetch records: {response.content}")
        return []

def main():
    first_app_id = "64e55236fe94933e2e380e60"
    second_app_id = "64ee591bd392bd8ec9c62d73"
    rfid_to_master_id = {}

    # Fetch records from first_app and map RFID to master_id
    first_app_records = fetch_records(first_app_id, ["sbb8fea034", "sd48be64b7"])
    for record in first_app_records:
        rfid = record.get("sbb8fea034")
        master_id = record.get("sd48be64b7")
        if rfid and master_id:
            rfid_to_master_id[rfid] = master_id

    # Fetch records from second_app
    second_app_records = fetch_records(second_app_id, ["id", "s99187d139"])
    for record in second_app_records:
        rfid = record.get("s99187d139")
        record_id = record.get("id")
        if rfid in rfid_to_master_id:
            master_id = rfid_to_master_id[rfid]
            update_record(second_app_id, record_id, master_id)

if __name__ == '__main__':
    main()
