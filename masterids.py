import requests
import json
import os
from pathlib import Path
from ratelimit import limits, sleep_and_retry
import subprocess

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
    data = {"sd48be64b7": master_id}
    response = requests.patch(endpoint, headers=HEADERS, json=data)
    if response.status_code == 200:
        print(f"Successfully updated record {record_id}, Response: {response.json()}")
    else:
        print(f"Failed to update record {record_id}, Reason: {response.content}")

def fetch_records(app_id):
    endpoint = f"{API_URL}{app_id}/records/list/"
    payload = {
        "sort": [{"direction": "asc", "field": "sd48be64b7"}],
        "filter": {}
    }
    response = requests.post(endpoint, headers=HEADERS, json=payload)
    if response.status_code == 200:
        return response.json().get('items', [])
    else:
        print(f"Failed to fetch records: {response.content}")
        return []

def assign_master_id(records):
    master_id_counter = 0  # Initialize to 0
    master_id_dict = {}  # To hold mappings of unique records to their master_id
    seen = set()  # To keep track of unique records already seen

    # Prepopulate master_id_dict with existing master IDs
    for record in records:
        unique_id = (record.get('sac87d276d', {}).get('date', ""),
                     record.get('s37af43f83', {}).get('sys_root', ""),
                     tuple(record.get('sac950cfcc', [])))
        master_id_value = record.get('sd48be64b7')
        if master_id_value and master_id_value != '':
            try:
                master_id_dict[unique_id] = int(master_id_value)
                master_id_counter = max(master_id_counter, int(master_id_value))
            except ValueError:
                print(f"Warning: Could not convert sd48be64b7 value '{master_id_value}' to integer.")

    # Process each record
    for record in records:
        record_id = record['id']
        unique_id = (record.get('sac87d276d', {}).get('date', ""),
                     record.get('s37af43f83', {}).get('sys_root', ""),
                     tuple(record.get('sac950cfcc', [])))

        # If this unique_id was seen before, use the master ID from master_id_dict
        if unique_id in master_id_dict:
            master_id = master_id_dict[unique_id]
        else:
            master_id_counter += 1
            master_id = master_id_counter
            master_id_dict[unique_id] = master_id

        # Update master_id for this record if empty or different
        if not record.get('sd48be64b7') or int(record.get('sd48be64b7')) != master_id:
            update_record(
                app_id=record['application_id'],
                record_id=record['id'],
                master_id=master_id
            )

        seen.add(unique_id)


def main():
    app_id = "64e55236fe94933e2e380e60"
    records = fetch_records(app_id)
    assign_master_id(records)
    
    # Triggering the second script
    second_script_path = Path(os.path.expanduser("~")) / "Desktop" / "trailmasteridupdates.py"
    subprocess.run(["python3", str(second_script_path)])

if __name__ == '__main__':
    main()
