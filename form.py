import json
import os
import requests
import gspread
from pathlib import Path

def get_next_title(existing_records):
    highest_title = 0
    for record in existing_records:
        try:
            title = int(record.get('title', 0))
            if title > highest_title:
                highest_title = title
        except ValueError:
            continue
    next_title = highest_title + 1
    print(f"Next title to be used: {next_title}")
    return str(next_title)

def fetch_all_records(headers, APP_ID):
    list_records_url = f"https://app.smartsuite.com/api/v1/applications/{APP_ID}/records/list/"
    response = requests.post(list_records_url, headers=headers)
    if response.status_code == 200:
        items = response.json().get('items', [])
        return items
    else:
        print(f"Failed to fetch all records: {response.text}")
        return []

def load_auth_file(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"{file_path.name} file not found at {file_path}. Please make sure the file exists.")
        exit()

def fetch_existing_records(passport_number, headers, APP_ID):
    list_records_url = f"https://app.smartsuite.com/api/v1/applications/{APP_ID}/records/list/"
    filter_payload = {
        "operator": "and",
        "fields": [{"comparison": "eq", "field": "sbb8fea034", "value": str(passport_number)}]  # Convert passport_number to string
    }
    
    print(f"Debug: Filter payload being sent: {filter_payload}")  # Debug line

    response = requests.post(list_records_url, headers=headers, json=filter_payload)
    
    print(f"Debug: Response from server: {response.text}")  # Debug line

    if response.status_code == 200:
        items = response.json().get('items', [])
        
        # Convert passport_number to string before comparing
        matching_records = [item for item in items if str(item.get('sbb8fea034')) == str(passport_number)]
        
        return matching_records
    else:
        print(f"Failed to fetch existing records: {response.text}")
        return []


def map_fields(record):
    fields = {}
    field_mappings = {
        's37af43f83': 'Full Name',
        'sac87d276d': 'Date of Birth',
        'sac950cfcc': 'Email Address',
        'sb91047f0b': 'Address',
        's5d2aed3fd': 'Can we text you statistics about your trail progress, coupons and promotions, and info about weekly brewery happenings?',
        's37e762ac3': 'Phone number',
        'sbb8fea034': 'Passport Number'
    }
    for api_key, sheet_key in field_mappings.items():
        if record.get(sheet_key):
            if sheet_key == 'Can we text you statistics about your trail progress, coupons and promotions, and info about weekly brewery happenings?':
                fields[api_key] = record[sheet_key].lower() == 'yes'
            elif sheet_key == 'Full Name':
                first_name, last_name = record[sheet_key].split(' ', 1)
                fields[api_key] = {"first_name": first_name, "last_name": last_name}
            elif sheet_key == 'Address':
                addr_parts = record[sheet_key].split(' ')
                zip_code = addr_parts[-1]
                state = addr_parts[-2]
                city = addr_parts[-3]
                address_line = ' '.join(addr_parts[:-3])
                fields[api_key] = {
                    "location_address": address_line,
                    "location_city": city,
                    "location_state": state,
                    "location_zip": zip_code,
                    "location_country": "United States"  # Assuming the country is always the U.S.
                }
            elif sheet_key == 'Passport Number':
                fields[api_key] = str(record[sheet_key])
            elif sheet_key == 'Phone number':
                phone = [{
                    "phone_country": "US",  # Assuming the country is always the U.S.
                    "phone_number": str(record[sheet_key]),
                    "phone_type": 2  # Assuming it's always a mobile phone
                }]
                fields[api_key] = phone
            else:
                fields[api_key] = record[sheet_key]
    return fields
def main():
    auth_path = Path(os.path.expanduser("~")) / "Desktop" / "auth.json"
    google_auth_path = Path(os.path.expanduser("~")) / "Desktop" / "google.json"
    smartsuite_auth = load_auth_file(auth_path)
    google_auth = load_auth_file(google_auth_path)

    gc = gspread.service_account_from_dict(google_auth)
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1xqTTlR9GIFWtxbH-KaIvyqgFWdU26YTpEq7rkLLzN4g/edit?usp=sharing")
    worksheet = sh.get_worksheet(0)

    SMARTSUITE_API_KEY = smartsuite_auth['Authorization'].split(' ')[1]
    SMARTSUITE_ACCOUNT_ID = smartsuite_auth['ACCOUNT-ID']
    APP_ID = '64e55236fe94933e2e380e60'

    headers = {
        "Authorization": f"Token {SMARTSUITE_API_KEY}",
        "Account-Id": SMARTSUITE_ACCOUNT_ID
    }

    all_existing_records = fetch_all_records(headers, APP_ID)
    next_title = get_next_title(all_existing_records)

    records_from_google = worksheet.get_all_records()

    for record in records_from_google:
        passport_number = record.get('Passport Number')
        existing_records = fetch_existing_records(passport_number, headers, APP_ID)

        print(f"Debug: Passport Number being processed: {passport_number}")
        print(f"Debug: Existing records fetched for this passport: {existing_records}")

        mapped_fields = map_fields(record)

        if existing_records:
            for existing_record in existing_records:
                record_id = existing_record.get("id")
                mapped_fields['title'] = existing_record.get('title')
                
                response = requests.patch(
                    f"https://app.smartsuite.com/api/v1/applications/{APP_ID}/records/{record_id}/",
                    headers=headers,
                    json=mapped_fields
                )
                if response.status_code != 200:
                    print(f"Failed to update existing record: {response.text}")
        else:
            mapped_fields['title'] = next_title
            next_title = str(int(next_title) + 1)

            response = requests.post(
                f"https://app.smartsuite.com/api/v1/applications/{APP_ID}/records/",
                headers=headers,
                json=mapped_fields
            )
            if response.status_code != 201:
                print(f"Failed to create new record: {response.text}")

if __name__ == "__main__":
    main()
