import pandas as pd
import time
import mapper
import requests
import salesforce
import json
import sys

with open("config.json", "r") as file:
  config = json.load(file)

companies_dict = {
   "cb":{
      "sheetName" : "casablanca",
      "company" : "CASABLANCA"
   },
   "sr":{
      "sheetName" : "santa_reyes",
      "company" : "SANTA_REYES"
   }
}

selected_company = "sr"

def load_views_from_file(filepath: str, sheet_name: str) -> list[dict]:
  try:
    df = pd.read_excel(filepath, sheet_name=sheet_name, engine="openpyxl", keep_default_na=False)
    df.dropna(how='all', inplace=True)
    return df.to_dict(orient="records")
  except Exception as e:
    print(f"Error loading sheet '{sheet_name}' : {e}")

def load_existing_records():
  with open("./data/existing_salesforce_records.json", "r") as file:
    return json.load(file)

def update_existing_salesforce_records(url: str, auth_data: dict):
  field_names = {
    "cb": "ti_Id_PV_ERP_Casablanca__c",
    "ic": "Id_PV_ERP_Balanceados__c",
    "sr": "ti_Id_PV_ERP_Italhuevo__c"
  }
  # First GET all existing salesforce records for sucursales
  existing_records = []
  query_url = f"{url}"

  while query_url:
    print(f"Checking existing salesforce records at {query_url}")
    response = requests.get(
    url = query_url,
    headers={
      "Authorization" : f"Bearer {auth_data["access_token"]}"}
    )
    data = response.json()

    records = data.get("records", [])
    existing_records.extend(records)

    next_url_fragment = data.get("nextRecordsUrl")
    if next_url_fragment:
      query_url = f"{auth_data["base_url"]}{next_url_fragment}"
    else:
      query_url = None

  print(f"{len(existing_records)} records retrieved from salesforce")
  # Extract only useful data from SalesForce response
  existing_records = [
    {
      'Id': record['Id'],
      'AccountId': record['AccountId'],
      field_names["cb"]: record[field_names["cb"]],
      field_names["ic"]: record[field_names["ic"]],
      field_names["sr"]: record[field_names["sr"]]
    } for record in existing_records]
  # Load data that will be compare against and index it by id_salesforce_tercero to simplify search operations
  records_data = load_existing_records()
  salesforce_indexed_data = {item['id_salesforce_tercero'].get("S"): item for item in records_data}

  for record in existing_records:
    account_id = record['AccountId']
    store_id = record['Id']
    id_erp = next((record[field] for field in field_names.values() if record.get(field)), "")
    match = salesforce_indexed_data.get(account_id)
    if match:
      new_sucursal = {
        "M":{
          "id_erp" : {"S" : id_erp},
          "id_salesforce_sucursal" : {"S": store_id}
        }
      }
      already_exists = any(
        sucursal.get("M", {}).get("id_erp", {}).get("S") == id_erp
        for sucursal in match.get("sucursales").get("L", [])
      )

      if not already_exists:
          match["sucursales"]["L"].append(new_sucursal)

  # Load existing salesforce records from file
  store_salesforce_data(list(salesforce_indexed_data.values()))

def store_salesforce_data(data):
  with open("./data/existing_salesforce_records.json", "w") as file:
    file.write(json.dumps(data))

def check_duplicate_existing_salesforce_records(mapped_data):
    """
    Checks if the incoming mapped data contains ERP branch IDs already present
    in the existing Salesforce records.
    
    Returns:
        filtered_data: list of new, non-duplicate mapped records
    """
    try:
        with open("./data/existing_salesforce_records.json", "r") as file:
            existing_records = json.load(file)
    except Exception as e:
        print(f"Error reading existing Salesforce records: {e}")
        return mapped_data  # fallback: assume none are duplicates
    # Gather all existing ERP IDs from sucursale
    existing_erp_keys = {}
    for record in existing_records:
      for sucursal in record.get("sucursales", {}).get("L", []):
        erp_id = sucursal.get("M").get("id_erp").get("S")
        salesforce_id = sucursal.get("M").get("id_salesforce_sucursal").get("S")
        if erp_id and salesforce_id:
          existing_erp_keys[erp_id] = salesforce_id
    
    filtered_data = []
    already_existing_data = []
    for record in mapped_data:
        # Determine which ERP ID key is present
        erp_id = (
            record.get("ti_Id_PV_ERP_Casablanca__c") or
            record.get("Id_PV_ERP_Balanceados__c") or
            record.get("ti_Id_PV_ERP_Italhuevo__c")
        )
        
        if erp_id not in existing_erp_keys:
            filtered_data.append(record)
        else:
           existing_record = record.copy()
           existing_record["id_salesforce_sucursal"] = existing_erp_keys[erp_id]
           already_existing_data.append(existing_record)

    filtered_records, mapped_records = len(filtered_data), len(mapped_data)
    if filtered_records == mapped_records:
      print(f"There are no duplicated records, posting all {mapped_records} records to SalesForce")
    else:
      print(f"Skipping {mapped_records-filtered_records} existing records, posting {filtered_records} records to SalesForce")
    return filtered_data, already_existing_data

def post_new_salesforce_records(url: str, auth_data: dict, data: list[dict]):
    headers = {
        "Authorization": f"Bearer {auth_data['access_token']}",
        "Content-Type": "application/json"
    } 
    error_list = [] 
    existing_records = load_existing_records()
    indexed_records = {item['nit'].get("S"): item for item in existing_records}
    
    valid_data = []
    for body in data:
        id = body.pop("id_sucursal")
        third_party_nit = id.split("-")[0]
        if not indexed_records.get(third_party_nit):
            print(f"Third party with nit {third_party_nit} not found, skipping")
        else:
            third_party_sf_id = indexed_records[third_party_nit]['id_salesforce_tercero']['S']
            body["AccountId"] = third_party_sf_id
            valid_data.append(body)
    
    total = len(data)
    batch_size = 200  # Salesforce limit
    start_time = time.time()

    for idx in range(0, total, batch_size):
        batch = valid_data[idx:idx+batch_size]
        payload = {
            "records": [
                {
                    "attributes": {"type": "RetailStore", "referenceId": f"ref{index}"},
                    **record
                } for index, record in enumerate(batch)
            ]
        }
        current_time = time.time()
        elapsed = current_time - start_time
        avg_time = elapsed / ((idx // batch_size) + 1)
        remaining = ((total - idx - len(batch)) // batch_size + 1) * avg_time

        print(
            f"Posting {idx + len(batch)}/{total} | "
            f"Elapsed: {elapsed:.1f}s | "
            f"ETA: {remaining:.1f}s | "
            f"AVG Time per Request: {avg_time:.2f}s", end="\r"
        )
        sys.stdout.flush()

        try:
            response = requests.post(url=url, json=payload, headers=headers)
            if response.status_code not in (200, 201):
                error_list.append(response)
                print(f"\nBatch failed: {response.text}")
        except Exception as e:
            print(f"\nException occurred: {e}")

    total_time = time.time() - start_time
    print(f"\nFinished posting {total} records in {total_time:.2f}s.")

def match_account_id_to_store(stores_list):
  existing_records = load_existing_records()
  indexed_records = {item['nit'].get("S"): item for item in existing_records}
  valid_data = []

  for body in stores_list:
        id = body.pop("id_sucursal")
        third_party_nit = id.split("-")[0]
        if not indexed_records.get(third_party_nit):
            print(f"Third party with nit {third_party_nit} not found, skipping")
        else:
            third_party_sf_id = indexed_records[third_party_nit]['id_salesforce_tercero']['S']
            body["AccountId"] = third_party_sf_id
            valid_data.append(body)
  return valid_data

def patch_existing_records(existing_records: list[dict], auth_data: list[dict]):
  base_url = auth_data["base_url"]
  access_token = auth_data["access_token"]
  
  headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
  }

  batch_size = 200  # Salesforce limit
  errors = []
  start_time = time.time()
  valid_records = match_account_id_to_store(existing_records)
  total = len(valid_records)
  url = f"{base_url}/services/data/v63.0/composite/sobjects"
  for idx in range(0, total, batch_size):
    batch = valid_records[idx:idx+batch_size]

    records_payload = [
      {
        "attributes": {"type": "RetailStore"},
        "Id": record.pop("id_salesforce_sucursal"),
        **record
      }
      for record in batch
      if "id_salesforce_sucursal" in record
    ]

    payload = {
      "allOrNone": False,
      "records": records_payload
    }

    response = requests.patch(url=url, headers=headers, json=payload)
    print(f"{idx} records updated")
    if response.status_code != 200:
      print(f"Batch {idx} failed: {response.text}")
      errors.append(response.text)
  elapsed = time.time() - start_time
  print(f"Finished updating {total} records in {elapsed:.2f} seconds.")

  if errors:
    print(f"Encountered {len(errors)} errors during batch update.")
  else:
    print("All records updated successfully.")

if __name__ == "__main__":
    # Load data from excel file and map it to a salesforce compatible format
    path = "../sucursales.xlsx"
    sheet = companies_dict[selected_company].get("sheetName")
    data = load_views_from_file(path, sheet)
    print(f"Loaded {len(data)} registries from {sheet}")
    mappedData = mapper.map_retail_stores_from_view(companies_dict[selected_company].get("company"), data)
    
    # Necessary configurations and data for interaction with salesforce API
    auth_result = salesforce.authenticate(config["AUTH_URL"], config["AUTH_PARAMS"])
    get_url = f"{auth_result['base_url']}{config['API_URIS'].get('QUERY').get('PUNTO_ENVIO')}"
    post_url = f"{auth_result['base_url']}{config['API_URIS'].get('POST').get('PUNTO_ENVIO')}"

    if config["UPDATE_SALESFORCE_RECORDS"]:
      update_existing_salesforce_records(get_url, auth_result)
    
    filtered_duplicate_data, existing_data = check_duplicate_existing_salesforce_records(mappedData)

    patch_existing_records(existing_records=existing_data, auth_data=auth_result)
    #post_new_salesforce_records(post_url, auth_result, filtered_duplicate_data)
