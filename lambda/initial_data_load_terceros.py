import pandas as pd
import mapper
import requests
import salesforce
import json
import sys, time

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
   },
   "ic":{
      "sheetName" : "italcol",
      "company" : "ITALCOL"
   }
}

selected_company = "cb"

def load_views_from_file(filepath: str, sheet_name: str) -> list[dict]:
  try:
    df = pd.read_excel(filepath, sheet_name=sheet_name, engine="openpyxl", keep_default_na=False)
    df.dropna(how='all', inplace=True)
    return df.to_dict(orient="records")
  except Exception as e:
    print(f"Error loading sheet '{sheet_name}' : {e}")

def update_existing_salesforce_records(url: str, auth_data: dict):
  # First GET all existing salesforce records
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

  FIELD_TO_COMPANY= {
    "Id_Cliente_ERP_Casablanca__c":"CASABLANCA",
    "Id_Cliente_ERP_Balanceados__c":"ITALCOL",
    "Id_Cliente_ERP_Italhuevo__c": "SANTA_REYES"
}
  existing_salesforce_records = []
  for record in existing_records:
    compania = None
    nit = None
    id = record.get("Id")

    #Check each possible Id_Cliente_ERP field
    for field, company_name in FIELD_TO_COMPANY.items():
      if record.get(field) is not None:
        compania = company_name
        nit = record[field]
        existing_salesforce_records.append({
          "compania" : {
            "S": compania
          },
          "nit" : {
            "S" : nit
          },
          "id_salesforce_tercero" : {
            "S" : id
          },
          "sucursales":{
            "L":[]
          }
        })
        break
  store_salesforce_data(existing_salesforce_records)

def store_salesforce_data(data):
  with open("./data/existing_salesforce_records.json", "w") as file:
    file.write(json.dumps(data))

def check_duplicate_existing_salesforce_records(mappedData: list[dict]):
  # Open file containing the salesforce records that are already added
  with open("./data/existing_salesforce_records.json", "r") as file:
    data = json.load(file)
  
  # Checks which records are already created with the current company (CASABLANCA or SANTAREYES or ITALCOL)
  company = companies_dict[selected_company]["company"]
  data = [item for item in data if item.get('compania')['S'] == company]
  
  # Builds a set with all of the nit values to exclude
  nit_set = {
    int(item['nit']['S']) : item['id_salesforce_tercero']['S'] 
    for item in data if 'nit' in item
  }

  # Exclude items with matching AccountNumber in mappedData
  filtered_mappedData = []
  excluded_data = []

  for entry in mappedData:
    account_number = entry.get("AccountNumber")
    if account_number not in nit_set:
      filtered_mappedData.append(entry)
    else:
      excluded_entry = entry.copy()
      print(nit_set[account_number])
      excluded_entry["id_salesforce_tercero"] = nit_set[account_number]
      excluded_data.append(excluded_entry)

  filtered_records, mapped_records = len(filtered_mappedData), len(mappedData) 
  if filtered_records == mapped_records:
    print(f"There are no duplicated records, posting all {mapped_records} records to SalesForce")
  else:
    print(f"Skipping {mapped_records-filtered_records} existing records, posting {filtered_records} records to SalesForce")
  return filtered_mappedData, excluded_data

def post_new_salesforce_records(url: str, auth_data: dict, data: list[dict]):
  headers = {
    "Authorization": f"Bearer {auth_data['access_token']}",
    "Content-Type": "application/json"
  } 
  error_list = [] 
  
  total = len(data)
  batch_size = 200
  start_time = time.time()

  for idx in range(0, total, batch_size):
    batch = data[idx:idx+batch_size]
    payload = {
      "records" : [
        {
          "attributes": {"type":"Account", "referenceId":"ref{index}"},
          **record
        } for index, record in enumerate(batch)
      ]
    }
    current_time = time.time()
    elapsed = current_time - start_time
    avg_time = elapsed / (idx + 1)
    remaining = (total - idx - 1) * avg_time
    print(
      f"Posting {idx+1}/{total} | "
      f"Elapsed: {elapsed:.1f}s | "
      f"ETA: {remaining:.1f}s | "
      f"AVG per request: {avg_time:.2f}s", end="\r"
    )
    try:
      response = requests.post(url=url, json=payload, headers=headers)
      if response.status_code not in (200, 201):
        error_list.append(response)
        print(f"\nBatch failed: {response.text}")
    except Exception as e:
      print(f"\nException occurred: {e}")
  
  print()
  total_time = time.time() - start_time
  print(f"\nFinished posting {total} records in {total_time:.2f}s.")
  return error_list

def patch_existing_records(existing_records: list[dict], auth_data: list[dict]):
  base_url = auth_data["base_url"]
  access_token = auth_data["access_token"]
  
  headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
  }

  batch_size = 200  # Salesforce limit
  total = len(existing_records)
  errors = []
  start_time = time.time()

  url = f"{base_url}/services/data/v63.0/composite/sobjects"
  for idx in range(0, total, batch_size):
    batch = existing_records[idx:idx+batch_size]

    records_payload = [
      {
        "attributes": {"type": "Account"},
        "Id": record.pop("id_salesforce_tercero"),
        **record
      }
      for record in batch
      if "id_salesforce_tercero" in record
    ]

    payload = {
      "allOrNone": False,
      "records": records_payload
    }

    response = requests.patch(url=url, headers=headers, json=payload)
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
    path = "../Terceros.xlsx"
    sheet = companies_dict[selected_company].get("sheetName")
    data = load_views_from_file(path, sheet)
    print(f"Loaded {len(data)} registries from {sheet}")
    mappedData = mapper.map_third_parties_from_view(companies_dict[selected_company].get("company"), data)

    # Necessary configurations and data for interaction with salesforce API
    auth_result = salesforce.authenticate(config["AUTH_URL"], config["AUTH_PARAMS"])
    get_url = f"{auth_result['base_url']}{config['API_URIS'].get('QUERY').get('TERCEROS')}"
    post_url = f"{auth_result['base_url']}{config['API_URIS'].get('POST').get('TERCEROS')}"

    if config["UPDATE_SALESFORCE_RECORDS"]:
      update_existing_salesforce_records(get_url, auth_result)
    
    filtered_duplicate_data, excluded_data = check_duplicate_existing_salesforce_records(mappedData[0:10])
    patch_existing_records(existing_records=excluded_data, auth_data=auth_result)
    #post_new_salesforce_records(post_url, auth_result, filtered_duplicate_data)

    #update_existing_salesforce_records(get_url, auth_result)
    #print(error_list)
