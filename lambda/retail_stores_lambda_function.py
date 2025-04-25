import requests
import json
import databases, salesforce, mapper
import boto3
from boto3.dynamodb.conditions import Key

ALLOWED_COMPANIES = {"CASABLANCA", "ITALCOL", "SANTA_REYES"}
required_config_keys = ["VIEW", "AUTH_PARAMS", "AUTH_URL", "API_URI"] # This are all necessary to run the lambda correctly

# Connect to dynamodb
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("salesforce")

# Lambda handler for the RETAIL STORES view
def lambda_handler(event, context):
    try:
        # Open config file
        with open("config.json", "r") as file:
            config = json.load(file)
        # And check if there's anything missing in the configurations
        missing = [key for key in required_config_keys if key not in config]
        if missing:
            raise KeyError(f"Missing keys in config.json: {', '.join(missing)}")
        
        # Set configuration
        selected_company = event.get("selected_company")
        validate_selected_company(selected_company)

        # Connect to database
        cursor, conn = databases.connect_to_database(selected_company)
        
        # Select view and perform query
        query = config["VIEW"]
        cursor.execute(query)

        # Convert query results to usable data        
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns,row)) for row in cursor.fetchall()]
        mapped_data = mapper.map_from_view(selected_company, rows)

        # Authenticate against salesforce API
        auth_params = config["AUTH_PARAMS"]         #NOTE this should be retrieved from SecretsManager when using prod credentials
        auth_url = config["AUTH_URL"]
        auth_response = salesforce.authenticate(auth_url, auth_params)
        auth_response["base_url"] = f"{auth_response['base_url']}{config['API_URI']}"
        # Send the mapped results from the Views to the respective salesforce endpoints
        success_responses = []
        failed_responses = []
        for body in mapped_data:
            try:
                third_party_nit = body.pop("id_sucursal").split("-")[0]
                
                # Query dynamo to check if third party with accountnumber (nit) and company exists
                add_retail_store_if_not_exists(selected_company, third_party_nit, body, auth_response)
            except Exception as e:
                failed_responses.append({"error":str(e)})

        # Close connection
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Success",
                "company": selected_company,
                "records_sent": len(success_responses), #TODO update logs for better trazability
                "failed_records": failed_responses
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error en la conexión: {str(e)}"
        }

def validate_selected_company(selected_company):
    if not selected_company:
        raise ValueError("Missing 'selected_company' in request.")

    if selected_company not in ALLOWED_COMPANIES:
        raise ValueError(f"Invalid 'selected_company': '{selected_company}'. Must be one of {', '.join(ALLOWED_COMPANIES)}.")

def check_salesforce_record(name, nit):
    response = table.get_item(
        Key = {
            'compania' : name,
            'nit': nit
        }
    )
    return response.get("Item")

def add_retail_store_if_not_exists(company, nit, new_retail_store, auth_response):
    # Adds new record to the sucursales field in dynamodb table if there's no item with the same id_erp

    response = table.get_item(Key={'compania': company, 'nit':nit})
    item = response.get("Item")

    if not item:
        raise Exception(f"No third party with nit {nit} was found associated with company {company}")
    
    salesforce_account_id = item.get("id_salesforce_tercero")
    existing_retail_stores = item.get("sucursales", [])

    salesforce_url = auth_response['base_url']
    access_token = auth_response['access_token']

    for store in existing_retail_stores:
        if store["id_erp"] == new_retail_store["id_sucursal"]:
            # Send PATCH endpoint
            return
    
    # Send POST with new retail store and retrieve new retailstore id from salesforce
    new_retail_store["AccountId"] = salesforce_account_id
    
    post_response = requests.post(
        url = salesforce_url,
        json = new_retail_store,
        headers ={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
    )
    if post_response.status_code not in [200, 201]:
        raise Exception(f"Error al crear sucursal en Salesforce: {post_response.text}")
    
    print(post_response.json())
    existing_retail_stores.append({"id_erp":new_retail_store["id_sucursal"], "id_salesforce_tercero":""})
