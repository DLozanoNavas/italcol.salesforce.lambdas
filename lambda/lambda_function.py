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

# Lambda handler for the THIRD PARTIES view
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

        # Send the mapped results from the Views to the respective salesforce endpoints
        success_responses = []
        failed_responses = []
        duplicated_responses = []
        for body in mapped_data:
            try:
                # Query dynamo to check if accountnumber (nit) and company is already added
                if check_salesforce_record(selected_company, body.get("AccountNumber")):
                    duplicated_responses.append(body)
                    continue
                # If not on dynamo, create
                else:
                    response = requests.post(
                        url = f"{auth_response['base_url']}{config['API_URI']}",
                        json = body,
                        headers = {"Content-Type" : "application/json", "Authorization" : f"Bearer {auth_response['access_token']}"}
                    )
                    table.put_item(
                        Item = {
                            'compania': selected_company,
                            'nit' : body.get("AccountNumber"),
                            'id_salesforce': response.json().get("id")
                        }
                    )
                    success_responses.append(response.json())
            except Exception as e:
                failed_responses.append({"error":str(e)})

        # Close connection
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Success",
                "records_sent": len(success_responses),
                "failed_records": len(failed_responses),
                "salesforce_duplicated": len(duplicated_responses)
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
    return "Item" in response