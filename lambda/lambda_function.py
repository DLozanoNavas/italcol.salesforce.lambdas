import pymssql
import os
import requests
import json

ALLOWED_COMPANIES = {"CASABLANCA", "ITALCOL", "SANTA_REYES"}

# Lambda handler for the THIRD PARTIES view
def lambda_handler(event, context):
    try:
        with open("config.json", "r") as file:
            config = json.load(file)

        # Set configuration
        selected_company = event.get("selected_company")
        validate_selected_company(selected_company)

        # Connect to database
        cursor, conn = connect_to_database(selected_company)
        
        # Select view and perform query
        query = config["VIEW"]
        cursor.execute(query)

        # Convert query results to usable data        
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns,row)) for row in cursor.fetchall()]
        mapped_data = map_from_view(selected_company, rows)

        # Authenticate against salesforce API        
        auth_params = config["AUTH_PARAMS"]         #NOTE this should be retrieved from SecretsManager when using prod credentials
        auth_url = config["AUTH_URL"]
        auth_response = authenticate(auth_url, auth_params)

        # Send the mapped results from the Views to the respective salesforce endpoints
        post_responses = []
        for body in mapped_data:
            try:
                response = requests.post(
                    url = f"{auth_response['base_url']}{config['API_URI']}",
                    json = body,
                    headers = {"Content-Type" : "application/json", "Authorization" : f"Bearer {auth_response['access_token']}"}
                )
                post_responses.append(response.json())
            except Exception as e:
                post_responses.append({"error":str(e)})

        print(post_responses)
        # Close connection
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": "Success"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error en la conexión: {str(e)}"
        }
    
def map_from_view(selected_company, db_rows):
    # Mapping logic based on what view was selected
    COMPANY_ID_FIELD_MAP = {
        "CASABLANCA": "Id_Cliente_ERP_Casablanca__c",
        "ITALCOL": "Id_Cliente_ERP_Balanceados__c",
        "SANTA_REYES": "Id_Cliente_ERP_Italhuevo__c"
    }
    client_id_field_name = COMPANY_ID_FIELD_MAP.get(selected_company)
        
    mapped_list = []

    for row in db_rows:
        row_body = {
            client_id_field_name : row["F200_nit"],
            "AccountNumber" : row["F200_nit"],
            "Type" : "Customer",
            "Industry" : "Retail",        
            "ShippingCity": row["f011_descripcion"],
            "ShippingCountry": row["f013_descripcion"], 
            "ShippingStreet" : row["f015_direccion1"],
            "cu_CIIU__c" : row["f200_id_ciiu"],
        }
        # The following logic requires updating of the views
        if row["f200_id_tipo_ident"] == "N":
            row_body["Name"] = row["f200_razon_social"]
            row_body["cu_colaborador_grupo_italcol__c"] = False
        elif row["f200_id_tipo_ident"] == "C" or row["f200_id_tipo_ident"] == "E":
            row_body["FirstName"] = row["f200_nombres"]
            row_body["LastName"] = f"{row['f200_apellido1']} {row['f200_apellido2']}"
            row_body["Salutation"] = "Sr."
            row_body["cu_colaborador_grupo_italcol__c"] = True
            # Si es colaborador se envia campo co_compania
            if row["f200_ind_empleado"] == 1:
                row_body["co_compania__pc"] = selected_company
        mapped_list.append(row_body)
    return mapped_list
    
def connect_to_database(selected_company):
    # Select which database to select based on event input
    server = os.environ['DB_HOST']  # Dirección del RDS
    if selected_company == "CASABLANCA":
        database = os.environ['DB_NAME']
    elif selected_company == "SANTA_REYES":
        database = os.environ['DB_NAME1']
    elif selected_company == "ITALCOL":
        database = os.environ['DB_NAME2']
        server = os.environ['DB_HOST_ITALCOL']

    user = os.environ['DB_USER']    # Usuario de la base de datos
    password = os.environ['DB_PASSWORD']  # Contraseña

    # Conectar a SQL Server
    # conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}') # With odbc
    conn = pymssql.connect(server, user, password, database)
    cursor = conn.cursor()
    return cursor, conn

def authenticate(url, params):
  response = requests.post(url=url, params=params, headers={"Content-Type": "application/json"})

  if response.status_code == 200:
    token_data = response.json()
    return {
      "access_token" : token_data.get("access_token"),
      "base_url" : token_data.get("instance_url")
    }
  raise Exception(f"Authentication failed: {response.text}")

def validate_selected_company(selected_company):
    if not selected_company:
        raise ValueError("Missing 'selected_company' in request.")

    if selected_company not in ALLOWED_COMPANIES:
        raise ValueError(f"Invalid 'selected_company': '{selected_company}'. Must be one of {', '.join(ALLOWED_COMPANIES)}.")