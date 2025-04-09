import pymssql
import os
import requests
import json

def lambda_handler(event, context):
    try:
        with open("config.json", "r") as file:
            config = json.load(file)

        # Set configurations
        VIEWS = config["VIEWS"]
        API_URIS = config["API_URIS"]

        # Database connection
        server = os.environ['DB_HOST']  # Dirección del RDS
        user = os.environ['DB_USER']    # Usuario de la base de datos
        password = os.environ['DB_PASSWORD']  # Contraseña
        database = os.environ['DB_NAME']  # Nombre de la base de datos

        selected_view = "PUNTO_ENVIO" # TODO this must be selected dynamically, probably from ENV Variables or if possible using event input from the lambda handler

        # Conectar a SQL Server
        conn = pymssql.connect(server, user, password, database)
        cursor = conn.cursor()

        # Check if selected_view type is in dictionary 
        if selected_view not in VIEWS:
            return {
            "statusCode": 500,
            "body": f"Invalid View Selected"
          }
        
        # Select view and perform query
        query = VIEWS[selected_view]
        cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]

        rows = [dict(zip(columns,row)) for row in cursor.fetchall()]

        # Authenticate to perform latter REST requests
        auth_response = get_auth_token(config["AUTH_URL"], config["AUTH_PARAMS"])
        
        print(auth_response["base_url"])        
        for row in rows:
            print(row)
            # Map query data to a JSON body, then post body to the selected url
            # TODO: Uncomment next statement, first the auth logic must be well implemented and tested
            """
            requests.post(
                url= f"{auth_response["base_url"]}{API_URIS[selected_view]}", 
                json=body,
                headers=
                  {
                    "Content-Type" : "application/json",
                    "Authorization": f"Bearer {auth_token["access_token"]}"
                  }
                )            
            """



        # Close connection
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": auth_response["base_url"]
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error en la conexión: {str(e)}"
        }
    
def map_from_view(selected_view, db_rows):
    # Mapping logic based on what view was selected
    # TODO: Map data using client requirements (still pending)
    if selected_view == "PUNTO_ENVIO":
        return [{} for row in db_rows]
    elif selected_view == "THIRD_PARTIES":
        return [{} for row in db_rows]


# Authentication method, returns the token string to be later added to the requests header
def get_auth_token(url, auth_params):
    response = requests.post(url, params=auth_params, headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        token_data = response.json()
        return {
            "access_token" : token_data.get("access_token"),
            "base_url": token_data.get("instance_url")
        }
    raise Exception(f"Authentication failed: {response.text}")
