import pymssql
import os
import time
import requests

VIEWS = {
    "PUNTO_ENVIO" : "SELECT * FROM vwSalesforce_PuntoEnvio", # Subject to future changes, limit which fields are being recovered
    "THIRD_PARTIES" : "SELECT * FROM vwSalesforce_Terceros"
}
API_ENDPOINTS = {
    "PUNTO_ENVIO" : "http://localhost:1234",    # TODO retrieve from env variables and use real urls
    "THIRD_PARTIES" : "http://localhost:1234"   # TODO retrieve from env variables and use real urls
}

AUTH_URL = "" #TODO Add the authentication url
AUTH_BODY = {
    "username": os.getenv("AUTH_USER"),
    "password": os.getenv("AUTH_PASSWORD")
}
auth_token = ""

def lambda_handler(event, context):
    try:
        # Datos de conexión
        server = os.environ['DB_HOST']  # Dirección del RDS
        user = os.environ['DB_USER']    # Usuario de la base de datos
        password = os.environ['DB_PASS']  # Contraseña
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
        db_rows = cursor.fetchone()

        # Authenticate to perform latter REST requests
        # auth_token = get_auth_token()

        # Map query data to a body, then post body to the selected url
        bodies_from_view = map_from_view(selected_view, db_rows)
        for body in bodies_from_view:
            print(body)
            # TODO: Uncomment next line, first the auth logic must be well implemented and tested
            # requests.post(url= API_ENDPOINTS[selected_view], json=body, headers={"Content-Type" : "application/json", "Authorization": f"Bearer {auth_token}"})            

        # Close connection
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": body
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
def get_auth_token():
    response = requests.post(AUTH_URL, json=AUTH_BODY, headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get("access_token")
    
    raise Exception(f"Authentication failed: {response.text}")
