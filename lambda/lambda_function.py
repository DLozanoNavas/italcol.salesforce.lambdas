import pymssql
import os
import requests
import json

# This will be the lambda handler for the THIRD PARTIES view
def lambda_handler(event, context):
    try:
        with open("config.json", "r") as file:
            config = json.load(file)

        # Set configurations
        VIEW_TYPE = "THIRD_PARTIES"
        VIEWS = config["VIEWS"]
        API_URIS = config["API_URIS"]
        selected_company = event.get("selected_company")

        # Connect to database
        cursor, conn = connect_to_database(selected_company)
        
        # Select view and perform query
        query = VIEWS[VIEW_TYPE]
        cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]

        rows = [dict(zip(columns,row)) for row in cursor.fetchall()]

        print(rows)

        #print(auth_response["base_url"])        
            # Map query data to a JSON body, then post body to the selected url
            # TODO: Uncomment next statement, first the auth logic must be well implemented and tested
            
            #requests.post(
            #    url= f"{auth_response["base_url"]}{API_URIS[selected_view]}", 
            #    json=body,
            #    headers=
            #      {
            #        "Content-Type" : "application/json",
            #        "Authorization": f"Bearer {auth_token["access_token"]}"
            #      }
            #    )            

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
    
def map_from_view(selected_view, db_rows):
    # Mapping logic based on what view was selected
    # TODO: Map data using client requirements (still pending)
    return [{} for row in db_rows]
    
def connect_to_database(selected_company):
    # Select which database to select based on event input
    if selected_company == "CASABLANCA":
      database = os.environ['DB_NAME']  # Nombre de la base de datos
    elif selected_company == "SANTA_REYES":
      database = os.environ['DB_NAME1']      

    if selected_company == "CASABLANCA"  or selected_company == "SANTA_REYES":
      server = os.environ['DB_HOST']  # Dirección del RDS
    
    user = os.environ['DB_USER']    # Usuario de la base de datos
    password = os.environ['DB_PASSWORD']  # Contraseña

    # Conectar a SQL Server
    conn = pymssql.connect(server, user, password, database)
    cursor = conn.cursor()
    return cursor, conn