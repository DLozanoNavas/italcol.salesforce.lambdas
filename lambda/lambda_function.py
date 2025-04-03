import pymssql
import os

VIEWS = {
    "PUNTO_ENVIO" : "SELECT * FROM vwSalesforce_PuntoEnvio", #TODO Subject to future changes
    "THIRD_PARTIES" : "SELECT * FROM vwSalesforce_Terceros"
}    

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
            "body": f"Invalid Query Type"
          }
        
        # Select view and perform query
        query = VIEWS[selected_view]
        cursor.execute(query)
        db_row = cursor.fetchone()

        # With query performed mapped the recieved data to a body
        body = map_from_view(selected_view, db_row)

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
    
def map_from_view(selected_view, db_row):
    print(db_row)

# TODO implement logic to authenticate and consume from an API, endpoint based on which view was selected to be executed.