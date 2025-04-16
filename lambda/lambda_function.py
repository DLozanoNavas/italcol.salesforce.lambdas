import pymssql
import os
import requests
import json

# This is the lambda handler for the THIRD PARTIES view
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
        
        mapped_data = map_from_view(selected_company, rows)

        print(mapped_data)
        #lambda_client = boto3.client('lambda')
        #response = lambda_client.invoke(
        #    FunctionName="Test_ConectVPC",
        #    InvocationType="Event",
        #    Payload=json.dumps(mapped_data, default=str)
        #)

        #print(response)

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
    if selected_company == "CASABLANCA":
        client_id_field_name = "Id_Cliente_ERP_Casablanca__c"
    elif selected_company == "ITALCOL":
        client_id_field_name = "Id_Cliente_ERP_Balanceados__c"
    elif selected_company == "SANTA_REYES":
        client_id_field_name = "Id_Cliente_ERP_Italhuevo__c"
        
    mapped_list = []

    for row in db_rows:
        row_body = {
            client_id_field_name : row["F200_nit"],
            "AccountNumber" : row["F200_nit"],
            "Type" : "Costumer",
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
            row_body["FirstName"] = row[""]
            row_body["LastName"] = f"{row["f200_apellido1"]} {row["f200_apellido2"]}"
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