import os
import pymssql

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