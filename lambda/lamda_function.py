import pymssql
import os

def lambda_handler(event, context):
    try:
        # Datos de conexión
        server = os.environ['DB_HOST']  # Dirección del RDS
        user = os.environ['DB_USER']    # Usuario de la base de datos
        password = os.environ['DB_PASS']  # Contraseña
        database = os.environ['DB_NAME']  # Nombre de la base de datos

        # Conectar a SQL Server
        conn = pymssql.connect(server, user, password, database)
        cursor = conn.cursor()
        
        # Consulta de prueba
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()

        # Cerrar la conexión
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": f"Conexión exitosa a SQL Server. Versión: {row[0]}"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error en la conexión: {str(e)}"
        }