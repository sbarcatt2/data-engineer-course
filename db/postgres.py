import os
from dotenv import load_dotenv
import psycopg2
load_dotenv() # busca .env en el directorio actual

def conectar_db():
    """
    Establece conexión con la base de datos PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST'),
            database=os.getenv('PG_DB'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            port=5432,
            sslmode='require'
        )
        print("Conexión exitosa a la base de datos PostgreSQL")
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos PostgreSQL: {e}")
        return None
