import sqlite3
import pandas as pd
from datetime import datetime
import boto3
import os
import psycopg2

from dotenv import load_dotenv
load_dotenv() # busca .env en el directorio actual

print(os.getenv('PG_HOST'))

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

def extraer_datos(conn):
    """
    Extrae todos los datos de la tabla accounts desde PostgreSQL
    """
    try:
        # Usamos pandas read_sql_query que funciona con conexiones psycopg2
        query = "SELECT * FROM accounts"
        df = pd.read_sql_query(query, conn)
        print(f"\nDatos extraídos: {len(df)} registros")
        return df
    except Exception as e:
        print(f"Error al extraer datos: {e}")
        return None

def transformar_datos(df_extraido):
    """
    Transforma los datos del DataFrame extraído usando SQL en una DB SQLite en memoria
    para generar un reporte de cuentas creadas por mes
    """
    try:
        # Crear una conexión a una base de datos SQLite en memoria
        conn_sqlite = sqlite3.connect(':memory:')
        
        # Cargar el DataFrame en una tabla temporal en la DB en memoria
        # Asegurarse de que el nombre de la tabla coincida con el usado en la query SQL
        df_extraido.to_sql('accounts', conn_sqlite, index=False, if_exists='replace')
        print("DataFrame cargado en DB SQLite en memoria.")
        
        query = """
        SELECT 
            strftime('%Y', created_at) as año,
            strftime('%m', created_at) as mes,
            COUNT(*) as cantidad_cuentas
        FROM accounts
        GROUP BY año, mes
        ORDER BY año, mes
        """
        
        df_transformado = pd.read_sql_query(query, conn_sqlite)
        print("\nReporte generado:")
        print(df_transformado)
        
        # Cerrar la conexión a la DB en memoria
        conn_sqlite.close()
        
        return df_transformado
    except Exception as e:
        print(f"Error en la transformación: {e}")
        return None

def guardar_csv(df, nombre_archivo):
    """
    Guarda los datos transformados en un archivo CSV localmente
    """
    try:
        df.to_csv(nombre_archivo, index=False)
        print(f"\nArchivo guardado exitosamente: {nombre_archivo}")
        return nombre_archivo
    except Exception as e:
        print(f"Error al guardar el archivo: {e}")
        return None

def subir_a_s3(nombre_archivo_local):
    """
    Sube el archivo CSV a S3
    """
    try:
        # Crear cliente de S3
        session = boto3.Session(profile_name='bruno_especializacion')
        s3_client = session.client('s3', region_name='us-west-2')
        
        # Subir archivo
        s3_client.upload_file(
            nombre_archivo_local,
            BUCKET_NAME,
            f"reportes/{nombre_archivo_local}"  # Guardamos en una carpeta 'reportes'
        )
        
        print(f"\nArchivo subido exitosamente a S3: s3://{BUCKET_NAME}/reportes/{nombre_archivo_local}")
        
        # Opcional: eliminar archivo local después de subirlo
        os.remove(nombre_archivo_local)
        print(f"Archivo local eliminado: {nombre_archivo_local}")
        
    except Exception as e:
        print(f"Error al subir archivo a S3: {e}")

def main():
    # Conectar a la base de datos PostgreSQL
    conn_pg = conectar_db()
    if conn_pg is None:
        return

    try:
        # Extraer datos
        df_extraido = extraer_datos(conn_pg)
        if df_extraido is None:
            return
            
        # Transformar datos (pasando el DataFrame extraído)
        df_transformado = transformar_datos(df_extraido) # Pasamos el DataFrame aquí
        if df_transformado is None:
            return
        
        # Guardar resultados localmente
        nombre_archivo = f"reporte_cuentas_por_mes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        archivo_guardado = guardar_csv(df_transformado, nombre_archivo)
        
        if archivo_guardado:
            # Subir a S3
            subir_a_s3(archivo_guardado)
            
    finally:
        conn_pg.close()

if __name__ == "__main__":
    main() 