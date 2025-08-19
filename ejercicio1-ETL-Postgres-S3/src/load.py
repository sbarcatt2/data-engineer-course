import boto3
import os
from dotenv import load_dotenv
load_dotenv() # busca .env en el directorio actual

BUCKET_NAME=os.getenv('BUCKET_NAME')

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
        session = boto3.Session(profile_name='seba_dateneo')
        s3_client = session.client('s3', region_name='us-west-2')
        
        # Subir archivo
        s3_client.upload_file(
            nombre_archivo_local,
            BUCKET_NAME,
            nombre_archivo_local  # Guardamos en una carpeta 'reportes'
        )
        
        print(f"\nArchivo subido exitosamente a S3: s3://{BUCKET_NAME}/reportes/{nombre_archivo_local}")
        
        # Opcional: eliminar archivo local después de subirlo
        os.remove(nombre_archivo_local)
        print(f"Archivo local eliminado: {nombre_archivo_local}")
        
    except Exception as e:
        print(f"Error al subir archivo a S3: {e}")