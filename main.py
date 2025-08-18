from datetime import datetime

#Conexion a postgres
from db.postgres import conectar_db

#Funciones de extract, transform y load
from src.extract import extraer_datos
from src.transform import transformar_datos
from src.load import guardar_csv, subir_a_s3

import logging
logging.basicConfig(level=logging.INFO)



def main():
    # Conectar a la base de datos PostgreSQL
    conn_pg = conectar_db()
    if conn_pg is None:
        return

    try:
        logging.info("Comienza extraccion de datos.")
        # Extraer datos
        account_columns = ['account_id', 'account_name', 'created_at']
        df_accounts = extraer_datos(conn_pg, 'accounts', account_columns)
        if df_accounts is None:
            return

        account_subscription_columns = ['account_subscription_id', 'account_id', 'subscription_id', 'start_date', 'end_date']
        df_accounts_subscription = extraer_datos(conn_pg, 'accounts_subscription', account_subscription_columns)
        if df_accounts_subscription is None:
            return

        subscription_columns = ['subscription_id', 'subscription_name']
        df_subscription = extraer_datos(conn_pg, 'subscriptions', subscription_columns)
        if df_subscription is None:
            return
        logging.info("Extraccion de datos completada.")
        #imprimir 5 registros de cada df
        print(df_accounts.head())
        print(df_accounts_subscription.head())
        print(df_subscription.head())

        # Transformar datos (pasando el DataFrame extraído)
        logging.info("Comienza transformacion de datos.")
        df_transformado = transformar_datos(df_accounts, df_accounts_subscription, df_subscription) # Pasamos el DataFrame aquí
        if df_transformado is None:
            return
        logging.info("Transformacion de datos completada.")
        
        # Guardar resultados localmente
        logging.info("Comienza guardado de resultados.")
        nombre_archivo = f"reporte_cuentas_por_mes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        archivo_guardado = guardar_csv(df_transformado, nombre_archivo)
        
        if archivo_guardado:
            logging.info("Guardado de resultados completado.")
            # Subir a S3
            logging.info("Comienza subida a S3.")
            subir_a_s3(archivo_guardado)
            logging.info("Subida a S3 completada.")
            
    finally:
        conn_pg.close()

if __name__ == "__main__":
    main() 