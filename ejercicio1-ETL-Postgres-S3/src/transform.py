import sqlite3
import pandas as pd


def transformar_datos(df_accounts, df_accounts_subscription, df_subscription):
    """
    Transforma los datos del DataFrame extraído usando SQL en una DB SQLite en memoria
    para generar un reporte de cuentas creadas por mes
    """
    try:
        # Crear una conexión a una base de datos SQLite en memoria
        conn_sqlite = sqlite3.connect(':memory:')
        
        # Cargar el DataFrame en una tabla temporal en la DB en memoria
        # Asegurarse de que el nombre de la tabla coincida con el usado en la query SQL
        df_accounts.to_sql('accounts', conn_sqlite, index=False, if_exists='replace')
        df_accounts_subscription.to_sql('accounts_subscription', conn_sqlite, index=False, if_exists='replace')
        df_subscription.to_sql('subscriptions', conn_sqlite, index=False, if_exists='replace')
        print("DataFrames cargados en DB SQLite en memoria.")
        
        query = """
        SELECT
            acc.account_id AS 'ID CUENTA',
            acc.account_name AS 'Nombre Cuenta',
            acc.created_at AS 'Fecha creación cuenta',
            MIN(CASE 
                WHEN s.subscription_name = 'Empresarial' THEN acc_sub.start_date 
                ELSE NULL 
            END) AS 'Fecha primer upgrade a Empresarial',
            'Sí' AS 'Hizo upgrade',
            CAST(ROUND(JULIANDAY(MIN(acc_sub.start_date)) - JULIANDAY(acc.created_at)) AS INTEGER)+1 AS 'Días desde creación hasta upgrade'
        FROM accounts acc
        JOIN accounts_subscription acc_sub ON acc.account_id = acc_sub.account_id
        JOIN subscriptions s ON acc_sub.subscription_id = s.subscription_id
        WHERE s.subscription_name = 'Empresarial'
        GROUP BY acc.account_id, acc.account_name, acc.created_at
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