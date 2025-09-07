import dlt
from dlt.sources.sql_database import sql_database


def ingesta_ejemplo():
    """
    Ejemplo simplificado de ingesta con DLT.
    Demuestra carga full refresh vs incremental.
    """
    
    # 1. Configurar el pipeline
    pipeline = dlt.pipeline(
        pipeline_name='ejemplo_simple',
        destination='duckdb',  # Base de datos local para el ejemplo
        dataset_name='datos_ejemplo'
    )
    
    # 2. Conectar a la base de datos origen
    db = sql_database()
    
    print("Comienza ingesta!")

    # 3. Configurar tabla con carga FULL REFRESH
    # Esta tabla se recarga completamente cada vez
    tabla_suscription = db.with_resources("subscriptions")
    tabla_suscription.subscriptions.apply_hints(
        write_disposition="replace"  # Reemplaza toda la tabla
    )
    
    # 4. Configurar tabla con carga INCREMENTAL
    # Esta tabla solo carga registros nuevos/modificados
    tabla_accounts = db.with_resources("accounts")
    tabla_accounts.accounts.apply_hints(
        write_disposition="merge",  # Fusiona con datos existentes
        incremental=dlt.sources.incremental("updated_at")  # Solo registros nuevos/modificados
    )

    tabla_contents = db.with_resources("contents")
    tabla_contents.accounts.apply_hints(
        write_disposition="merge",  # Fusiona con datos existentes
        incremental=dlt.sources.incremental("updated_at")  # Solo registros nuevos/modificados
    )

    tabla_premium_features = db.with_resources("premium_features")
    tabla_premium_features.accounts.apply_hints(
        write_disposition="merge",  # Fusiona con datos existentes
        incremental=dlt.sources.incremental("updated_at")  # Solo registros nuevos/modificados
    )
    
    
    # 5. Ejecutar el pipeline
    info = pipeline.run([tabla_suscription, tabla_accounts, tabla_contents, tabla_premium_features])
    
    # 6. Mostrar resultados
    print("✅ Ingesta completada!")
    print(f"📊 Información de la ejecución: {info}")

if __name__ == "__main__":
    ingesta_ejemplo() 