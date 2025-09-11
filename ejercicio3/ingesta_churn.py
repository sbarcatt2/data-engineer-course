import dlt
import argparse
import os
from dlt.sources.sql_database import sql_database
from dlt.common.pipeline import get_dlt_pipelines_dir
from typing import Dict, List, Optional

# Definir las tablas por defecto como constante global
DEFAULT_TABLES = {
    "accounts": False,
    "accounts_subscription": False,
    "subscriptions": False,
    "contents": True,
    "content_attributes": True
}

def carga_datos(
    env: str = 'local',
    full_refresh_tables: Optional[Dict[str, bool]] = None,
    tables: Optional[List[str]] = None
) -> None:
    """
    Carga datos de las tablas especificadas, permitiendo carga incremental o full refresh.
    
    Args:
        env (str): Entorno de ejecución ('local', 'dev' o 'prod'). Por defecto es 'local'.
        full_refresh_tables (Dict[str, bool]): Diccionario con las tablas que se cargarán en modo full refresh.
            Ejemplo: {"contents": True, "content_attributes": False}
            Si es None, todas las tablas se cargan incrementalmente.
        tables (List[str]): Lista de tablas a cargar. Si es None, se cargan todas las tablas disponibles.
    """
    # Determinar el destino según el entorno
    if env == 'local':
        destination = 'duckdb'
        dataset_name = 'raw_datavision_local'
        dev_mode = True  # Modo desarrollo para experimentación
    elif env == 'dev':
        destination = 'athena'
        dataset_name = 'raw_datavision_dev'
        dev_mode = False  # Modo producción, sin reset automático
    else:  # prod
        destination = 'athena'
        dataset_name = 'raw_datavision_prod'
        dev_mode = False  # Modo producción, sin reset automático
    
    # Configurar directorio específico para el entorno, ver https://dlthub.com/docs/general-usage/pipeline#pipeline-working-directory
    pipelines_dir = os.path.join(get_dlt_pipelines_dir(), env)
    
    # Configurar el pipeline de datos con directorio específico y modo de desarrollo
    pipeline = dlt.pipeline(
        pipeline_name='datavision',
        destination=destination,
        dataset_name=dataset_name,
        pipelines_dir=pipelines_dir,
        dev_mode=dev_mode
    )
    
    # Crear una conexión a la base de datos origen
    db = sql_database()
    
    # Tablas disponibles por defecto y su configuración
    # True = carga incremental (tiene updated_at)
    # False = full refresh
    # (El diccionario ahora es global: DEFAULT_TABLES)
    
    # Determinar qué tablas procesar
    tables_to_process = tables if tables else list(DEFAULT_TABLES.keys())
    
    # Configurar las tablas a cargar
    tables_to_load = []
    for table in tables_to_process:
        if table not in DEFAULT_TABLES:
            print(f"Advertencia: La tabla {table} no está en la lista de tablas disponibles")
            continue
            
        table_resource = db.with_resources(table)
        
        # Determinar si la tabla debe ser full refresh
        is_full_refresh = (
            full_refresh_tables is not None and 
            full_refresh_tables.get(table, False)
        ) or not DEFAULT_TABLES[table]  # Si no soporta incremental, es full refresh
        
        # Configurar carga incremental usando updated_at solo si la tabla lo soporta
        if not is_full_refresh and DEFAULT_TABLES[table]:
            getattr(table_resource, table).apply_hints(
                table_format="iceberg",
                incremental=dlt.sources.incremental("updated_at")
            )
        
        # Configurar el write_disposition específico para esta tabla
        getattr(table_resource, table).apply_hints(
            write_disposition="replace" if is_full_refresh else "merge"
        )
        
        tables_to_load.append(table_resource)

    # Ejecutar el pipeline
    info = pipeline.run(tables_to_load)

    # Mostrar el resultado de la operación
    print(info)

if __name__ == "__main__":
    # Configurar el parser de argumentos
    parser = argparse.ArgumentParser(description='Script de ingesta de datos para Datavision')
    parser.add_argument('--env', choices=['local', 'dev', 'prod'], default='local',
                      help='Entorno de ejecución (local, dev o prod). Por defecto es local.')
    parser.add_argument('--tables', nargs='+',
                      help='Tablas específicas a cargar. Si no se especifica, se cargan todas las tablas disponibles. Ejemplo: --tables contents content_attributes')
    parser.add_argument('--full-refresh', action='store_true',
                      help='Si se especifica, todas las tablas indicadas se cargarán en modo full refresh')
    args = parser.parse_args()
    
    # Corregido: si se usa --full-refresh sin --tables, aplicar a todas las tablas por defecto
    if args.full_refresh:
        tables_for_refresh = args.tables if args.tables else list(DEFAULT_TABLES.keys())
        full_refresh_tables = {table: True for table in tables_for_refresh}
    else:
        full_refresh_tables = None
    
    # Ejecutar la carga de datos
    carga_datos(args.env, full_refresh_tables, args.tables)