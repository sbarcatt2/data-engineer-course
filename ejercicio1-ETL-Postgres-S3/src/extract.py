import pandas as pd

def extraer_datos(conn, table, columns='*'):
    """
    Extrae datos de una tabla de PostgreSQL
    
    Args:
        conn: Conexión a la base de datos
        table (str): Nombre de la tabla de la que extraer datos
        columns (str or list, optional): Columnas a seleccionar. Puede ser:
            - '*' para todas las columnas (por defecto)
            - Un string con columnas separadas por comas: 'col1, col2'
            - Una lista de strings: ['col1', 'col2']
    """
    try:
        # Convertir lista a string de columnas separadas por comas si es necesario
        if isinstance(columns, (list, tuple)):
            columns = ', '.join(columns)
            
        query = f"SELECT {columns} FROM {table}"
        df = pd.read_sql_query(query, conn)
        print(f"\nDatos extraídos: {len(df)} registros")
        return df
    except Exception as e:
        print(f"Error al extraer datos: {e}")
        return None