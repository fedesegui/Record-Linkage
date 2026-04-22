import os


def guardar_como_parquet(df, ruta_archivo):
    """
    Guarda un DataFrame como archivo Parquet en la ruta especificada.

    Args:
    - df (pandas.DataFrame): DataFrame que se desea guardar.
    - ruta_archivo (str): Ruta completa del archivo Parquet que se va a guardar.
    """
    # Verificar si la carpeta padre de la ruta_archivo existe, si no existe la crea
    directorio = os.path.dirname(ruta_archivo)
    if not os.path.exists(directorio):
        os.makedirs(directorio)
        print(f"Se creó el directorio: {directorio}")
    
    # Guardar el DataFrame como archivo Parquet
    df.to_parquet(ruta_archivo)
    print(f"Se guardó el DataFrame como Parquet en: {ruta_archivo}")
