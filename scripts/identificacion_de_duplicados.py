import pandas as pd

def detect_duplicates(df):
    # Detectar duplicados basados en ciertos criterios
    duplicates = df[df.duplicated(subset=['nombre', 'apellido', 'fecha_nacimiento'], keep=False)]
    return duplicates