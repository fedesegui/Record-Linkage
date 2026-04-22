from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
CENSO_FILE = BASE_DIR / "microdatos_censo_sintetico.csv"


def _read_censo() -> pd.DataFrame:
    return pd.read_csv(CENSO_FILE)


def get_personas_censo(muestra=None):
    personas_censo = _read_censo()
    if muestra is not None:
        personas_censo = personas_censo.head(int(muestra))
    if personas_censo.empty:
        print("Warning: El DataFrame de personas_censo está vacío después de aplicar la muestra.")
    else:
        print(f"personas_censo cargado con {len(personas_censo)} filas.")
    if personas_censo is None:
        print("Warning: personas_censo es None después de cargar los datos.")
    else:
        print("personas_censo no es None después de cargar los datos.")
    return personas_censo


def get_personas_ampliada(variables):
    personas_censo = _read_censo()
    columnas = ["id_censo", *variables]
    return personas_censo[columnas]


def get_personas(variables):
    personas_censo = _read_censo()
    columnas = ["id_censo", *variables]
    return personas_censo[columnas]