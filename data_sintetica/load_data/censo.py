from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
CENSO_FILE = BASE_DIR / "data_sintetica" / "microdatos_censo_sintetico.csv"


def _read_censo() -> pd.DataFrame:
    return pd.read_csv(CENSO_FILE)


def get_personas_censo(muestra=None):
    personas_censo = _read_censo()
    if muestra is not None:
        personas_censo = personas_censo.head(int(muestra))
    return personas_censo


def get_personas_ampliada(variables):
    personas_censo = _read_censo()
    columnas = ["id_censo", *variables]
    return personas_censo[columnas]


def get_personas(variables):
    personas_censo = _read_censo()
    columnas = ["id_censo", *variables]
    return personas_censo[columnas]