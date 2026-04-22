from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
RRAA_FILE = BASE_DIR / "data_sintetica" / "microdatos_registro_poblacion_sintetico.csv"


def _read_rraa() -> pd.DataFrame:
    return pd.read_csv(RRAA_FILE)


def get_personas_rraa(muestra=None):
    personas = _read_rraa()
    if muestra is not None:
        personas = personas.head(int(muestra))
    return personas


def update_personas_rraa(lista_id_estadistico):
    personas = _read_rraa()
    return personas[~personas["id_unico"].isin(lista_id_estadistico)]


def get_personas_extranjeras_rraa(muestra=None):
    raise NotImplementedError(
        "En modo sintético solo se usa microdatos_registro_poblacion_sintetico.csv"
    )


def get_id_estado():
    raise NotImplementedError(
        "En modo sintético no se utiliza la tabla de estados de registro de población."
    )


def get_departamento_rraa():
    raise NotImplementedError(
        "En modo sintético no se utiliza la tabla de departamento de RRAA."
    )


def get_documentos_extranjeros():
    raise NotImplementedError(
        "En modo sintético no se procesa el flujo de documentos extranjeros."
    )


def get_personas_extranjeras_censo(muestra, documentos):
    raise NotImplementedError(
        "En modo sintético no se procesa el flujo de extranjeros censo-RRAA."
    )


def get_fecha_nacimiento_sexo_rraa():
    personas = _read_rraa()
    return personas[["id_unico", "fecha_nacimiento", "id_sexo"]]