import pandas as pd
import numpy as np
from data_sintetica.utils.limpieza_de_datos.funciones_texto import (
    limpieza_database,
    modificar_nombre_apellido
)
from data_sintetica.utils.limpieza_de_datos.funciones_documentos import validar_cedula
from data_sintetica.load_data.rraa import get_personas_rraa
from data_sintetica.load_data.censo import get_personas_censo
from datetime import datetime 


def get_rraa_clean(muestra):
    rraa = get_personas_rraa(muestra)
    rraa = limpieza_database(rraa)
    rraa = rraa.apply(lambda row: modificar_nombre_apellido(row, "rraa"), axis=1)
    # rraa["id_sexo"] = rraa["id_sexo"].astype("int64")
    condicion = (rraa["id_pais_documento"] == 858) & (rraa["id_tipo_documento"] == 1)
    rraa["documento_valido"] = np.where(
        condicion,
        rraa["documento"].astype(str).apply(validar_cedula),
        0,
    )

    rraa["id_pais_documento"] = rraa["id_pais_documento"].astype(str)
    rraa["id_tipo_documento"] = rraa["id_tipo_documento"].astype(str)

    rraa["fecha_nacimiento"] = pd.to_datetime(rraa["fecha_nacimiento"], errors = "coerce")

    rraa["ano_nacimiento"] = rraa["fecha_nacimiento"].dt.year
    rraa["mes_nacimiento"] = rraa["fecha_nacimiento"].dt.month
    rraa["dia_nacimiento"] = rraa["fecha_nacimiento"].dt.day

    return rraa

def update_rraa_clean(rraa):
    raise NotImplementedError(
        "En modo sintético no se actualiza rraa incrementalmente: usar get_rraa_clean()."
    )

def get_extranjeros_clean(muestra):
    raise NotImplementedError(
        "En modo sintético solo se ejecuta el pipeline con censo + registro de población (rraa)."
    )


def get_censo_clean(muestra):
    print("Comenzo el proceso: " + datetime.now().strftime("%H:%M:%S"))

    censo = get_personas_censo(muestra)
    print("Se cargo la tabla: " + datetime.now().strftime("%H:%M:%S"))

    censo = limpieza_database(censo)
    print("Se limpio la tabla: " + datetime.now().strftime("%H:%M:%S"))

    censo = censo.apply(lambda row: modificar_nombre_apellido(row, "censo"), axis=1)
    print("Se modificaron los nombres y apellidos: " + datetime.now().strftime("%H:%M:%S"))

    # Usa la fecha de nacimiento ya disponible en el dataset sintético si existe.
    if "fecha_nacimiento" in censo.columns:
        censo["fecha_nacimiento"] = pd.to_datetime(censo["fecha_nacimiento"], errors="coerce")
    else:
        # Compatibilidad con la lógica histórica basada en perna02_r/perna02_r_imp.
        censo["fecha_nacimiento"] = pd.to_datetime(
            np.where(
                censo.apply(lambda row: row["perna02_r_imp"] == 1, axis=1),
                pd.NaT,
                censo["perna02_r"],
            ),
            errors="coerce",
        )

    censo["ano_nacimiento"] = censo["fecha_nacimiento"].dt.year
    censo["mes_nacimiento"] = censo["fecha_nacimiento"].dt.month
    censo["dia_nacimiento"] = censo["fecha_nacimiento"].dt.day
    print("Se modificaron las fechas de nacimiento: " + datetime.now().strftime("%H:%M:%S"))

print("ci" + censo["ci"])

    censo["documento"] = np.where(
        censo.apply(lambda row: validar_cedula(row["ci"]) == 1, axis = 1),
        censo["ci"].astype(str).str.lstrip('0'),
        np.where(
            censo.apply(lambda row: row["docextnro"] is not np.nan, axis=1),
            censo["docextnro"],
            censo["ci"]
        )
    )

    censo["id_pais_documento"] = np.where(
        censo.apply(lambda row: row["ci"] == row["documento"] and row["documento"] is not np.nan or row["docextpais"] == 895, axis=1),
        858,
        np.where(
            censo.apply(lambda row: row["docextnro"] == row["documento"] and row["docextnro"] is not np.nan, axis=1),
            censo["docextpais"],
            np.nan
            )
    )

    censo["id_pais_documento"] = censo["id_pais_documento"].astype(pd.StringDtype())

    censo["id_tipo_documento"] = np.where(
        censo.apply(lambda row: row["ci"] == row["documento"] and row["documento"] is not np.nan, axis=1),
        1,
        np.where(
            censo.apply(lambda row: row["docextnro"] == row["documento"] and row["docextnro"] is not np.nan, axis=1),
            censo["docexttipo"],
            np.nan
            )
    )

    censo["id_tipo_documento"] = censo["id_tipo_documento"].astype(pd.StringDtype())

    def condition(row):
        if pd.isna(row["id_pais_documento"]) or pd.isna(row["id_tipo_documento"]):
            return False
        return row["id_pais_documento"] == "858" and row["id_tipo_documento"] == "1"

    censo["documento_valido"] = np.where(
        censo.apply(condition, axis=1),
        censo["documento"].apply(validar_cedula),
        0,
    )
    print("Se modificaron los documentos: " + datetime.now().strftime("%H:%M:%S"))