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
    censo = get_personas_censo(muestra)
    censo = limpieza_database(censo)
    censo = censo.apply(lambda row: modificar_nombre_apellido(row, "censo"), axis=1)
    # censo["id_sexo"] = censo["id_sexo"].astype("int64")
    condicion = (censo["id_pais_documento"] == 858) & (censo["id_tipo_documento"] == 1)
    censo["documento_valido"] = np.where(
        condicion,
        censo["documento"].astype(str).apply(validar_cedula),
        0,
    )

    censo["id_pais_documento"] = censo["id_pais_documento"].astype(str)
    censo["id_tipo_documento"] = censo["id_tipo_documento"].astype(str)

    censo["fecha_nacimiento"] = pd.to_datetime(censo["fecha_nacimiento"], errors = "coerce")

    censo["ano_nacimiento"] = censo["fecha_nacimiento"].dt.year
    censo["mes_nacimiento"] = censo["fecha_nacimiento"].dt.month
    censo["dia_nacimiento"] = censo["fecha_nacimiento"].dt.day

    return censo
