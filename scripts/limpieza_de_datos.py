import pandas as pd
import numpy as np
from utils.limpieza_de_datos.funciones_texto import (
    limpieza_database,
    modificar_nombre_apellido
)
from utils.limpieza_de_datos.funciones_documentos import validar_cedula
from load_data.rraa import get_personas_dnic, update_personas_dnic
from load_data.rraa import get_personas_extranjeras
from load_data.censo import get_personas_censo
from datetime import datetime 


def get_dnic_clean(muestra):
    dnic = get_personas_dnic(muestra)
    dnic = limpieza_database(dnic)
    dnic = dnic.apply(lambda row: modificar_nombre_apellido(row, "rraa"), axis=1)
    # dnic["id_sexo"] = dnic["id_sexo"].astype("int64")
    condicion = (dnic["id_pais_documento"] == 858) & (dnic["id_tipo_documento"] == 1)
    dnic["documento_valido"] = np.where(
        condicion,
        dnic["documento"].astype(str).apply(validar_cedula),
        0,
    )

    dnic["id_pais_documento"] = dnic["id_pais_documento"].astype(str)
    dnic["id_tipo_documento"] = dnic["id_tipo_documento"].astype(str)

    dnic["fecha_nacimiento"] = pd.to_datetime(dnic["fecha_nacimiento"], errors = "coerce")

    dnic["ano_nacimiento"] = dnic["fecha_nacimiento"].dt.year
    dnic["mes_nacimiento"] = dnic["fecha_nacimiento"].dt.month
    dnic["dia_nacimiento"] = dnic["fecha_nacimiento"].dt.day

    return dnic

def update_dnic_clean(rraa):

    lista_id_estadisticos = rraa["id_unico"]

    dnic = update_personas_dnic(lista_id_estadisticos)
    dnic = limpieza_database(dnic)
    dnic = dnic.apply(lambda row: modificar_nombre_apellido(row, "rraa"), axis=1)
    # dnic["id_sexo"] = dnic["id_sexo"].astype("int64")
    condicion = (dnic["id_pais_documento"] == 858) & (dnic["id_tipo_documento"] == 1)
    dnic["documento_valido"] = np.where(
        condicion,
        dnic["documento"].astype(str).apply(validar_cedula),
        0,
    )

    dnic["id_pais_documento"] = dnic["id_pais_documento"].astype(str)
    dnic["id_tipo_documento"] = dnic["id_tipo_documento"].astype(str)

    dnic["fecha_nacimiento"] = pd.to_datetime(dnic["fecha_nacimiento"], errors = "coerce")

    dnic["ano_nacimiento"] = dnic["fecha_nacimiento"].dt.year
    dnic["mes_nacimiento"] = dnic["fecha_nacimiento"].dt.month
    dnic["dia_nacimiento"] = dnic["fecha_nacimiento"].dt.day

    dnic_updated = pd.concat([rraa, dnic], ignore_index=True)

    return dnic_updated

def get_extranjeros_clean(muestra):
    hub = get_personas_extranjeras(muestra)
    hub = limpieza_database(hub)
    hub = hub.apply(lambda row: modificar_nombre_apellido(row, "rraa"), axis=1)
    # dnic["id_sexo"] = dnic["id_sexo"].astype("int64")
    condicion = (hub["id_pais_documento"] == 858) & (hub["id_tipo_documento"] == 1)
    hub["documento_valido"] = np.where(
        condicion,
        hub["documento"].astype(str).apply(validar_cedula),
        0,
    )

    hub["id_pais_documento"] = hub["id_pais_documento"].astype(str)
    hub["id_tipo_documento"] = hub["id_tipo_documento"].astype(str)

    hub["fecha_nacimiento"] = pd.to_datetime(hub["fecha_nacimiento"], errors = "coerce")

    hub["ano_nacimiento"] = hub["fecha_nacimiento"].dt.year
    hub["mes_nacimiento"] = hub["fecha_nacimiento"].dt.month
    hub["dia_nacimiento"] = hub["fecha_nacimiento"].dt.day

    return hub


def get_censo_clean(muestra):
    print("Comenzo el proceso: " + datetime.now().strftime("%H:%M:%S"))

    censo = get_personas_censo(muestra)
    print("Se cargo la tabla: " + datetime.now().strftime("%H:%M:%S"))

    censo = limpieza_database(censo)
    print("Se limpio la tabla: " + datetime.now().strftime("%H:%M:%S"))

    censo = censo.apply(lambda row: modificar_nombre_apellido(row, "censo"), axis=1)
    print("Se modificaron los nombres y apellidos: " + datetime.now().strftime("%H:%M:%S"))

    #censo["fecha_nacimiento"] = pd.to_datetime(censo["perna02_r"], format="%Y%m%d", errors="coerce")
    censo["fecha_nacimiento"] = pd.to_datetime(np.where(
        censo.apply(lambda row: row["perna02_r_imp"] == 1, axis = 1), 
        pd.NaT, 
        censo["perna02_r"]), 
        errors="coerce")

    censo["ano_nacimiento"] = censo["fecha_nacimiento"].dt.year
    censo["mes_nacimiento"] = censo["fecha_nacimiento"].dt.month
    censo["dia_nacimiento"] = censo["fecha_nacimiento"].dt.day
    print("Se modificaron las fechas de nacimiento: " + datetime.now().strftime("%H:%M:%S"))


    censo["documento"] = np.where(
        censo.apply(lambda row: validar_cedula(row["ci"]) == 1, axis = 1),
        censo["ci"].str.lstrip('0'),
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

    return censo