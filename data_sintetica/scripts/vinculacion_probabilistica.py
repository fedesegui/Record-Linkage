from difflib import SequenceMatcher

import numpy as np
import pandas as pd

from scripts.limpieza_de_datos import get_censo_clean, get_rraa_clean


def _sim_texto(a, b):
    if pd.isna(a) or pd.isna(b):
        return 0.0
    return SequenceMatcher(None, str(a), str(b)).ratio()


def _normalizar_prob_input(df):
    columnas_requeridas = [
        "id_pais_documento",
        "id_tipo_documento",
        "documento",
        "fecha_nacimiento",
        "primer_nombre",
        "primer_apellido",
    ]
    faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas para vinculación probabilística: {faltantes}")

    out = df.copy()
    out["id_pais_documento"] = out["id_pais_documento"].astype(str)
    out["id_tipo_documento"] = out["id_tipo_documento"].astype(str)
    out["documento"] = out["documento"].astype(str)
    out["fecha_nacimiento"] = pd.to_datetime(out["fecha_nacimiento"], errors="coerce")
    return out


def probabilistic_linkage(df1, df2, threshold=0.85):
    """
    Vinculación probabilística liviana para datasets ya limpiados.
    Se generan candidatos por documento y se calcula un score compuesto.
    """
    left = _normalizar_prob_input(df1)
    right = _normalizar_prob_input(df2)

    candidatos = pd.merge(
        left,
        right,
        on=["id_pais_documento", "id_tipo_documento", "documento"],
        how="inner",
        suffixes=("_censo", "_rraa"),
    )

    if candidatos.empty:
        candidatos["match_probability"] = pd.Series(dtype=float)
        return candidatos

    candidatos["sim_nombre"] = candidatos.apply(
        lambda r: _sim_texto(r["primer_nombre_censo"], r["primer_nombre_rraa"]), axis=1
    )
    candidatos["sim_apellido"] = candidatos.apply(
        lambda r: _sim_texto(r["primer_apellido_censo"], r["primer_apellido_rraa"]), axis=1
    )
    candidatos["sim_fecha"] = np.where(
        candidatos["fecha_nacimiento_censo"] == candidatos["fecha_nacimiento_rraa"],
        1.0,
        0.0,
    )

    candidatos["match_probability"] = (
        0.45 * candidatos["sim_nombre"]
        + 0.35 * candidatos["sim_apellido"]
        + 0.20 * candidatos["sim_fecha"]
    )

    return candidatos[candidatos["match_probability"] >= threshold].copy()


def probabilistic_linkage_sintetico(muestra=None, threshold=0.85):
    """
    Ejecuta vinculación probabilística usando exclusivamente los dos CSV sintéticos.
    """
    censo = get_censo_clean(muestra)
    rraa = get_rraa_clean(muestra)
    return probabilistic_linkage(censo, rraa, threshold=threshold)