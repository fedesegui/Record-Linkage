def get_comparacion_fecha_nacimiento(con_dc=False):
    """
    Define una comparación de fecha de nacimiento para el pipeline probabilístico.
    """
    comparacion = {
        "tipo": "fecha_nacimiento",
        "columna": "fecha_nacimiento",
        "permitir_diferencia_dias": 0,
        "permitir_diferencia_anos": 1,
    }

    if con_dc:
        comparacion["usar_date_comparison"] = True
    return comparacion


def get_comparacion_ano_nacimiento_imputados():
    return {
        "output_column_name": "ano_nacimiento_imputados",
        "comparison_levels": ["null", "levenshtein<=1", "else"],
    }


def get_comparacion_nombres_combinados(var1, var2, var_comb):
    return {
        "tipo": "nombres_combinados",
        "forename": var1,
        "surname": var2,
        "combined": var_comb,
        "jaro_winkler_threshold": 0.88,
        "levenshtein_threshold": 1,
    }


def get_comparacion_nombres_especial(nombre_comparacion, var1, var2, var_comb):
    return {
        "output_column_name": nombre_comparacion,
        "tipo": "nombres_especial",
        "var1": var1,
        "var2": var2,
        "var_comb": var_comb,
        "jaro_winkler_threshold": 0.88,
    }


def get_comparacion_nombre(var, usar_tfa):
    return {
        "tipo": "nombre",
        "columna": var,
        "usar_tfa": usar_tfa,
        "jaro_winkler_threshold": 0.88,
    }