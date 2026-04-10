import splink

import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
import splink.duckdb.comparison_level_library as cll

from splink.duckdb.comparison_template_library import date_comparison
from splink.duckdb.comparison_template_library import name_comparison, forename_surname_comparison

def get_comparacion_fecha_nacimiento(con_dc = False):
    """
    Comparison: Date of birth
    ├─-- ComparisonLevel: Exact match  
    ├─-- ComparisonLevel: Up to one character difference  
    ├─-- ComparisonLevel: Dates within 1 month of each other  
    ├─-- ComparisonLevel: Dates within 1 year of each other  
    ├─-- ComparisonLevel: Dates within 10 years of each other  
    ├─-- ComparisonLevel: All other  
    """

    comparacion_fecha = date_comparison(
    "fecha_nacimiento",
    levenshtein_thresholds=[],
    damerau_levenshtein_thresholds=[1],
    date_format = "%Y-%m-%d",
    invalid_dates_as_null=True,
    cast_strings_to_date=True,
    datediff_thresholds=[1],
    datediff_metrics=["year"]
    )

    comparacion_string = cl.damerau_levenshtein_at_thresholds("fecha_nacimiento", [1])

    if con_dc:
        return comparacion_fecha
    else:
        return comparacion_string
    

def get_comparacion_ano_nacimiento_imputados():
    comparacion = {
    "output_column_name": "ano_nacimiento_imputados",
    "comparison_levels": [cll.null_level("ano_nacimiento_imputados"),
                          cll.levenshtein_level("ano_nacimiento_imputados", distance_threshold=1),
                          cll.else_level()]
    }

    return comparacion

def get_comparacion_nombres_combinados(var1, var2, var_comb):
    """
    Comparison: Name
    ├─-- ComparisonLevel: Exact match
    ├─-- ComparisonLevel: Up to one character difference
    ├─-- ComparisonLevel: First Names with Jaro-Winkler similarity of 0.9 or greater 
    ├─-- ComparisonLevel: First Names with Jaro-Winkler similarity of 0.8 or greater
    ├─-- ComparisonLevel: All other
    """

    comparacion = forename_surname_comparison(
    var1,
    var2,
    include_exact_match_level=True,
    term_frequency_adjustments=False,
    #tf_adjustment_col_forename_and_surname=var_comb,
    levenshtein_thresholds=[1],
    damerau_levenshtein_thresholds=[],
    jaro_winkler_thresholds=[0.88],
    jaccard_thresholds=[],
    )

    return comparacion

def get_comparacion_nombres_especial(nombre_comparacion, var1, var2, var_comb):

    comparacion = {
        "output_column_name": f"{nombre_comparacion}",
        "comparison_description": "no description",
        "comparison_levels": [
            {
                "sql_condition": f"({var1}_l IS NULL AND {var2}_l IS NULL) OR ({var1}_r IS NULL AND {var2}_r IS NULL)",
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": f"jaro_winkler_sim({var1}_l, {var1}_r) > 0.88 AND jaro_winkler_sim({var2}_l, {var2}_r) > 0.88",
                "label_for_charts": "Exact match both",
                "tf_adjustment_column": f"{var_comb}",
                "tf_adjustment_weight": 1.0,
                "tf_minimum_u_value": 0.001,
            },
            {
                "sql_condition": f"jaro_winkler_sim({var1}_l, {var2}_r) > 0.88 AND jaro_winkler_sim({var2}_l, {var1}_r) > 0.88",
                "label_for_charts": "Exact match both swapped"
            },
            {
                "sql_condition": f"jaro_winkler_sim({var1}_l, {var2}_r) > 0.88",
                "label_for_charts": "rraa2 - censo1"
            },
            {
                "sql_condition": f"jaro_winkler_sim({var1}_l, {var1}_r) > 0.88",
                "label_for_charts": "Exact match first",
                "tf_adjustment_column": f"{var1}",
                "tf_adjustment_weight": 1.0,
                "tf_minimum_u_value": 0.001,
            },
            {
                "sql_condition": f"jaro_winkler_sim({var2}_l, {var2}_r) > 0.88",
                "label_for_charts": "Exact match second",
                "tf_adjustment_column": f"{var1}",
                "tf_adjustment_weight": 1.0,
                "tf_minimum_u_value": 0.001,
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
    }

    return comparacion




def get_comparacion_nombre(var, usar_tfa):

    comparacion = name_comparison(
    var,
    #phonetic_col_name="nombre_pronunciacion", (habría que calcularlo)
    term_frequency_adjustments=usar_tfa,
    levenshtein_thresholds=[],
    damerau_levenshtein_thresholds=[],
    jaro_winkler_thresholds=[0.88],
    jaccard_thresholds=[],
    )

    return comparacion
