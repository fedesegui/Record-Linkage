from scripts.limpieza_de_datos import get_censo_clean, get_dnic_clean
from scripts.vinculacion_deterministica import vincular_datasets_criterio


CRITERIOS_POR_DEFECTO = [
    "criterio_solo_documento",
    "criterio_sin_nombres",
    "criterio_completo",
]


def ejecutar_record_linkage_sintetico(muestra=None, criterios=None):
    """
    Ejecuta el pipeline de record linkage usando exclusivamente:
    - data_sintetica/microdatos_censo_sintetico.csv
    - data_sintetica/microdatos_registro_poblacion_sintetico.csv
    """
    criterios = criterios or CRITERIOS_POR_DEFECTO

    censo = get_censo_clean(muestra)
    rraa = get_dnic_clean(muestra)

    resultados = {}
    for criterio_nombre in criterios:
        vinculados, censo_no_vinculados, rraa_no_vinculados, info = vincular_datasets_criterio(
            censo,
            rraa,
            ["censo", "rraa"],
            criterio_nombre,
        )
        resultados[criterio_nombre] = {
            "vinculados": vinculados,
            "censo_no_vinculados": censo_no_vinculados,
            "rraa_no_vinculados": rraa_no_vinculados,
            "info": info,
        }

    return resultados