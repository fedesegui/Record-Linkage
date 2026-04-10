import pandas as pd

def criterio(criterio_vinculacion):

    if criterio_vinculacion == "criterio_completo":
        return ["id_pais_documento", "id_tipo_documento", "documento", "fecha_nacimiento", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido"]
    if criterio_vinculacion == "criterio_sin_nombres":
        return ["id_pais_documento", "id_tipo_documento", "documento", "fecha_nacimiento"]
    if criterio_vinculacion == "criterio_sin_documento":
        return ["perna02_r", "primer_nombre", "segundo_nombre", "primer_apellido", "segundo_apellido"]
    if criterio_vinculacion == "criterio_solo_documento":
        return ["id_pais_documento", "id_tipo_documento", "documento"]