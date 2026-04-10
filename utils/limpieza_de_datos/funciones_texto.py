import numpy as np
import re
import pandas as pd
from unidecode import unidecode


def limpiar_texto(texto):
    texto = str(texto).lower()
    texto = re.sub(r"[\‘\“.,()*+&\/\\\!\%:;?]", "", texto)
    texto = re.sub(r"\s+", " ", texto)
    texto = unidecode(texto)
    texto = re.sub(r"[^a-zA-Z0-9\s]", "", texto)
    texto = texto.strip()
    return texto


def limpieza_database(df):
    print("limpieza_database")
    # Aplica la limpieza de datos a cada columna de tipo string (usa la funcion limpiar_texto)
    for col in df.select_dtypes(include=["object"]):
        df[col] = df[col].map(limpiar_texto)

    # Reemplazar cadenas vacías y "None" con NaN
    df = df.replace({"": np.nan, "none": np.nan})
    return df


def limpieza_campo(campo):
    if pd.isnull(campo):
        return campo
    campo = str(campo)  # Ensure campo is a string
    campo = campo.lower()  # Convert to lowercase
    campo = re.sub(
        r"[\‘\“.,()*+&\/\-\\\+\!\%:;?]", "", campo
    )  # Replace special characters
    campo = re.sub(r"\s+", " ", campo)  # Remove extra whitespaces
    campo = unidecode(campo)  # Convert Unicode characters to ASCII
    campo = campo.replace("'", "")
    campo = campo.replace('"', "")
    campo = campo.strip()  # Remove leading and trailing whitespaces
    return campo


def modificar_nombre_apellido(row, base):
     #if base == "rraa":
     #   row["nombres"] = f"{row['primer_nombre']} {row['segundo_nombre']}"
     #   row["apellidos"] = f"{row['primer_apellido']} {row['segundo_apellido']}"

     if base == "rraa":
         row["fuente"] = "rraa"

     if base == "censo":
        if row["fuente"] == "sen" and not (pd.isnull(row["primer_apellido"])) and not (pd.isnull(row["segundo_apellido"])):
            row["nombres"] = concatenar_palabras(row, "primer_nombre", "segundo_nombre")
            row["apellidos"] = concatenar_palabras(row, "primer_apellido", "segundo_apellido")
        elif (row["fuente"] != "survey") and (row["fuente"] != "sen"):
            row["nombres"] = row["primer_nombre"]
            row["apellidos"] = row["primer_apellido"]
        else:
            row["nombres"] = concatenar_palabras(row, "primer_nombre", "segundo_nombre")
            row["apellidos"] = np.nan

    
     if row["fuente"] == "survey" or (row["fuente"] == "sen" and pd.isnull(row["apellidos"])):
        nombre_split = row["nombres"].split() if pd.notnull(row["nombres"]) else []
        nombre_concatenado = estandarizar_nombre_apellido(nombre_split)
        # si el campo nombre trae 3 palabras lo asigno como 2 apellidos
        if len(nombre_concatenado) == 1:
            row["primer_nombre"] = nombre_concatenado[0]
            row["segundo_nombre"] = ""
            row["primer_apellido"] = ""
            row["segundo_apellido"] = ""
        elif len(nombre_concatenado) == 2:
            row["primer_nombre"] = nombre_concatenado[0]
            row["segundo_nombre"] = ""
            row["primer_apellido"] = nombre_concatenado[1]
            row["segundo_apellido"] = ""
        elif len(nombre_concatenado) == 3:
            row["primer_nombre"] = nombre_concatenado[0]
            row["segundo_nombre"] = ""
            row["primer_apellido"] = nombre_concatenado[1]
            row["segundo_apellido"] = nombre_concatenado[2]
        elif len(nombre_concatenado) > 3:
            row["primer_nombre"] = nombre_concatenado[0]
            row["segundo_nombre"] = nombre_concatenado[1]
            row["primer_apellido"] = nombre_concatenado[2]
            row["segundo_apellido"] = (
                " ".join(nombre_concatenado[3:])
            )


     else:
        nombre_split = row["nombres"].split() if pd.notnull(row["nombres"]) else []
        nombre_concatenado = estandarizar_nombre_apellido(nombre_split)
        row["primer_nombre"] = nombre_concatenado[0] if nombre_concatenado else ""
        row["segundo_nombre"] = (
            " ".join(nombre_concatenado[1:]) if len(nombre_concatenado) > 1 else ""
        )

        apellido_split = row["apellidos"].split() if pd.notnull(row["apellidos"]) else []
        apellido_concatenado = estandarizar_nombre_apellido(apellido_split)
        row["primer_apellido"] = apellido_concatenado[0] if apellido_concatenado else ""
        row["segundo_apellido"] = (
            " ".join(apellido_concatenado[1:]) if len(apellido_concatenado) > 1 else ""
        )

     del row["nombres"]
     del row["apellidos"]

     return row



def estandarizar_nombre_apellido(nombres):
    palabras_a_eliminar = {
        "de",
        "la",
        "da",
        "do",
        "di",
        "lo",
        "san",
        "del",
        "dos",
        "los",
        "las",
        "delos",
        "dela",
        "delas",
    }

    palabras_unidas_a_eliminar = {
        "delos",
        "dela",
        "delas",
    }

    resultado = []
    i = 0

    while i < len(nombres):
        if nombres[i] in palabras_a_eliminar and i + 1 < len(nombres):
            if nombres[i] + nombres[i + 1] in palabras_unidas_a_eliminar and i + 2 < len(nombres):
                resultado.append(nombres[i] + nombres[i + 1] + nombres[i + 2])
                i += 3
            else:
                resultado.append(nombres[i] + nombres[i + 1])
                i += 2
        else:
            resultado.append(nombres[i])
            i += 1

    return resultado

def concatenar_palabras(row, var1, var2):
    parts = [row[var1], row[var2]]
    parts = [str(part) for part in parts if pd.notna(part)]
    return ' '.join(parts)

