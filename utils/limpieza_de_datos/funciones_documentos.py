import numpy as np

def validar_cedula(cedula):
    if cedula is None:
        return None
    # Remover cualquier caracter que no sea un dígito
    cedula = "".join(filter(str.isdigit, str(cedula)))

    if len(cedula) == 7:
        cedula = "0" + cedula

    # Separar los dígitos y convertirlos a enteros
    digitos = [int(d) for d in cedula]

    if len(cedula) == 8:
        # Multiplicar cada dígito por su respectiva constante
        multiplicadores = [2, 9, 8, 7, 6, 3, 4]
        s = sum(d * m for d, m in zip(digitos[:7], multiplicadores))

        # Calcular el dígito verificador
        M = s % 10
        h = (10 - M) % 10

        # Verificar si el dígito verificador es igual al último dígito de la cédula
        if h == digitos[-1]:
            return 1

    return 0


def descartar_documentos(row, documentos):
    row["documento"] = np.nan if row["documento"] in documentos else row["documento"]

    return row

