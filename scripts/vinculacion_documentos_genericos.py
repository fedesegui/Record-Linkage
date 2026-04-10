import pandas as pd
import numpy as np




def vincular_documentos_genericos(documentos_genericos, censo, rraa, variables):

    censo_genericos = censo[censo["documento"].isin(documentos_genericos)]

    vinculados = pd.merge(censo_genericos, rraa[["documento"] + variables], on = ["documento"] + variables, how = "left", indicator = True)

    vinculados = vinculados[vinculados["_merge"] == "both"]

    return vinculados