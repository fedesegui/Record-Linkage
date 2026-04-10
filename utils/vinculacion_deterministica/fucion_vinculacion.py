import pandas as pd

def vincular_datasets(dataset1, dataset2, orden, criterio_vinculacion):

    dataset1 = dataset1.drop_duplicates(subset = criterio_vinculacion)
    dataset2 = dataset2.drop_duplicates(subset = criterio_vinculacion)

    result = pd.merge(dataset1, dataset2, how='outer', on = criterio_vinculacion, suffixes = ("_" + orden[0], "_" + orden[1]))

    vinculados = result[pd.notnull(result["id_censo"]) & pd.notnull(result["id_unico"])]
    dataset1_no_vinculados = result[pd.notnull(result["id_censo"]) & pd.isnull(result["id_unico"])]
    dataset2_no_vinculados = result[pd.isnull(result["id_censo"]) & pd.notnull(result["id_unico"])]
    info_vinculacion = {"valores distintos dataset 1: ": len(dataset1),
                        "valores distintos dataset 2: ": len(dataset2)}

    return [
        vinculados,
        dataset1_no_vinculados,
        dataset2_no_vinculados,
        info_vinculacion
    ]
