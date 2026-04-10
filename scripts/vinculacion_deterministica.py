from utils.vinculacion_deterministica.criterios import criterio
from utils.vinculacion_deterministica.fucion_vinculacion import vincular_datasets

def vincular_datasets_criterio(dataset1, dataset2, order, criterio_vinculacion):
    print("Vinculación segun " + criterio_vinculacion)

    vinculacion = vincular_datasets(dataset1, dataset2, order, criterio(criterio_vinculacion))

    return vinculacion[0], vinculacion[1], vinculacion[2], vinculacion[3]

