from utils.db_connections.registros import make_dw_conection

def crear_tabla_en_batches(df, nombre_base, nombre_esquema, nombre_tabla):

    dw_connection = make_dw_conection(nombre_base)

    print(f"Creando tabla {nombre_esquema}.{nombre_tabla} en DW")
    total_filas = len(df)
    tamano_lote = 1000000
    total_lotes = (total_filas // tamano_lote) + 1
    for i in range(total_lotes):
        inicio = i * tamano_lote
        fin = min((i + 1) * tamano_lote, total_filas)
        lote_actual = df.iloc[inicio:fin]
        print(f"Progreso: {i/total_lotes * 100:.2f}%")
        if i == 0:
            lote_actual.to_sql(
                schema=nombre_esquema,
                name=nombre_tabla,
                con=dw_connection,
                if_exists="replace",
                index=False,
            )
        else:
            lote_actual.to_sql(
                schema=nombre_esquema,
                name=nombre_tabla,
                con=dw_connection,
                if_exists="append",
                index=False,
            )
    print(f"La tabla {nombre_esquema}.{nombre_tabla} fue actualizada.")
