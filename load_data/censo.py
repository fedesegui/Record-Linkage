import pandas as pd
from utils.db_connections.registros import make_dw_conection

dw_connection = make_dw_conection("censo_prod")


def get_personas_censo(muestra):
    sql_query = f"""
    select 
        dp.id_censo, 
        per.fuente, 
        dp.tipo_caso, 
        per.perna02_r, 
        per.perna02_r_imp, 
        dp.ci, 
        dp.docextpais, 
        dp.docexttipo, 
        dp.docextnro, 
        dp.primer_nombre, 
        dp.segundo_nombre, 
        dp.primer_apellido, 
        dp.segundo_apellido
    from vinculacion.personas_con_datos_personales dp
    left join censo.id_censo_caso idc on idc.id_censo = dp.id_censo 
    left join censo.personas_15_10 per on (per.caso, per.vivid, per.hogid, per.perid) = (idc.caso, idc.vivid, idc.hogid, idc.perid)
    where per.caso is not null
    LIMIT {muestra};
    """
    personas_censo = pd.read_sql(sql_query, dw_connection)

    return personas_censo

def get_personas_ampliada(variables):
    """
    las variables se pasan como string para meter en la consulta
    """
    variables_query = ', '.join(f"per.{var}" for var in variables)
    
    sql_query = f"""
    select 
        idc.id_censo,
        {variables_query}
    from censo.personas_ampliada per
    left join censo.id_censo_caso idc on (per.caso, per.vivid, per.hogid, per.perid) = (idc.caso, idc.vivid, idc.hogid, idc.perid)
    """
    personas_censo = pd.read_sql(sql_query, dw_connection)

    return personas_censo

def get_personas(variables):
    """
    las variables se pasan como string para meter en la consulta
    """
    variables_query = ', '.join(f"per.{var}" for var in variables)
    
    sql_query = f"""
    SELECT 
    idc.id_censo,
    {variables_query}
    FROM 
        censo.personas_11_11 per
    LEFT JOIN 
        censo.id_censo_caso idc 
    ON 
        TRIM(per.caso) = TRIM(idc.caso)
        AND COALESCE(per.vivid, 99) = COALESCE(idc.vivid, 99)
        AND per.hogid = idc.hogid
        AND per.perid = idc.perid;
    """
    personas_censo = pd.read_sql(sql_query, dw_connection)

    return personas_censo