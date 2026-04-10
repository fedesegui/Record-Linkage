import pandas as pd
from utils.db_connections.registros import make_dw_conection

dw_connection = make_dw_conection("registros_prod")


def get_personas_dnic(muestra):
    sql_query = f"""
    select 
        hpd.id_estadistico_persona as id_unico,
        dp.numeric as id_pais_documento, 
        hpd.id_tipo_documento, 
        hpd.documento,
        concat(spdp.primer_nombre, ' ', spdp.segundo_nombre) AS nombres,
        concat(spdp.primer_apellido, ' ', spdp.segundo_apellido) AS apellidos,
        drp.fecha_nacimiento,
        drp.id_sexo
    from poblacion.hub_personas_dnic hpd 
    left join poblacion.sat_personas_datos_personales spdp using (id_estadistico_persona)
    left join poblacion.dim_rb_personas drp using (id_estadistico_persona)
    left join poblacion.dim_paises dp on hpd.id_pais_documento = dp.id_pais
    where hpd.id_estado_dato in (2,4)
    LIMIT {muestra};
    """
    personas_dnic = pd.read_sql(sql_query, dw_connection)

    return personas_dnic

def update_personas_dnic(lista_id_estadistico):

    sql_query = f"""
    select 
        hpd.id_estadistico_persona as id_unico,
        dp.numeric as id_pais_documento, 
        hpd.id_tipo_documento, 
        hpd.documento,
        concat(spdp.primer_nombre, ' ', spdp.segundo_nombre) AS nombres,
        concat(spdp.primer_apellido, ' ', spdp.segundo_apellido) AS apellidos,
        drp.fecha_nacimiento,
        drp.id_sexo
    from poblacion.hub_personas_dnic hpd 
    left join poblacion.sat_personas_datos_personales spdp using (id_estadistico_persona)
    left join poblacion.dim_rb_personas drp using (id_estadistico_persona)
    left join poblacion.dim_paises dp on hpd.id_pais_documento = dp.id_pais
    where hpd.id_estado_dato in (2,4);
    """
    personas_dnic = pd.read_sql(sql_query, dw_connection)

    personas_dnic_updated = personas_dnic[~personas_dnic["id_unico"].isin(lista_id_estadistico)]

    return personas_dnic_updated

# registro_poblacion

def get_personas_extranjeras(muestra):
    sql_query = f"""
    select 
        hpd.id_estadistico_persona,
        dp.numeric as id_pais_documento, 
        hpd.id_tipo_documento, 
        hpd.documento,
        concat(spdp.primer_nombre, ' ', spdp.segundo_nombre) AS nombres,
        concat(spdp.primer_apellido, ' ', spdp.segundo_apellido) AS apellidos,
        drp.fecha_nacimiento,
        drp.id_sexo
    from poblacion.hub_personas hpd 
    left join poblacion.sat_personas_datos_personales spdp using (id_estadistico_persona)
    left join poblacion.dim_rb_personas drp using (id_estadistico_persona)
    left join poblacion.dim_paises dp on hpd.id_pais_documento = dp.id_pais
    where hpd.id_pais_documento != 1
    LIMIT {muestra};
    """
    personas = pd.read_sql(sql_query, dw_connection)

    return personas

def get_id_estado():
    sql_query = f"""
    select 
        fact.id_estadistico_persona,
        fact.id_estado
    from registro_poblacion.fact_poblacion_lucas fact
    """
    id_estado = pd.read_sql(sql_query, dw_connection)

    return id_estado


def get_departamento_rraa():
    sql_query = f"""
    select hpd.id_estadistico_persona, dug.id_departamento
        from poblacion.hub_personas_dnic hpd
        left join registro_poblacion.fact_residentes fr using(id_estadistico_persona)
        left join ubicacion.dim_ubic_geo dug on CAST(CAST(fr.id_ubic_geo AS FLOAT) AS INTEGER) = dug.id_ubic_geo 
    """
    departamento = pd.read_sql(sql_query, dw_connection)

    return departamento

def get_documentos_extranjeros():
    sql_query = f"""
    SELECT hp.id_estadistico_persona, hp.id_pais_documento, dp.numeric, hp.id_tipo_documento, hp.documento
        FROM poblacion.hub_personas hp 
        LEFT JOIN poblacion.dim_paises dp on hp.id_pais_documento = dp.id_pais
        WHERE hp.id_estadistico_persona IN (
            SELECT id_estadistico_persona
            FROM poblacion.hub_personas hp
            WHERE id_pais_documento = 1
        )
        AND hp.id_estadistico_persona IN (
            SELECT id_estadistico_persona
            FROM poblacion.hub_personas hp
            WHERE id_pais_documento != 1
        )
        AND hp.id_pais_documento != 1
        """
    doc_extranjeros = pd.read_sql(sql_query, dw_connection)

    return doc_extranjeros


def get_personas_extranjeras_censo(muestra, documentos):

    docs_str = ', '.join(f"'{doc}'" for doc in documentos)

    sql_query = f"""
    select 
        hpd.id_estadistico_persona,
        dp.numeric as id_pais_documento, 
        hpd.id_tipo_documento, 
        hpd.documento,
        concat(spdp.primer_nombre, ' ', spdp.segundo_nombre) AS nombres,
        concat(spdp.primer_apellido, ' ', spdp.segundo_apellido) AS apellidos,
        drp.fecha_nacimiento
        --,drp.id_sexo
    from poblacion.hub_personas hpd 
    left join poblacion.sat_personas_datos_personales spdp using (id_estadistico_persona)
    left join poblacion.dim_rb_personas drp using (id_estadistico_persona)
    left join poblacion.dim_paises dp on hpd.id_pais_documento = dp.id_pais
    where doc in ({docs_str})
    LIMIT {muestra};
    """
    personas = pd.read_sql(sql_query, dw_connection)

    return personas


def get_fecha_nacimiento_sexo_rraa():
    sql_query = f"""
    WITH ranked_birthdates AS (
            SELECT 
                id_estadistico_persona, 
                fecha_nacimiento,
                COUNT(*) AS count_repetitions,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estadistico_persona 
                    ORDER BY COUNT(*) DESC, RANDOM()
                ) AS rn
            FROM poblacion.hub_nacimientos
            GROUP BY id_estadistico_persona, fecha_nacimiento   
        ), nacimientos as (
        SELECT 
            id_estadistico_persona, 
            fecha_nacimiento
        FROM ranked_birthdates
        WHERE rn = 1
        ), ranked_sexo AS (
            SELECT 
                id_estadistico_persona, 
                id_sexo,
                COUNT(*) AS count_repetitions,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estadistico_persona 
                    ORDER BY COUNT(*) DESC, RANDOM()
                ) AS rn
            FROM poblacion.fact_sexo
            GROUP BY id_estadistico_persona, id_sexo   
        ), sexo as (
        SELECT 
            id_estadistico_persona, 
            id_sexo
        FROM ranked_sexo
        WHERE rn = 1
        )
        SELECT 
            dnic.id_estadistico_persona, 
            hn.fecha_nacimiento,
            hs.id_sexo
        FROM poblacion.hub_personas_dnic dnic 
        LEFT JOIN poblacion.dim_rb_personas persona ON dnic.id_estadistico_persona = persona.id_estadistico_persona 
        LEFT JOIN nacimientos hn ON dnic.id_estadistico_persona = hn.id_estadistico_persona 
        LEFT JOIN sexo hs ON dnic.id_estadistico_persona = hs.id_estadistico_persona 
        WHERE   
            (dnic.id_estado_dato IN (2, 4) OR dnic.id_tipo_documento = 9)
        GROUP BY dnic.id_estadistico_persona, hn.fecha_nacimiento, persona.fecha_defuncion, hs.id_sexo;
    """

    fecha_nacimiento = pd.read_sql(sql_query, dw_connection)

    return fecha_nacimiento