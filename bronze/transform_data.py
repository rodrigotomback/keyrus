from ..data_set.data_set import select
from ..controllers.cast_data import DataFrameCaster

import pandas as pd


def transform_prestador():
    src_stg_prestador = select('''SELECT * FROM dev_stg_multidados_dw.stg_prestador''')

    # Joga o resultado da query em um dataframe
    df_stg_prestador = pd.DataFrame(src_stg_prestador)
    df_stg_prestador = DataFrameCaster(df_stg_prestador)

    # CONVERSAO DE TIPO DE COLUNAS

    list_col_int = [
        'idvendedor',
        'idgruposcomiss',
        'idcategoria',
        'meta_ligacoes_diarias',
        'idpagtos',
        'idbancos',
        'idcliente',
        'dia_pagto',
        'idmod_contratacao',
        'idplniveisligacoes',
        'idplniveis_1',
        'idplniveis_2',
        'idplniveis_3',
        'meta_hr_dia_billable',
        'meta_hr_mes_billable',
        'meta_hr_dia_nonbillable',
        'meta_hr_mes_nonbillable',
        'carga_max_dia_minutos',
        'banco',
        'carga_max_mes_minutos',
        'carga_min_dia_minutos',
        'carga_min_mes_minutos',
        'idgrupoempresa',
        'idempresa',
        'idempresa_vinculacao',
        'idequipe',
        'idctabanc_interna',
        'idvend_faixas',
        'dia_pagto_jur',
        'dia_pagto_jur2',
        'dia_pagto_jur3',
        'dia_pagto_jur4',
        'idbancos_jur',
        'idtptit_pg_vend_web',
        'numero_endereco'
    ]

    df_stg_prestador.cast_to_int(list_col_int)
    # cast_to_int(df_stg_prestador, list_col_int)

    list_col_date = ['dt_aniversario','rh_dt_admissao','rh_dt_desligamento']
    df_stg_prestador.cast_to_date(list_col_date)
    # cast_to_date(df_stg_prestador, list_col_date)

    list_col_float = ['meta_vendas', 'rateio', 'salario', 'encargo_social']
    df_stg_prestador.cast_to_float(list_col_float)
    # cast_to_float(df_stg_prestador, list_col_float)

    list_col_timestamp = ['ultimo_acesso', 'dh_atualizacao', 'dt_ref', 'ts_exec']
    df_stg_prestador.cast_to_timestamp(list_col_timestamp)
    # cast_to_timestamp(df_stg_prestador, list_col_timestamp)

    # transforma todo dado convertido em dataframe
    df = df_stg_prestador.df

    print('TRANFORMAÇÃO DE DADOS CONCLUIDO')

    return df
