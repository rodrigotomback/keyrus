from ..data_set.data_set import execute
from ..linked_service.linked_service import get_data_api_multidados

from pandas import json_normalize
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime





def cp_api_prestador_to_db():

    # informações do banco de dados
    user_db = 'adminbi'
    host_db = 'keyrusbidb.postgres.database.azure.com'
    # tratamento de senha para evitar conflito de caracteres na string de conexao
    senha_db = 'Keyrus@2022!'
    senha_db_postgres = quote_plus(senha_db)
    schema = 'dev_stg_multidados_dw'
    table_name = 'stg_prestador'
    ######################################

    data_hora_atual = datetime.now()

    # trunca a tabela antes de inserir os dados
    execute(f''' TRUNCATE TABLE {schema}.{table_name}''')
    print('truncate executado')


    # requisição na api com regra de paginação aplicadas e transformação da requisição em dataframe para inserção no banco de dados
    pagina_atual = 1
    
    while True:

        # informações da api
        token_api = 'aW50ZWdyYWNhb19lcnA6dGVzdGVfZXJw'

        url_api = 'https://timesheet.keyrus.com.br/api'

        subject_api = '/Prestadores/consultar'

        params_api = { 

                    "dh_atualizacao_de": '2013-01-01 00:00:00',
                    "dh_atualizacao_ate": '2023-07-21 00:00:00', 
                    "registros_por_pagina":3000,
                    "pagina_atual": pagina_atual
                }
        
        # função de requisição de dados na api multidados
        res_api_multidados = get_data_api_multidados(url_api, subject_api, params_api, token_api)
        

        # transforma a resposta da api em dataFrame
        df = json_normalize(res_api_multidados)

        # Formata a data e hora atual como string no formato desejado (por exemplo, 'YYYY-MM-DD HH:MM:SS')
        dt_ref = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')
        ts_exec = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')
        
        # Adicionar a coluna de timestamp ao DataFrame
        df['dt_ref'] = dt_ref
        df['ts_exec'] = ts_exec

        
        try:
            
            # Configurar a conexão com o banco de dados (PostgreSQL neste exemplo)
            string_conn = f'postgresql+psycopg2://{user_db}:{senha_db_postgres}@{host_db}/postgres_dev'

            # executa insert dos dados do datafram no banco de dados
            engine = create_engine(string_conn)
            df.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
            

        except Exception as e:
            print(e)
            exit()

        # se nao existir mais dados na requisição sai do while
        if res_api_multidados == []:
            print("não há mais páginas")   
            break

        print("dados inseridos no banco")
        pagina_atual += 1
        print(f'pagina_atual = {pagina_atual}')

    print('EXTRAÇÃO DE DADOS PARA stg_prestador CONCLUÍDA')

