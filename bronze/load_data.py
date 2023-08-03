from ..data_set.data_set import execute

from sqlalchemy import create_engine
from urllib.parse import quote_plus




def load_data_prestador(data):

        # informações do banco de dados
    # informações do banco de dados
    user_db = 'adminbi'
    host_db = 'keyrusbidb.postgres.database.azure.com'

    # tratamento de senha para evitar conflito de caracteres na string de conexao
    senha_db = 'Keyrus@2022!'
    senha_db_postgres = quote_plus(senha_db)
    schema = 'dev_stg_multidados_dw'
    table_name = 'stg_prestador'

    ######################################
    schema = 'dev_multidados_dw'
    table_name = 'raw_prestador'
    ######################################

        # trunca a tabela antes de inserir os dados
    execute(f''' TRUNCATE TABLE {schema}.{table_name}''')
    print('truncate executado')

    try:
        
        # Configurar a conexão com o banco de dados (PostgreSQL neste exemplo)
        string_conn = f'postgresql+psycopg2://{user_db}:{senha_db_postgres}@{host_db}/postgres_dev'

        engine = create_engine(string_conn)
        print('conectado ao banco')

        # executa insert dos dados do dataframe no banco de dados
        data.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
        print('dados inseridos no banco')

    except Exception as e:
        print(e)
        exit()

    print('CARREGAMENTO DE DADOS PARA raw_prestador CONCLUíDO')