from ..linked_service.linked_service import db_conn_posgres_dev, get_data_api_multidados


# FUNCAO PARA DAR SELECT NO BANCO DE DADOS
def select(query):
    conn = db_conn_posgres_dev()
    cur = conn.cursor()
    cur.execute(query)
    columns = [column[0] for column in cur.description]
    data = []
    for row in cur.fetchall():
        data.append(dict(zip(columns, row)))

    cur.close()
    conn.close()

    return data

# FUNCAO PARA EXECUTAR UPDATE, DELETE E INSERT NO BANCO DE DADOS
def execute(query):
    conn = db_conn_posgres_dev()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
