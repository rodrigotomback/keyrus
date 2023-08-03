import psycopg2
import requests
import json


# CONEXAO BANCO DE DADOS PARA DESENVOLVIMENTO
def db_conn_posgres_dev():

    conn = psycopg2.connect(
            dbname = "postgres_dev",  #DEV
            user = "adminbi",
            password = "Keyrus@2022!",
            host = "keyrusbidb.postgres.database.azure.com"
            )
    return conn



def get_data_api_multidados(url, subject, params, token):

    headers = {
                'Authorization-Token': token, 
                "Content-Type":"application/json"
               }
    
    res = requests.get(f"{url}{subject}", headers=headers, params=params)
    res = res.json()

    response = res['data']

    return response

