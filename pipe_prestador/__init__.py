import datetime
import logging

import azure.functions as func

from ..bronze.extract_data import cp_api_prestador_to_db
from ..bronze.transform_data import transform_prestador
from ..bronze.load_data import load_data_prestador


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
        

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
            ################# PIPELINE PRESTADOR #################

    # EXECUTA EXTRAÇÃO DE DADOS DA API PRESTADOR
    cp_api_prestador_to_db()    

    # # EXECUTA A TRANSFORMAÇÃO NA API DE PRESTADOR
    data = transform_prestador()

    # # EXECUTA A CARGA DE DADOS TRANSFORMADOS NO BANCO DE DADOS
    load_data_prestador(data)
