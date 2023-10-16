import logging
import os
import pandas as pd


def import_sample() -> pd.DataFrame:

    dir_path = './../data/raw'
    files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]

    if len(files) == 1:
        file_path = os.path.join(dir_path, files[0])
        amostra = pd.read_excel(file_path)
        message = 'Importação da amostra realizada com sucesso.'
        logging.debug(message)
        print(message)

    else:
        message = 'Nenhum arquivo encontrado na pasta raw ou existe mais de um arquivo no formato xlsx.'
        logging.debug(message)
        print(message)
    
    return amostra