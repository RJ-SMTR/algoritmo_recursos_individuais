import os
import pandas as pd
from utils import *

def import_sample() -> pd.DataFrame:

    dir_path = './../data/raw'
    files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]

    if len(files) == 1:
        file_path = os.path.join(dir_path, files[0])
        amostra = pd.read_excel(file_path)
        log_info('Importação da amostra realizada com sucesso.')


    else:
        log_info('Nenhum arquivo encontrado ou existe mais de um arquivo no formato xlsx dentro da pasta "data/raw".')
    
    return amostra