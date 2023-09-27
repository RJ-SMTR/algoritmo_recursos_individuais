## --- Setup libraries and paths --- ###

import basedosdados as bd
import logging
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta, datetime
import os
import sys
from pathlib import Path 

current_path = Path().resolve().parent

paths = dict()
paths["raw"] = current_path / 'data' / 'raw'
paths["treated"] = current_path / 'data' / 'treated'
paths["output"] = current_path / 'data' / 'output'
paths["figures"] = current_path / 'data' / 'figures'
paths["scripts"] = current_path / 'scripts'
paths["queries"] = current_path / 'scripts' / 'queries'
paths["data_processing"] = current_path / 'scripts' / 'data_processing'
paths["utils"] = current_path / 'scripts' / 'utils'

for path in paths.values():
    if not os.path.exists(path):
        os.makedirs(path)      
        
directories = [paths["scripts"], paths["queries"], paths["data_processing"], paths["utils"]]

for directory in directories:
    if directory not in sys.path:
        sys.path.append(str(directory))

from set_credentials import *
from graphs import *
from categorize_trips import *
from queries_functions import *


## --- Configuração dos arquivos de log --- ##
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"./log/log_{current_time}.txt"
logging.basicConfig(
    filename=log_filename, 
    encoding='utf-8', 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # This format excludes milliseconds
)



### --- 1. Tratar a amostra --- ###

dir_path = '../data/raw'
files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]

if len(files) == 1:
    file_path = os.path.join(dir_path, files[0])
    amostra = pd.read_excel(file_path)
else:
    print("Mais de um arquivo .xlsx encontrado ou nenhum encontrado.")

amostra['servico'] = amostra['servico'].astype(str)
amostra['data'] = amostra['data'].astype(str)
amostra['id_veiculo'] = amostra['id_veiculo'].astype(str)
amostra['hora_inicio'] = amostra['hora_inicio'].astype(str)
amostra['datetime_partida'] = pd.to_datetime(amostra['data'] + ' ' + amostra['hora_inicio'])
amostra['hora_fim'] = amostra['hora_fim'].astype(str)
amostra['datetime_chegada'] = pd.to_datetime(amostra['data'] + ' ' + amostra['hora_fim'])

message = 'Importação da amostra concluída com sucesso.'
logging.debug(message)
print(message)

### --- 2. Remover dados da amostra inválidos / duplicados --- ###
amostra_tratada = remove_overlapping_trips(amostra)


# Quais são os dias e id_veiculo
id_veiculo_query = amostra_tratada['id_veiculo'].drop_duplicates().tolist()
data_query = amostra_tratada['data'].drop_duplicates().tolist()
data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])


message = 'Tratamento da amostra concluído com sucesso.'
logging.debug(message)
print(message)

### --- 3. Fazer a query de viagens completas --- ###

# # descomentar este código
viagem_completa = query_viagem_completa(data_query, id_veiculo_query)
viagem_completa.to_csv('../data/treated/viagem_completa.csv', index = False)

viagem_completa = pd.read_csv('../data/treated/viagem_completa.csv')



# incluir um arquivo de log aqui!

message = 'Acesso aos dados de viagens completas concluído com sucesso.'
logging.debug(message)
print(message)


### --- 4. Classificar Viagens Completas --- ###

viagens_completas_classificadas = check_trips(amostra_tratada, viagem_completa,
                                    "Viagem identificada e já paga")


viagens_completas_classificadas.to_excel('../data/treated/viagem_completa_classificada.xlsx', index = False)

message = 'Classificação das viagens completas concluída com sucesso.'
logging.debug(message)
print(message)



### --- 5. Fazer a query de viagens conformidade --- ###

# Quais são os dias e id_veiculo ainda não classificados

linhas_nan = viagens_completas_classificadas[pd.isna(viagens_completas_classificadas['status'])]

id_veiculo_query = linhas_nan['id_veiculo_amostra'].drop_duplicates().tolist()
data_query = linhas_nan['data'].drop_duplicates().tolist()

data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])


# descomentar este código
viagem_conformidade = query_viagem_conformidade(data_query, id_veiculo_query)
viagem_conformidade.to_csv('../data/treated/viagem_conformidade.csv', index = False)

viagem_conformidade = pd.read_csv('../data/treated/viagem_conformidade.csv')


message = 'Acesso aos dados de viagens conformidade concluído com sucesso.'
logging.debug(message)
print(message)


### --- 6. Classificar Viagens conformidade --- ###

viagens_conformidade_classificadas = check_trips(viagens_completas_classificadas, viagem_conformidade,
                                    "Viagem inválida - Não atingiu % de GPS ou trajeto correto")


viagens_conformidade_classificadas.to_excel('../data/treated/viagem_conformidade_classificada.xlsx', index = False)


message = 'Classificação das viagens conformidade concluída com sucesso.'
logging.debug(message)
print(message)


### --- 7. Fazer a query dos dados de GPS --- ###

# Quais são as datas e veículos não encontrados nas etapas anteriores

linhas_nan = viagens_conformidade_classificadas[pd.isna(viagens_conformidade_classificadas['status'])]

id_veiculo_query = linhas_nan['id_veiculo_amostra'].drop_duplicates().tolist()
data_query = linhas_nan['data'].drop_duplicates().tolist()

data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])



### ---Confirmar se a query dos dados de GPS deve ser executada --- ###

datas_unicas = len(linhas_nan['data'].drop_duplicates())
estimativa_custo = (datas_unicas * 390) / 1000 
print(datas_unicas)

response = ""
while response not in ['y', 'n']:
    response = input(f"Estimativa de consumo de {estimativa_custo} GB para consulta de dados de GPS. Deseja continuar? (y/n): ").lower()
if response == 'y':
    print("Continuando a execução...")
    
    
    
    
    
else:
    print("Parando a execução...")




# adicionar argumento booleano chamado cache para fazer a query ou checar se o arquivo existe localmente nas queries anteriores

# caso este argumento seja verdadeiro perguntar no console se deseja continuar:
# estimativa de consumo de xxxx GB. Deseja continuar? (y/n)


# adicionar pergunta se deseja continuar ou não com a estimativa de processamento de dados:
# gps_sppo estima uns 390mb por data na gps_sppo
# e 390mb por data na status_viagem 




### --- 8. Classificar dados de GPS --- ###

# tranformar tudo isto em uma função com o argumento cache para usar os arquivos csv ou fazer a query




### --- 9. Fazer os mapas em HTML --- ###

# Adicionar argumento para tornar opcional 








# colocar aquele if main aqui no final
# colocar uma flag que permite usar o cache para debugar o código



# instalar o black
# https://dev.to/adamlombard/how-to-use-the-black-python-code-formatter-in-vscode-3lo0


