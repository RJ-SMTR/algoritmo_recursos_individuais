## --- Setup libraries and paths --- ###

modelo_versao = 'v0.1'

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


for path in paths.values():
    if not os.path.exists(path):
        os.makedirs(path)      
        
directories = [paths["scripts"], paths["queries"], paths["data_processing"]]

for directory in directories:
    if directory not in sys.path:
        sys.path.append(str(directory))

from set_credentials import *
from graphs import *
from categorize_trips import *
from queries_functions import *
from treat_data import *

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


message = 'Iniciando execução do Modelo de Classificação de Recursos Individuais versão: ' + modelo_versao 
logging.debug(message)
print(message)



### --- 1. Tratar a amostra --- ###

dir_path = '../data/raw'
files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]

if len(files) == 1:
    file_path = os.path.join(dir_path, files[0])
    amostra = pd.read_excel(file_path)
else:
    message = 'Nenhum arquivo encontrado na pasta raw ou existe mais de um arquivo no formato xlsx.'
    logging.debug(message)
    print(message)
 

# Tratar dados da amostra
amostra = treat_sample(amostra)


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

# # descomentar este código ----------------------------------------------------------------
# viagem_completa = query_viagem_completa(data_query, id_veiculo_query)
# viagem_completa.to_csv('../data/treated/viagem_completa.csv', index = False)

viagem_completa = pd.read_csv('../data/treated/viagem_completa.csv')

# Tratar os dados
viagem_completa = treat_trips(viagem_completa)



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


# descomentar este código -------------------------------------------------------------------------
# viagem_conformidade = query_viagem_conformidade(data_query, id_veiculo_query)
# viagem_conformidade.to_csv('../data/treated/viagem_conformidade.csv', index = False)

viagem_conformidade = pd.read_csv('../data/treated/viagem_conformidade.csv')

# Tratar os dados
viagem_conformidade = treat_trips(viagem_conformidade)


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



### --- 7. Confirmar se query dos dados de GPS deve ser feita --- ###

# Quais são as datas e veículos não encontrados nas etapas anteriores

linhas_nan = viagens_conformidade_classificadas[pd.isna(viagens_conformidade_classificadas['status'])]

id_veiculo_query = linhas_nan['id_veiculo_amostra'].drop_duplicates().tolist()
data_query = linhas_nan['data'].drop_duplicates().tolist()

data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])


# Input com resposta y ou n
datas_unicas = len(linhas_nan['data'].drop_duplicates())
estimativa_custo = (datas_unicas * 390) / 1000 

response = ""
while response not in ['y', 'n']:
    response = input(f"Estimativa de consumo de {estimativa_custo} GB para consulta de dados de GPS. Deseja continuar? (y/n): ").lower()
if response == 'y':
    print("Continuando a execução...")

        
    ### --- Realizar a query de GPS --- ###
    # DESCOMENTAR ----------------------------------------------------------------------------
    # dados_gps = query_gps(data_query, id_veiculo_query)
    # dados_gps.to_csv('../data/treated/dados_gps.csv', index = False)

    dados_gps = pd.read_csv('../data/treated/dados_gps.csv')

    # TRATAR OS DADOS COM A FUNÇÃO NOVA
    dados_gps = treat_gps(dados_gps)
    
    

### --- 8. Classificar dados de GPS --- ###
    
    viagens_gps_classificadas_nan = viagens_conformidade_classificadas[viagens_conformidade_classificadas['status'].isna()]
    viagens_gps_classificadas_not_nan = viagens_conformidade_classificadas[viagens_conformidade_classificadas['status'].notna()]    

    # passar esta função para outro script!!!

    def check_gps(row, df_check):
        # Filter the df_check by vehicle ID and time range
        filtered_df = df_check[
            (df_check['id_veiculo'] == row['id_veiculo_amostra']) & 
            (df_check['timestamp_gps'] >= row['datetime_partida_amostra']) & 
            (df_check['timestamp_gps'] <= row['datetime_chegada_amostra'])
        ]
        
        # Get unique services from filtered_df
        unique_servicos = filtered_df['servico'].unique()
        servico_apurado = ', '.join(unique_servicos)

        if not filtered_df.empty and np.isnan(row['status']):
            if filtered_df.iloc[0]['servico'] == row['servico_amostra']:
                return ("Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra", servico_apurado)
            else:
                return ("Sinal de GPS encontrado para o veículo operando em serviço diferente da amostra", servico_apurado)
        else:
            return ("Sinal de GPS não encontrado para o veículo no horário da viagem", np.nan)

    # Apply the function to the rows where status is NaN
    results = viagens_gps_classificadas_nan.apply(lambda row: check_gps(row, dados_gps), axis=1)
    viagens_gps_classificadas_nan['status'] = results.apply(lambda x: x[0])
    viagens_gps_classificadas_nan['servico_apurado'] = results.apply(lambda x: x[1])

    # Concatenate the modified DataFrame with the DataFrame where status is not NaN
    viagens_gps_classificadas = pd.concat([viagens_gps_classificadas_nan, viagens_gps_classificadas_not_nan], ignore_index=True)

    # Save to Excel
    viagens_gps_classificadas.to_excel('../data/treated/viagens_gps_classificadas.xlsx', index=False)


### --- 9. Fazer os mapas em HTML para casos com GPS  --- ###

    
    # checar status
    status_check = 'Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra'
    viagens_gps_to_map = viagens_gps_classificadas[viagens_gps_classificadas['status'] == status_check]
    viagens_gps_not_map = viagens_gps_classificadas[viagens_gps_classificadas['status'] != status_check]

    id_veiculo_query = viagens_gps_to_map['id_veiculo_amostra'].drop_duplicates().tolist()
    data_query = viagens_gps_to_map['data'].drop_duplicates().tolist()
    servico_query = viagens_gps_to_map['servico_amostra'].drop_duplicates().tolist()

    data_query = ','.join([f"'{d}'" for d in data_query])
    id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])
    servico_query = ','.join([f"'{id}'" for id in servico_query])
    
    datas_unicas = len(viagens_gps_to_map['data'].drop_duplicates())
    estimativa_custo = (datas_unicas * 390) / 1000 
    
    response = ""
    while response not in ['y', 'n']:
        response = input(f"Estimativa de consumo de {estimativa_custo} GB para geração de mapas com GPS. Deseja continuar? (y/n): ").lower()
    
    if response == 'y':
        print("Continuando a execução...")        
        
        # ## --- 10. Fazer a query com os mapas --- ##
        # dados_gps_status = query_gps_status(data_query, id_veiculo_query)
        # dados_gps_status.to_csv('../data/treated/dados_gps_status.csv', index = False)

        dados_gps_status = pd.read_csv('../data/treated/dados_gps_status.csv')
           
        # CONTINUAR DAQUI!!!
        # tratar dados do gps_status como fiz com as outras queries com uma função específica
           
           
        dados_shape = query_shape(data_query, servico_query) 
        dados_shape.to_csv('../data/treated/dados_gps_shape.csv', index = False)
        dados_shape = pd.read_csv('../data/treated/dados_gps_shape.csv')

        # tratar dados da viagem_planejada como fiz com as outras queries com uma função específica
               
        
        
        # Fazer o mapa aqui e salvar em uma pasta com o nome dinamico
        


        
        
        
        
        # add coluna com a versão do modelo que rodei a desse é v_0.1
        
        
        # retornar o df completo com os status em xlsx
        
        
        
        # imprimir no final e salvar um arquivo com um relatório (quantas viagens foram encontradas em cada situação)
        
        # qual foi o percentual de classificação respondida
        
        
    else:
        print("Execução finalizada.")
    
 
  
else:
    print("Execução finalizada.")
   
   
   
   
# Melhorias
# colocar aviso caso a query não funcione para checar as credenciais ou a conexão com o bigquery
# documentar as funções
# tranformar tudo isto em uma função com o argumento cache para usar os arquivos csv ou fazer a query
# adicionar argumento booleano chamado cache para fazer a query ou checar se o arquivo existe localmente nas queries anteriores
# colocar aquele if main aqui no final
# colocar uma flag que permite usar o cache para debugar o código
# checar se log está em todas as etapas
# adicionar conteúdo da pasta de log ao git ignore
# instalar o black
# https://dev.to/adamlombard/how-to-use-the-black-python-code-formatter-in-vscode-3lo0

# testar com viagens circulares


