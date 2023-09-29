## --- Setup libraries and paths --- ###
modelo_versao = 'v0.1'

cache = "off" # "off"

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
paths["maps"] = current_path / 'data' / 'output' / 'maps'
paths["figures"] = current_path / 'data' / 'figures'
paths["scripts"] = current_path / 'scripts'
paths["queries"] = current_path / 'scripts' / 'queries'
paths["data_processing"] = current_path / 'scripts' / 'data_processing'
paths["dataviz"] = current_path / 'scripts' / 'dataviz'


for path in paths.values():
    if not os.path.exists(path):
        os.makedirs(path)      
        
directories = [paths["scripts"], paths["queries"], paths["data_processing"], paths["dataviz"]]

for directory in directories:
    if directory not in sys.path:
        sys.path.append(str(directory))

from set_credentials import *
from graphs import *
from categorize_trips import *
from queries_functions import *
from treat_data import *
from graphs import *

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



### --- 3. Identificar se a linha é circular --- ### # comentar isto daqui pra voltar a funcionar 


servico_query = amostra_tratada['servico'].drop_duplicates().tolist()
servico_query = ','.join([f"'{id}'" for id in servico_query])

tipo_servico = query_tipo_linha(data_query, servico_query)
tipo_servico['servico_circular'] = np.where(tipo_servico['sentido'] == 'C', 1, 0)
tipo_servico = tipo_servico.drop(columns=['sentido'])


tipo_servico['data'] = tipo_servico['data'].astype(str)
tipo_servico['servico'] = tipo_servico['servico'].astype(str)
tipo_servico = tipo_servico.drop_duplicates()

amostra_tratada = amostra_tratada.merge(tipo_servico, on=['data', 'servico'])


# esta coluna indica se o servico é circular servico_circular = 1

# O tratamento deve ser diferente para elas!!







### --- 3. Fazer a query de viagens completas --- ###

if cache == "on":
    viagem_completa = pd.read_csv('../data/treated/viagem_completa.csv')
else:
    viagem_completa = query_viagem_completa(data_query, id_veiculo_query)
    viagem_completa.to_csv('../data/treated/viagem_completa.csv', index = False)

# Tratar os dados
viagem_completa = treat_trips(viagem_completa)


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


if cache == "on":
    viagem_conformidade = pd.read_csv('../data/treated/viagem_conformidade.csv')
else:
    viagem_conformidade = query_viagem_conformidade(data_query, id_veiculo_query)
    viagem_conformidade.to_csv('../data/treated/viagem_conformidade.csv', index = False)


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
    if cache == "on":
        dados_gps = pd.read_csv('../data/treated/dados_gps.csv')
    
    else:
        dados_gps = query_gps(data_query, id_veiculo_query)
        dados_gps.to_csv('../data/treated/dados_gps.csv', index = False)


    message = 'Acesso aos dados de GPS concluído com sucesso.'
    logging.debug(message)
    print(message)
    
    
    # TRATAR OS DADOS COM A FUNÇÃO NOVA
    dados_gps = treat_gps(dados_gps)
    
    message = 'Tratamento de dados de GPS concluído com sucesso.'
    logging.debug(message)
    print(message)

### --- 8. Classificar dados de GPS --- ###
    
    viagens_gps_classificadas_nan = viagens_conformidade_classificadas[viagens_conformidade_classificadas['status'].isna()]
    viagens_gps_classificadas_not_nan = viagens_conformidade_classificadas[viagens_conformidade_classificadas['status'].notna()]    

    # Apply the function to the rows where status is NaN
    results = viagens_gps_classificadas_nan.apply(lambda row: check_gps(row, dados_gps), axis=1)
    viagens_gps_classificadas_nan['status'] = results.apply(lambda x: x[0])
    viagens_gps_classificadas_nan['servico_apurado'] = results.apply(lambda x: x[1])

    # Concatenate the modified DataFrame with the DataFrame where status is not NaN
    viagens_gps_classificadas = pd.concat([viagens_gps_classificadas_nan, viagens_gps_classificadas_not_nan], ignore_index=True)

    # Save to Excel
    viagens_gps_classificadas.to_excel('../data/treated/viagens_gps_classificadas.xlsx', index=False)


### --- 9. Fazer os mapas em HTML para casos com GPS  --- ###
    
    status_check = 'Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra'
    viagens_gps_to_map = viagens_gps_classificadas[viagens_gps_classificadas['status'] == status_check]
    
    id_veiculo_query = viagens_gps_to_map['id_veiculo_amostra'].drop_duplicates().tolist()
    data_query = viagens_gps_to_map['data'].drop_duplicates().tolist()
    servico_query = viagens_gps_to_map['servico_amostra'].drop_duplicates().tolist()

    data_query = ','.join([f"'{d}'" for d in data_query])
    servico_query = ','.join([f"'{id}'" for id in servico_query])
    
    # shape    
    if cache == "on":
        dados_shape = pd.read_csv('../data/treated/dados_gps_shape.csv')
    
    else:
        dados_shape = query_shape(data_query, servico_query) 
        dados_shape.to_csv('../data/treated/dados_gps_shape.csv', index = False)   
    
    dados_shape['servico'] = dados_shape['servico'].astype(str) 
           
    # passar função para script de funções:
    def check_map(row, df_check):
        
        # Filter the df_check by vehicle ID and time range
        filtered_df = df_check[
            (df_check['id_veiculo'] == row['id_veiculo_amostra']) & 
            (df_check['timestamp_gps'] >= row['datetime_partida_amostra']) & 
            (df_check['timestamp_gps'] <= row['datetime_chegada_amostra'])
        ]

        unique_veiculos = filtered_df['id_veiculo'].unique()
        if not filtered_df.empty and row['status'] == status_check:   
        
            for veiculo in unique_veiculos:
                
                gps_mapa = filtered_df[filtered_df['id_veiculo'] == veiculo]
                servico_do_veiculo = gps_mapa['servico'].iloc[0]
                shape_mapa = dados_shape[dados_shape['servico'] == servico_do_veiculo]

                map = create_trip_map(gps_mapa, shape_mapa)
                # Pegando o valor da partida para nomear o arquivo
                partida = viagens_gps_to_map[viagens_gps_to_map['id_veiculo_amostra'] == veiculo]['datetime_partida_amostra'].iloc[0]

                hora_formatada = partida.strftime('%Hh%M')
                
                filename = f"./../data/output/maps/{veiculo} {partida.date()} {hora_formatada}.html"
                map.save(filename)
                    
           
    viagens_gps_to_map.apply(lambda row: check_map(row, dados_gps), axis=1)
    
    message = 'Mapas gerados com sucesso e disponíveis em data/output/maps.'
    logging.debug(message)
    print(message)
    
    
    # Adiciona a versão do modelo na tabela de status
        
    viagens_gps_classificadas['versao_modelo'] = modelo_versao
        
    # exportar em Excel
    viagens_gps_classificadas.to_excel('../data/output/amostra_classificada.xlsx', index=False)

    message = 'Arquivo com os status exportado com sucesso em xlsx.'
    logging.debug(message)
    print(message)
     
    viagens_gps_classificadas.to_json('../data/output/amostra_classificada.json')
    
    message = 'Arquivo com os status exportado com sucesso em json.'
    logging.debug(message)
    print(message)   
        
    # imprimir no final e salvar um arquivo com um relatório (quantas viagens foram encontradas em cada situação)
    
    message = 'Relatório da execução do algoritmo:'
    logging.debug(message)
    print(message)
    # Criando um dataframe para armazenar os resultados
    tabela = pd.DataFrame(index=['Parecer definido','Parecer indefinido'], columns=['Contagem', 'Porcentagem'])
    # Contando as ocorrências de cada caso
    total_rows = len(viagens_gps_classificadas)
    tabela.loc['Parecer indefinido', 'Contagem'] = sum(viagens_gps_classificadas['status'] == status_check)
    tabela.loc['Parecer definido', 'Contagem'] = total_rows - tabela.loc['Parecer indefinido', 'Contagem']

    # Calculando as porcentagens
    tabela['Porcentagem'] = (tabela['Contagem'] / total_rows) * 100    
    logging.debug(tabela)
    print(tabela)        
        
else:
    print("Execução do algoritmo finalizada.")
    