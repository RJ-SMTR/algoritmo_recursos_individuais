### --- 1. Configurar bibliotecas, diretórios, flags e arquivo de log --- ###
modelo_versao = 'v0.1'

### --- 1.1 Bibliotecas ---###
import argparse
import basedosdados as bd
import logging
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta, datetime
import os
import sys
from pathlib import Path 

### --- 1.2 Diretórios ---###
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
from automate_map import *

### --- 1.3 Arquivo de log ---###
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"./log/log_{current_time}.txt"
logging.basicConfig(
    filename=log_filename, 
    encoding='utf-8', 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # This format excludes milliseconds
)

### --- 1.4 Flags ---###
parser = argparse.ArgumentParser(description="Execute o script run.py com opções.")

# permite usar o cache para não baixar novamente os dados da última consulta no big query
parser.add_argument('--cache', action='store_true', help="Ative o cache")
args = parser.parse_args()
cache = "on" if args.cache else "off"

message = 'Dependências carregadas com sucesso. ' 
logging.debug(message)
print(message)

### --- 2. Amostra --- ###

message = 'Iniciando execução do Modelo de Classificação de Recursos Individuais versão: ' + modelo_versao 
logging.debug(message)
print(message)


### --- 2.1 Importar amostra --- ###
dir_path = '../data/raw'
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


### --- 2.2 Tratar amostra --- ###
# Tratar amostra
amostra = treat_sample(amostra)

message = 'Tratamento da amostra concluído com sucesso.'
logging.debug(message)
print(message)


### --- 2.3 Classificar dados inválidos / duplicados da amostra --- ###
amostra_tratada = remove_overlapping_trips(amostra)

message = 'Verificação de dados inconsistentes na amostra finalizada com sucesso.'
logging.debug(message)
print(message)



# Quais são os dias e o id_veiculo presentes na amostra
id_veiculo_query = amostra_tratada['id_veiculo'].drop_duplicates().tolist()
data_query = amostra_tratada['data'].drop_duplicates().tolist()
data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])




### --- 3. VIAGENS COMPLETAS --- ###

### --- 3.1 Acessar dados das viagens completas --- ###

if args.cache:
    viagem_completa = pd.read_csv('../data/treated/viagem_completa.csv')   
    
else:
    viagem_completa = query_viagem_completa(data_query, id_veiculo_query, reprocessed=False)
    viagem_completa.to_csv('../data/treated/viagem_completa.csv', index = False)

message = 'Acesso aos dados de viagens completas concluído com sucesso.'
logging.debug(message)
print(message)


### --- 3.2 Tratar dados das viagens completas --- ###

viagem_completa = treat_trips(viagem_completa)

message = 'Tratamento dos dados de viagens completas concluído com sucesso.'
logging.debug(message)
print(message)



### --- 3.3 Comparar amostra com as viagens completas --- ###

viagens_completas_classificadas = check_trips(amostra_tratada, viagem_completa,
                                    "Viagem identificada e já paga")

viagens_completas_classificadas.to_excel('../data/treated/viagem_completa_classificada.xlsx', index = False)

message = 'Classificação das viagens completas concluída com sucesso.'
logging.debug(message)
print(message)



### --- 4. VIAGENS CONFORMIDADE --- ###

### --- 4.1 Acessar dados das viagens conformidade --- ###

# Quais são os dias e id_veiculo ainda não classificados

linhas_nan = viagens_completas_classificadas[pd.isna(viagens_completas_classificadas['status'])]

id_veiculo_query = linhas_nan['id_veiculo_amostra'].drop_duplicates().tolist()
data_query = linhas_nan['data'].drop_duplicates().tolist()

data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])

# Acessar os dados
if args.cache:
    viagem_conformidade = pd.read_csv('../data/treated/viagem_conformidade.csv')  
    
else:
    viagem_conformidade = query_viagem_conformidade(data_query, id_veiculo_query, reprocessed=False)
    viagem_conformidade.to_csv('../data/treated/viagem_conformidade.csv', index = False)

### --- 4.2 Tratar dados das viagens conformidade --- ###
viagem_conformidade = treat_trips(viagem_conformidade)

message = 'Acesso aos dados de viagens conformidade concluído com sucesso.'
logging.debug(message)
print(message)


### --- 4.3 Comparar amostra com as viagens conformidade --- ###

viagens_conformidade_classificadas = check_trips(viagens_completas_classificadas, viagem_conformidade,
                                    "Viagem inválida - Não atingiu % de GPS ou trajeto correto")

viagens_conformidade_classificadas.to_excel('../data/treated/viagem_conformidade_classificada.xlsx', index = False)

message = 'Classificação das viagens conformidade concluída com sucesso.'
logging.debug(message)
print(message)


print(viagens_conformidade_classificadas)



### --- 5. Viagens circulares

### --- 5.1 Identificar se a linha é circular --- ###  
# Esta etapa serve para verificar se a viagem do recurso na verdade é uma meia viagem pertencente
# a uma viagem circular. A ideia é identificar a viagem circular a qual pertence a meia viagem

message = 'Verificando se existem linhas circulares.'
logging.debug(message)
print(message)

# verifica os serviços e as datas presentes na amostra
servico_query = amostra_tratada['servico'].drop_duplicates().tolist()
servico_query = ','.join([f"'{id}'" for id in servico_query])
data_query = amostra_tratada['data'].drop_duplicates().tolist()
data_query = ','.join([f"'{d}'" for d in data_query])

if args.cache:
    tipo_servico = pd.read_csv('../data/treated/tipo_servico.csv')   
    
else:
    tipo_servico = query_tipo_linha(data_query, servico_query, include_sentido_shape=True)
    tipo_servico.to_csv('../data/treated/tipo_servico.csv', index = False)


tipo_servico['circular_dividida'] = np.where(
    (tipo_servico['sentido'] == 'C') & (tipo_servico['sentido_shape'] != 'C'), 
    1,  # a linha circular tem o shape dividido em ida e volta
    0   # a linha circular não tem o shape dividido em ida e volta
)

tipo_servico = tipo_servico.drop(columns=['sentido','sentido_shape'])
tipo_servico['data'] = tipo_servico['data'].astype(str)
tipo_servico['servico'] = tipo_servico['servico'].astype(str)
tipo_servico = tipo_servico.drop_duplicates()
viagens_conformidade_classificadas['servico_amostra'] = viagens_conformidade_classificadas['servico_amostra'].astype(str)
viagens_conformidade_classificadas['data'] = viagens_conformidade_classificadas['data'].astype(str)

viagens_conformidade_classificadas = viagens_conformidade_classificadas.merge(
    tipo_servico, 
    left_on=['data', 'servico_amostra'], 
    right_on=['data', 'servico']
)

viagens_conformidade_classificadas.drop_duplicates()

viagens_conformidade_classificadas = viagens_conformidade_classificadas.drop(columns=['servico'])

viagens_conformidade_classificadas # este df tem uma coluna chamada circular_dividida que tem o valor 1




### --- 5.2 Identificar viagens circulares que ainda não foram classificadas --- ###  


# Criar um df com as viagens que serão procuradas

df_circular_na = viagens_conformidade_classificadas[
    (viagens_conformidade_classificadas['circular_dividida'] == 1) &
    pd.isna(viagens_conformidade_classificadas['status'])
]

# Criando o DataFrame df_demais_casos
df_demais_casos = viagens_conformidade_classificadas.drop(df_circular_na.index)


if not df_circular_na.empty:
    # Código a ser executado caso df_circular não esteja vazio
    print("Possíveis meias viagens circulares que constam na amostra:")
    print(df_circular_na)   
    
    # Esta função classifica as meia viagens circulares que não foram identificadas nos passos anteriores
    df_circular_na = check_circular_trip(df_circular_na, viagem_completa, viagem_conformidade)
                                 
    viagens_conformidade_classificadas = pd.concat([df_circular_na, df_demais_casos], ignore_index=True)      

else:
    print("Não existem viagens circulares divididas em shapes de ida e volta.")




### --- 6. SINAIS DE GPS --- ###

### --- 6.1 Confirmar se a query dos dados de GPS deve ser feita --- ###

# Quais são as datas e veículos não encontrados nas etapas anteriores:
linhas_nan = viagens_conformidade_classificadas[pd.isna(viagens_conformidade_classificadas['status'])]

id_veiculo_query = linhas_nan['id_veiculo_amostra'].drop_duplicates().tolist()
data_query = linhas_nan['data'].drop_duplicates().tolist()

data_query = ','.join([f"'{d}'" for d in data_query])
id_veiculo_query = ','.join([f"'{id}'" for id in id_veiculo_query])

# Estimar custos da query e perguntar se deseja continuar
datas_unicas = len(linhas_nan['data'].drop_duplicates())
estimativa_custo = (datas_unicas * 390) / 1000 

proceed = False

if not args.cache:
    response = ""
    while response not in ['y', 'n']:
        response = input(f"Estimativa de consumo de {estimativa_custo} GB para consulta de dados de GPS. Deseja continuar? (y/n): ").lower()
    if response == 'y':
        print("Continuando a execução...")
        proceed = True
else:
    proceed = True

if proceed: # Executar caso o comando cotenha a flag "cache" ou a resposta seja y
    
        
    ### --- 6.2 Acessar os sinais de GPS --- ###
    if args.cache:
        dados_gps = pd.read_csv('../data/treated/dados_gps.csv') 
    
    else:
        dados_gps = query_gps(data_query, id_veiculo_query)
        dados_gps.to_csv('../data/treated/dados_gps.csv', index = False)

    message = 'Acesso aos sinais de GPS concluído com sucesso.'
    logging.debug(message)
    print(message)
    
    
    ### --- 6.3 Tratar os sinais de GPS --- ###
    dados_gps = treat_gps(dados_gps)
    
    message = 'Tratamento de dados de GPS concluído com sucesso.'
    logging.debug(message)
    print(message)


    ### --- 6.4 Comparar amostra com os sinais de GPS --- ###
    viagens_gps_classificadas_nan = viagens_conformidade_classificadas[viagens_conformidade_classificadas['status'].isna()]
    viagens_gps_classificadas_not_nan = viagens_conformidade_classificadas[viagens_conformidade_classificadas['status'].notna()]    

    # Aplicar a função quando o status da viagem for nan
    results = viagens_gps_classificadas_nan.apply(lambda row: check_gps(row, dados_gps), axis=1)
    viagens_gps_classificadas_nan['status'] = results.apply(lambda x: x[0])
    viagens_gps_classificadas_nan['servico_apurado'] = results.apply(lambda x: x[1])

    # Juntar tabela com status nan e status não nan
    viagens_gps_classificadas = pd.concat([viagens_gps_classificadas_nan, viagens_gps_classificadas_not_nan], ignore_index=True)

    viagens_gps_classificadas.to_excel('../data/treated/viagens_gps_classificadas.xlsx', index = False)

    
    
    
    
    
    
    
    
    
    
    
    
    ### -- reprocessamento --###
    # Gerar arquivo csv para reprocessamento caso a viagem seja antes de 16/11/2022
    # e o valor da coluna flag_reprocessamento = 1
    
        
    viagens_gps_classificadas['data'] = pd.to_datetime(viagens_gps_classificadas['data'])
    data_limite = datetime.strptime('2022-11-16', '%Y-%m-%d')

    condicao = (
    (viagens_gps_classificadas['flag_reprocessamento'] == 1) & 
    (viagens_gps_classificadas['data'] <= data_limite) & 
    (viagens_gps_classificadas['servico_amostra'] != viagens_gps_classificadas['servico_apurado']) &
    (viagens_gps_classificadas['status'] != 'Viagem identificada e já paga')
    )

    linhas_condicao = viagens_gps_classificadas[condicao]
    
    demais_linhas = viagens_gps_classificadas[~condicao]
    
    # Verificar se há linhas que atendem à condição
    if not linhas_condicao.empty:
        print("As seguintes linhas atenderam à condição de reprocessamento do serviço:")
        
        print(linhas_condicao)
                
        linhas_condicao.to_csv('./../../queries-rj-smtr/data/reprocessar.csv', index = False)
        
        # Limpar os campos que serão reprocessados:
        columns_to_na = [
        "status", "data_apurado", "id_veiculo_apurado", "servico_apurado",
        "sentido_apurado", "datetime_partida_apurado", "datetime_chegada_apurado"
        ]   

        # Substituindo todos os valores nas colunas selecionadas por NaN
        linhas_condicao.loc[:, columns_to_na] = np.nan
        
        
        input("Execução pausada. Execute o modelo no DBT e pressione enter para continuar...")        
    
        # Baixar e classificar viagens completas reprocessadas
        if args.cache:
            viagem_completa_reprocessada = pd.read_csv('../data/treated/viagem_completa_reprocessada.csv')   
            
        else:
            viagem_completa_reprocessada = query_viagem_completa(data_query, id_veiculo_query, reprocessed=True)
            viagem_completa_reprocessada.to_csv('../data/treated/viagem_completa_reprocessada.csv', index = False)
            
        viagem_completa_reprocessada = treat_trips(viagem_completa_reprocessada)
        
        
        linhas_condicao = check_trips(linhas_condicao, viagem_completa_reprocessada,
                                    "Viagem a ser paga após o reprocessamento")
        
        linhas_condicao.to_excel('../data/treated/viagem_completa_classificada.xlsx', index = False)

        # Baixar e classificar viagens conformidade reprocessadas
        
        if args.cache:
            viagem_conformidade_reprocessada = pd.read_csv('../data/treated/viagem_conformidade_reprocessada.csv')   
            
        else:
            viagem_conformidade_reprocessada = query_viagem_conformidade(data_query, id_veiculo_query, reprocessed=True)
            viagem_conformidade_reprocessada.to_csv('../data/treated/viagem_conformidade_reprocessada.csv', index = False)
            
        viagem_conformidade_reprocessada = treat_trips(viagem_conformidade_reprocessada)
        
        
        linhas_condicao = check_trips(linhas_condicao, viagem_conformidade_reprocessada,
                                     "Viagem inválida devido a conformidade mesmo após o reprocessamento")
        
        linhas_condicao.to_excel('../data/treated/viagem_conformidade_classificada.xlsx', index = False)
                
        viagens_gps_classificadas = pd.concat([linhas_condicao, demais_linhas], ignore_index=True)
  
  



        

    ### --- 7. Criar mapas em HTML para viagens da amostra não identificadas, mas com sinal de GPS --- ###
    
    # status para criar mapa de GPS
    status_check = 'Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra'
    viagens_gps_to_map = viagens_gps_classificadas[viagens_gps_classificadas['status'] == status_check]
    
    ### --- 7.1 Acessar dados dos shapes --- ###
    if args.cache:
        dados_shape = pd.read_csv('../data/treated/dados_gps_shape.csv')
    
    else:
        dados_shape = query_shape(data_query, servico_query)
        dados_shape.to_csv('../data/treated/dados_gps_shape.csv', index=False)
               
    dados_shape['servico'] = dados_shape['servico'].astype(str)
    
    message = 'Shapes acessados com sucesso.'
    logging.debug(message)
    print(message)
    
    
    ### --- 7.2 Gerar mapas em HTML --- ###
    message = 'Iniciando a geração dos mapas em HTML.'
    logging.debug(message)
    print(message)
       
    results = viagens_gps_to_map.apply(lambda row: automate_map(row, dados_gps, dados_shape, viagens_gps_to_map), axis=1)

    message = 'Mapas em HTML gerados com sucesso e disponíveis no diretório data/output/maps.'
    logging.debug(message)
    print(message)
    
    ### --- 8. Ajustes finais --- ###
    
    # remover as colunas flag_reprocessamento e circular_dividida
    
    
    ### --- 8.1 Adicionar coluna com a versão do modelo --- ###
    viagens_gps_classificadas['versao_modelo'] = modelo_versao
    
    ### --- 8.2 Exportar tabela final com status em xlsx e json --- ###  

    viagens_gps_classificadas.to_excel('../data/output/amostra_classificada.xlsx', index=False)

    message = 'Arquivo com os status exportado com sucesso em xlsx no diretório data/output.'
    logging.debug(message)
    print(message)
     
    viagens_gps_classificadas.to_json('../data/output/amostra_classificada.json')
    
    message = 'Arquivo com os status exportado com sucesso em json no diretório data/output.'
    logging.debug(message)
    print(message)   
        
        
    ### --- 8.3 Gerar um resumo das viagens processadas pelo algoritmo --- ###  
    
    message = 'Relatório da execução do algoritmo:'
    logging.debug(message)
    print(message)
    
    # Criar um dataframe para armazenar os resultados
    tabela = pd.DataFrame(index=['Parecer definido','Parecer indefinido'], columns=['Contagem', 'Porcentagem'])
    
    # Contar as ocorrências de cada caso
    total_rows = len(viagens_gps_classificadas)
    tabela.loc['Parecer indefinido', 'Contagem'] = sum(viagens_gps_classificadas['status'] == status_check)
    tabela.loc['Parecer definido', 'Contagem'] = total_rows - tabela.loc['Parecer indefinido', 'Contagem']

    # Calcular as porcentagens
    tabela['Porcentagem'] = (tabela['Contagem'] / total_rows) * 100    
    logging.debug(tabela)
    print(tabela)     
    
    message = 'Execução do algoritmo finalizada.'
    logging.debug(message)
    print(message)   
        
else: # caso a resposta seja n na pergunta "Deseja continuar? (y/n):"
    print("Execução do algoritmo finalizada com sucesso!")