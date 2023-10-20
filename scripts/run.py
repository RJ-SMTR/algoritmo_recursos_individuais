### --- 1. Configurar bibliotecas, diretórios, flags e arquivo de log --- ###
modelo_versao = 'v0.1'

### --- 1.1 Bibliotecas ---###
import argparse
import basedosdados as bd
from datetime import timedelta, datetime
import geopy                          
from geopy.distance import geodesic
import logging
import numpy as np
import os
import pandas as pd
import plotly.graph_objects as go
import re
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
from import_files import *
from reprocess_trips import *
from circular_trips import *
from gps_data import *
from utils import *


### --- 1.3 Flags ---###
parser = argparse.ArgumentParser(description="Execute o script run.py com opções.")

# permite usar o cache para não baixar novamente os dados da última consulta no big query
parser.add_argument('--cache', action='store_true', help="Ative o cache para evitar baixar novamente os dados.")
args = parser.parse_args()
cache = "on" if args.cache else "off"

log_info('Dependências carregadas com sucesso.')

### --- 2. Amostra --- ###


# Esta etapa importa, faz o tratamento e remove viagens inconsistentes dos recursos recebidos.
log_info('Iniciando execução do Algoritmo de Classificação de Recursos (ACRe) versão: ' + modelo_versao)

### --- 2.1 Importar amostra --- ###
amostra = import_sample()    

### --- 2.2 Tratar amostra --- ###
# Tratar amostra
amostra = treat_sample(amostra)

### --- 2.3 Classificar dados inválidos / duplicados da amostra --- ###
amostra_tratada = remove_overlapping_trips(amostra)


# Datas e veículos presentes na amostra
id_veiculo_query = query_values(amostra_tratada, 'id_veiculo')
data_query = query_values(amostra_tratada, 'data')



### --- 3. VIAGENS COMPLETAS --- ###


# Compara os recursos com a tabela de viagem_completa.


### --- 3.1 Acessar dados das viagens completas --- ###

if args.cache:
    viagem_completa = pd.read_csv('../data/cache/viagem_completa.csv')   
    
else:
    viagem_completa = query_viagem_completa(data_query, id_veiculo_query, reprocessed=False)
    viagem_completa.to_csv('../data/cache/viagem_completa.csv', index = False)

log_info('Acesso aos dados de viagens completas concluído com sucesso.')


### --- 3.2 Tratar dados das viagens completas --- ###

viagem_completa = treat_trips(viagem_completa)

log_info('Tratamento dos dados de viagens completas concluído com sucesso.')



### --- 3.3 Comparar amostra com as viagens completas --- ###

viagem_completa_classificada = check_trips(amostra_tratada, viagem_completa,
                                    "Viagem identificada e já paga")

viagem_completa_classificada.to_excel('../data/treated/viagem_completa_classificada.xlsx', index = False)

log_info('Classificação das viagens completas concluída com sucesso.')


### --- 4. REPROCESSAMENTO ---###

viagem_completa_reprocessada = reprocess_trips(viagem_completa_classificada)


### --- 5. VIAGENS CONFORMIDADE --- ###


# Compara os recursos com a tabela de viagem_conformidade.

# ### --- 5.1 Acessar dados das viagens conformidade --- ###
# Quais são os dias e id_veiculo ainda não classificados

linhas_nan = viagem_completa_reprocessada[pd.isna(viagem_completa_reprocessada['status'])]
data_query = query_values(linhas_nan, 'data')
id_veiculo_query = query_values(linhas_nan, 'id_veiculo_amostra')

# Acessar os dados
if args.cache:
    viagem_conformidade = pd.read_csv('../data/cache/viagem_conformidade.csv')  
else:
    viagem_conformidade = query_viagem_conformidade(data_query, id_veiculo_query, reprocessed=False)
    viagem_conformidade.to_csv('../data/cache/viagem_conformidade.csv', index = False)

### --- 5.2 Tratar dados das viagens conformidade --- ###
viagem_conformidade = treat_trips(viagem_conformidade)

log_info('Acesso aos dados de viagens conformidade concluído com sucesso.')


### --- 5.3 Comparar amostra com as viagens conformidade --- ###
viagens_conformidade_classificadas = check_trips(viagem_completa_reprocessada, viagem_conformidade,
                                    "Viagem indeferida - Não atingiu % de GPS ou trajeto correto")

viagens_conformidade_classificadas.to_excel('../data/treated/viagem_conformidade_classificada.xlsx', index = False)

log_info('Classificação das viagens conformidade concluída com sucesso.')

print(viagens_conformidade_classificadas)



### --- 6. VIAGENS CIRCULARES --- ### 


# Esta etapa verifica se a viagem do recurso é, na verdade, uma meia viagem pertencente
# a uma viagem circular. Em seguida, esta meia viagem recebe um status, igual ao da outra meia
# viagem que, junto com ela, forma uma viagem completa.


viagens_conformidade_classificadas = circular_trips(viagens_conformidade_classificadas, 
                                                    viagem_completa,
                                                    viagem_conformidade)


### --- 7. SINAIS DE GPS --- ###

# Esta etapa busca os sinais de GPS para as viagens que ainda não foram classificadas nas etapas anteriores.

### --- Confirmar se a query dos dados de GPS deve ser feita --- ###

# Quais são as datas e veículos não encontrados nas etapas anteriores:
linhas_nan = viagens_conformidade_classificadas[pd.isna(viagens_conformidade_classificadas['status'])]

# Estimar custos da query e perguntar se deseja continuar
datas_unicas = len(linhas_nan['data'].drop_duplicates())
estimativa_custo = (datas_unicas * 400) / 1000 

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

if proceed: # Executar caso o comando contenha a flag "cache" ou a resposta seja y
    
    # Acessar, o tratar e a classificar as viagens de acordo com os dados de GPS.
    viagens_gps_classificadas = gps_data(linhas_nan, viagens_conformidade_classificadas)
    
    ## --- 7.1 Verificar se houveram sinais de GPS no entorno dos pontos inicial e final --- ###

    status_check = 'Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra'
    viagens_com_gps = viagens_gps_classificadas[viagens_gps_classificadas['status'] == status_check]
    viagens_ja_classificadas = viagens_gps_classificadas[viagens_gps_classificadas['status'] != status_check]
    
    if args.cache:
        dados_shape = pd.read_csv('../data/cache/dados_gps_shape.csv')
        
    else:
        dados_shape = query_planned_trips(viagens_com_gps,
                                        include_shape_direction = False,
                                        geometry_data = True)
        
        dados_shape.to_csv('../data/cache/dados_gps_shape.csv', index=False)
            
    dados_shape['servico'] = dados_shape['servico'].astype(str)
        
    # Acessar e tratar dados de GPS
    filtro_gps = viagens_com_gps[['id_veiculo_amostra','data']].drop_duplicates(subset=['id_veiculo_amostra', 'data'])  
    dados_gps = pd.read_csv('../data/cache/dados_gps.csv')
    dados_gps = treat_gps(dados_gps)
    
    dados_gps['data'] = dados_gps['timestamp_gps'].dt.date.astype(str)
    dados_gps['id_veiculo'] = dados_gps['id_veiculo'].astype(str)
    filtro_gps['data'] = filtro_gps['data'].astype(str)
    filtro_gps['id_veiculo_amostra'] = filtro_gps['id_veiculo_amostra'].astype(str)

    dados_gps = dados_gps.merge(filtro_gps, 
                                left_on=['data','id_veiculo'], 
                                right_on=['data','id_veiculo_amostra'], 
                                how='inner')
                
            
    # Acessar e tratar dados do shape/viagem_planejada
    filtro_shape = viagens_com_gps[['servico_amostra','data']].drop_duplicates(subset=['servico_amostra', 
                                                                                    'data']) 
    
    filtro_shape.to_excel('./../data/treated/dados_shape.xlsx')
    

    dados_shape['data'] = dados_shape['data'].astype(str)
    dados_shape['servico'] = dados_shape['servico'].astype(str)
    filtro_shape['data'] = filtro_shape['data'].astype(str)
    filtro_shape['servico_amostra'] = filtro_shape['servico_amostra'].astype(str)
        
    # juntar dados de GPS com os respectivos ponto final/inicial
    dados_shape = dados_shape.merge(filtro_shape, 
                                left_on=['data','servico'], 
                                right_on=['data','servico_amostra'], 
                                how='inner')

    # Verificar pontos nos raios de 500m inicial e final   
    merged_data = pd.merge(dados_shape, 
                        dados_gps, on=['data', 'servico'], how='inner')  
        
    # converter formato dos pontos
    def point_to_tuple(point_string):
        # Extrair os valores numéricos
        coords = re.findall(r"[-+]?\d*\.\d+|\d+", point_string)
        # Retorna as coordenadas como uma tupla de floats
        return (float(coords[1]), float(coords[0]))  # conversão para (lat, lon)
    
    merged_data['end_pt'] = merged_data['end_pt'].apply(point_to_tuple)
    merged_data['start_pt'] = merged_data['start_pt'].apply(point_to_tuple)    
    merged_data['posicao_veiculo_geo'] = merged_data['posicao_veiculo_geo'].apply(point_to_tuple)
    
    # Função para verificar se as coordenadas estão dentro do raio de 500m
    def is_within_radius(point1, point2, radius):
        distance = geodesic(point1, point2).meters
        return 1 if distance <= radius else 0

    # Criar colunas que indicam se o sinal de GPS está dentro do raio de 500m
    
    merged_data['check_start_pt'] = merged_data.apply(lambda row: is_within_radius(row['start_pt'], row['posicao_veiculo_geo'], 500), axis=1)
    merged_data['check_end_pt'] = merged_data.apply(lambda row: is_within_radius(row['end_pt'], row['posicao_veiculo_geo'], 500), axis=1)

    
    # Partida
    partida = []  
    
    for index, row in viagens_com_gps.iterrows():
        mask = (merged_data['id_veiculo'] == row['id_veiculo_amostra']) & (merged_data['timestamp_gps'] > row['datetime_partida_amostra']) & (merged_data['timestamp_gps'] < (row['datetime_partida_amostra'] + pd.Timedelta(minutes=5)))
        filtered_data = merged_data[mask]
        
        # Agregar dados
        aggregated_data = filtered_data.groupby('id_veiculo').agg({
            'check_start_pt': 'sum',
            'check_end_pt': 'sum',
            'timestamp_gps': 'mean'
        }).reset_index()
        
        partida.append(aggregated_data)

    # Concatenar ao final do loop
    df_partida = pd.concat(partida) 
    
    df_partida = df_partida.rename(columns={
    'check_start_pt': 'check_start_pt_partida',
    'check_end_pt': 'check_end_pt_partida',
    'timestamp_gps': 'timestamp_gps_partida'
    })


    # Chegada 
    chegada = []

    for index, row in viagens_com_gps.iterrows():
        mask = (merged_data['id_veiculo'] == row['id_veiculo_amostra']) & (merged_data['timestamp_gps'] < row['datetime_chegada_amostra']) & (merged_data['timestamp_gps'] > (row['datetime_chegada_amostra'] - pd.Timedelta(minutes=5)))        
        filtered_data = merged_data[mask]
        chegada.append(filtered_data)

    df_chegada = pd.concat(chegada)
        
    df_chegada = df_chegada.groupby('id_veiculo').agg({
        'check_start_pt': 'sum',
        'check_end_pt': 'sum',
        'timestamp_gps': 'mean'
    }).reset_index()
    
    df_chegada = df_chegada.rename(columns={
    'check_start_pt': 'check_start_pt_chegada',
    'check_end_pt': 'check_end_pt_chegada',
    'timestamp_gps': 'timestamp_gps_chegada'
    })
            
        
    df_chegada['data'] = df_chegada['timestamp_gps_chegada'].dt.date.astype(str)
    df_partida['data'] = df_partida['timestamp_gps_partida'].dt.date.astype(str)
    viagens_com_gps['data'] = viagens_com_gps['data'].astype(str) 
    
    merged_partida = pd.merge(viagens_com_gps, df_partida,
                            left_on=['data','id_veiculo_amostra'], 
                            right_on=['data','id_veiculo'],                             
                            how='left')
    

    merged_chegada = pd.merge(merged_partida, df_chegada, 
                            left_on=['data','id_veiculo_amostra'], 
                            right_on=['data','id_veiculo'],                             
                            how='left')

    # Remover dados que não são das viagens
    # Crie máscaras booleanas com base nas condições
    mask_partida = (merged_chegada['timestamp_gps_partida'] >= merged_chegada['datetime_partida_amostra']) & \
                (merged_chegada['timestamp_gps_partida'] <= merged_chegada['datetime_chegada_amostra'])

    mask_chegada = (merged_chegada['timestamp_gps_chegada'] >= merged_chegada['datetime_partida_amostra']) & \
                    (merged_chegada['timestamp_gps_chegada'] <= merged_chegada['datetime_chegada_amostra'])

    # Aplique as máscaras para definir valores vazios (NA) onde as condições não são atendidas
    columns_partida = ['id_veiculo_x', 'check_start_pt_partida', 'check_end_pt_partida', 'timestamp_gps_partida']
    columns_chegada = ['id_veiculo_y', 'check_start_pt_chegada', 'check_end_pt_chegada', 'timestamp_gps_chegada']

    merged_chegada.loc[~mask_partida, columns_partida] = np.nan
    merged_chegada.loc[~mask_chegada, columns_chegada] = np.nan

    
    
    # checa se passou a 500m do ponto inicial e final
    condition = (
    ((merged_chegada['check_start_pt_partida'] == 0) | merged_chegada['check_start_pt_partida'].isna()) |
    ((merged_chegada['check_start_pt_chegada'] == 0) | merged_chegada['check_start_pt_chegada'].isna()) |
    ((merged_chegada['check_end_pt_partida'] == 0) | merged_chegada['check_end_pt_partida'].isna()) |
    ((merged_chegada['check_end_pt_chegada'] == 0) | merged_chegada['check_end_pt_chegada'].isna())
    )

    # Atualizar a coluna 'status' com base na condição
    merged_chegada.loc[condition, 'status'] = "O veículo não passou no raio de 500m do ponto de partida/final do trajeto"
    
    start_idx = merged_chegada.columns.get_loc('id_veiculo_x')
    # Exclua todas as colunas a partir desse índice
    merged_chegada.drop(merged_chegada.columns[start_idx:], axis=1, inplace=True)
    
    viagens_gps_classificadas = pd.concat([viagens_ja_classificadas, merged_chegada], ignore_index=True)
    print(viagens_gps_classificadas)
    viagens_gps_classificadas.to_excel('./../data/treated/gps_classificado_inicio_fim.xlsx')
        
    # merged_data.to_excel('./../data/treated/merged_data.xlsx')



    ### --- 8. Criar mapas em HTML  --- ###
    # Esta etapa cria mapas em HTML para as viagens que tiveram sinais de GPS encontrados, mas não foram
    # classificadas nas etapas anteriores.
    
    
    ### --- 8.1 Acessar dados dos shapes --- ###
    
    dados_shape.to_csv('../data/cache/dados_gps_shape.csv', index=False)        
    dados_shape['servico'] = dados_shape['servico'].astype(str)
    
    log_info('Shapes acessados com sucesso.')
    
    dados_gps = pd.read_csv('../data/cache/dados_gps.csv')
    dados_gps = treat_gps(dados_gps)
            
    
    ### --- 8.2 Gerar mapas em HTML --- ###
    
    automate_map(viagens_gps_classificadas, dados_shape, dados_gps)

    
    ### --- 9. Ajustes finais --- ###
    
    # remover as colunas flag_reprocessamento e circular_dividida
    viagens_gps_classificadas = viagens_gps_classificadas.drop(['flag_reprocessamento'], axis=1)
    
    ### --- 9.1 Adicionar coluna com a versão do modelo --- ###
    viagens_gps_classificadas['versao_modelo'] = modelo_versao    
    
    ### --- 9.2 Exportar tabela final com status em xlsx e json --- ###  

    viagens_gps_classificadas.to_excel('../data/output/amostra_classificada.xlsx', index=False)

    log_info('Arquivo com os status exportado com sucesso em xlsx no diretório data/output.')
    
    viagens_gps_classificadas.to_json('../data/output/amostra_classificada.json')
    
    log_info('Arquivo com os status exportado com sucesso em json no diretório data/output.')
        
        
    ### --- 9.3 Gerar um resumo das viagens processadas pelo algoritmo --- ###  
    
    log_info('Relatório da execução do algoritmo:.')
        
    # Criar um dataframe para armazenar os resultados
    tabela = pd.DataFrame(index=['Parecer definido','Parecer indefinido'], columns=['Contagem', 'Porcentagem'])
    
    # Contar as ocorrências de cada caso
    total_rows = len(viagens_gps_classificadas)
    tabela.loc['Parecer indefinido', 'Contagem'] = sum(viagens_gps_classificadas['status'] == 'Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra')
    tabela.loc['Parecer definido', 'Contagem'] = total_rows - tabela.loc['Parecer indefinido', 'Contagem']

    # Calcular as porcentagens
    tabela['Porcentagem'] = (tabela['Contagem'] / total_rows) * 100    
    logging.debug(tabela)
    print(tabela)  
    
    # Checar número de viagens do arquivo raw vs o número de viagens na tabela final do algoritmo:
    if len(amostra) == len(viagens_gps_classificadas):
        log_info("Quantidade de viagens do input igual a quantidade de viagens no output.")
    else:
        log_info("ATENÇÃO: Quantidade de viagens do input DIFERENTE da quantidade de viagens no output.")
       
    log_info('Execução do algoritmo finalizada com sucesso.')
 
        
else: # caso a resposta seja n na pergunta "Deseja continuar? (y/n):"
    log_info('Execução do algoritmo finalizada pelo usuário.')
