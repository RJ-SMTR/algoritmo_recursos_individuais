### --- 1. Configurar bibliotecas, diretórios, flags e arquivo de log --- ###
modelo_versao = 'v0.1'

### --- 1.1 Bibliotecas ---###
import argparse
import os
import pandas as pd
import plotly.graph_objects as go
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

### --- 3. VIAGENS COMPLETAS --- ###
# Compara os recursos com a tabela de viagem_completa.


### --- 3.1 Acessar dados das viagens completas --- ###
if args.cache:
    viagem_completa = pd.read_csv('../data/cache/viagem_completa.csv')   
    
else:
    # Datas e veículos presentes na amostra
    id_veiculo_query = query_values(amostra_tratada, 'id_veiculo')
    data_query = query_values(amostra_tratada, 'data')
    
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

### --- 5.1 Acessar dados das viagens conformidade --- ###

# Acessar os dados
if args.cache:
    viagem_conformidade = pd.read_csv('../data/cache/viagem_conformidade.csv')  
else:
    # Verificar os dias e veiculos ainda não classificados
    linhas_nan = viagem_completa_reprocessada[pd.isna(viagem_completa_reprocessada['status'])]
    data_query = query_values(linhas_nan, 'data')
    id_veiculo_query = query_values(linhas_nan, 'id_veiculo_amostra')
    
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

viagens_gps_classificadas = gps_data(viagens_conformidade_classificadas)
    

## --- 7.1 Verificar se houveram sinais de GPS no entorno dos pontos inicial e final --- ###

viagens_gps_classificadas = check_start_end_gps(viagens_gps_classificadas)


### --- 8. Criar mapas em HTML  --- ###
# Esta etapa cria mapas em HTML para as viagens que tiveram sinais de GPS encontrados, mas não foram
# classificadas nas etapas anteriores.

# automate_map(viagens_gps_classificadas)
# Lista de status desejados
status_desejados = [
    "Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra",
    "Pós-reprocessamento: Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra"
]

# Chamar a função com a lista de status
automate_map(viagens_gps_classificadas, status_desejados)

### --- 9. Ajustes finais  --- ###

### --- 9.1 Alterações na tabela final --- ###
# Remover as colunas flag_reprocessamento e circular_dividida caso elas existam.
columns_to_drop = ['flag_reprocessamento', 'circular_dividida']
for column in columns_to_drop:
    if column in viagens_gps_classificadas.columns:
        viagens_gps_classificadas = viagens_gps_classificadas.drop(column, axis=1)

viagens_gps_classificadas['versao_modelo'] = modelo_versao    

### --- 9.2 Colocar as categorias simplificadas de status --- ###
viagens_gps_classificadas = simplified_status(viagens_gps_classificadas)


### --- 9.2 Exportar tabela final com status em xlsx e json --- ###  
export_data(viagens_gps_classificadas)

### --- 9.3 Gerar um resumo das viagens processadas pelo algoritmo --- ###  
generate_report(viagens_gps_classificadas, amostra)
    
log_info('Execução do algoritmo finalizada com sucesso.')