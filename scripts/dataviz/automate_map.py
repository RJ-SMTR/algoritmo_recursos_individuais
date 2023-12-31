from graphs import *
from utils import *
from treat_data import *
import pandas as pd

def automate_map(viagens_gps_classificadas, status_list):
    """
    Esta função gera mapas no formato HTML para as viagens não identificadas, mas que existe sinal de GPS com o serviço correto.
    
    :param viagens_gps_classificadas: DataFrame com as viagens classificadas
    :param status_list: Lista de strings contendo os status desejados para a geração de mapas
    """   
    log_info('Iniciando a etapa de geração dos mapas em HTML.')  
    
    log_info('Acessando dados de GPS e do trajeto.') 
    dados_shape = pd.read_csv('../data/cache/dados_gps_shape.csv')
    dados_shape['servico'] = dados_shape['servico'].astype(str)   

    dados_gps = pd.read_csv('../data/cache/dados_gps.csv')
    dados_gps = treat_gps(dados_gps)
      
    # Filtrar as viagens pelo status desejado
    viagens_para_mapa = viagens_gps_classificadas[viagens_gps_classificadas['status'].isin(status_list)]
    
    if not viagens_para_mapa.empty:
        for _, row in viagens_para_mapa.iterrows():
            # Aqui você pode continuar com a lógica de geração do mapa
            # Filtrar dados_gps
            gps_map = dados_gps[
                (dados_gps['id_veiculo'] == row['id_veiculo_amostra']) &
                (dados_gps['timestamp_gps'] >= row['datetime_partida_amostra']) &
                (dados_gps['timestamp_gps'] <= row['datetime_chegada_amostra'])
            ]
            
            # Filtrar dados_shape
            shape_map = dados_shape[
                (dados_shape['data'] == row['data']) &
                (dados_shape['servico'] == row['servico_amostra'])
            ]
            
            # Criar mapa
            map_obj = create_trip_map(gps_map, shape_map)
            
            # Salvar arquivo
            veiculo_label = row['id_veiculo_amostra']
            partida_label = row['datetime_partida_amostra'].date()
            hora_label = row['datetime_partida_amostra'].strftime('%Hh%M')
            filename = f"./../data/output/maps/{veiculo_label} {partida_label} {hora_label}.html"
            
            map_obj.save(filename)
            log_info(f"Gerando mapa: {veiculo_label} {partida_label} {hora_label}")  
        
        log_info('Mapas em HTML gerados com sucesso e disponíveis no diretório data/output/maps.')        
                    
    else:
        log_info('Não existem viagens classificadas nos status em que os mapas precisam ser gerados.')    

