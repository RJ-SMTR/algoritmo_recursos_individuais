## --- Treatment functions --- ###

import pandas as pd

def treat_sample(dados: pd.DataFrame) -> pd.DataFrame:
    
    dados['servico'] = dados['servico'].astype(str)
    dados['data'] = dados['data'].astype(str)
    dados['id_veiculo'] = dados['id_veiculo'].astype(str)
    dados['hora_inicio'] = dados['hora_inicio'].astype(str)
    dados['datetime_partida'] = pd.to_datetime(dados['data'] + ' ' + dados['hora_inicio'])
    dados['hora_fim'] = dados['hora_fim'].astype(str)
    dados['datetime_chegada'] = pd.to_datetime(dados['data'] + ' ' + dados['hora_fim'])
    
    return dados


def treat_trips(dados: pd.DataFrame) -> pd.DataFrame:
    
    dados['servico_informado'] = dados['servico_informado'].astype(str)
    dados['data'] = dados['data'].astype(str)
    dados['id_veiculo'] = dados['id_veiculo'].astype(str)
    dados['datetime_partida'] = pd.to_datetime(dados['datetime_partida'])
    dados['datetime_chegada'] = pd.to_datetime(dados['datetime_chegada']) 
    dados = dados.sort_values(by = 'datetime_partida')
    
    return dados
    

def treat_gps(dados: pd.DataFrame) -> pd.DataFrame:
    
    dados['servico'] = dados['servico'].astype(str)
    dados['id_veiculo'] = dados['id_veiculo'].astype(str)
    dados['timestamp_gps'] = pd.to_datetime(dados['timestamp_gps'])
    
    return dados



    # tratar viagem completa 
    # dados = dados.sort_values(by = 'datetime_partida')
    # dados['servico_informado'] = dados['servico_informado'].astype(str)
    # dados['data'] = dados['data'].astype(str)
    # dados['id_veiculo'] = dados['id_veiculo'].astype(str)
    # dados['datetime_partida'] = pd.to_datetime(dados['datetime_partida'])
    # dados['datetime_chegada'] = pd.to_datetime(dados['datetime_chegada'])   
    
    
    
    # tratar conformidade
    # dados = dados.sort_values(by = 'datetime_partida')
    # dados['servico_informado'] = dados['servico_informado'].astype(str)
    # dados['data'] = dados['data'].astype(str)
    # dados['id_veiculo'] = dados['id_veiculo'].astype(str)
    # dados['datetime_partida'] = pd.to_datetime(dados['datetime_partida'])
    # dados['datetime_chegada'] = pd.to_datetime(dados['datetime_chegada'])   
    
    
    
    
    # tratar gps
    dados['servico'] = dados['servico'].astype(str)
    dados['id_veiculo'] = dados['id_veiculo'].astype(str)
    dados['timestamp_gps'] = pd.to_datetime(dados['timestamp_gps'])
    
    
    # inserir após as queries ou leituras de csv