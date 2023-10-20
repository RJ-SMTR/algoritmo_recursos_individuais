import argparse
import numpy as np
import pandas as pd
from datetime import timedelta, datetime
from utils import *

from categorize_trips import *
from queries_functions import *
from treat_data import *

parser = argparse.ArgumentParser(description="Execute o script run.py com opções.")

parser.add_argument('--cache', action='store_true', help="Ative o cache")
args = parser.parse_args()
cache = "on" if args.cache else "off"


def reprocess_trips(dados: pd.DataFrame) -> pd.DataFrame:
    """
    Gera o arquivo no formato csv para reprocessamento caso a viagem seja antes de 16/11/2022,
    o valor da coluna flag_reprocessamento seja 1 e a viagem ainda não tenha sido paga.
    """   
    completas_na = dados[dados['status'].isna()]

    # Verificar se a soma da coluna 'flag_reprocessamento' para as linhas filtradas é maior que 0
    
    if completas_na['flag_reprocessamento'].sum() > 0:
        log_info('Iniciando o reprocessamento de viagens.')       
        
        id_veiculo = query_values(completas_na, 'id_veiculo_amostra')
        data = query_values(completas_na, 'data')  
        
        
        dados['data'] = pd.to_datetime(dados['data'])
        data_limite = datetime.strptime('2022-11-16', '%Y-%m-%d')

        condicao = (
        (dados['flag_reprocessamento'] == 1) & 
        (dados['data'] <= data_limite) & 
        ~(dados['status'].isin([
            "Viagem identificada e já paga", 
            "Viagem identificada e já paga para serviço diferente da amostra"
        ]))
        )      

        linhas_condicao = dados[condicao]

        demais_linhas = dados[~condicao]

        # Verificar se há linhas que atendem à condição
        if not linhas_condicao.empty:
            print("As seguintes viagens atenderam à condição de reprocessamento do serviço:", linhas_condicao)           
                    
            linhas_condicao.to_csv('./../../queries-rj-smtr/data/reprocessar.csv', index = False)
            
            # Limpar os campos que serão reprocessados:
            columns_to_na = [
            "status", "data_apurado", "id_veiculo_apurado", "servico_apurado",
            "sentido_apurado", "datetime_partida_apurado", "datetime_chegada_apurado"
            ]   

            # Substituindo todos os valores nas colunas selecionadas por NaN
            linhas_condicao.loc[:, columns_to_na] = np.nan
            
        input("Execução pausada. Execute o modelo no DBT e pressione enter para continuar...")        

        # Reprocessar 

        # Baixar e classificar viagens completas reprocessadas
        if args.cache:
            viagem_completa_reprocessada = pd.read_csv('../data/cache/viagem_completa_reprocessada.csv')   
            
        else:
            viagem_completa_reprocessada = query_viagem_completa(data, id_veiculo, reprocessed=True)
            viagem_completa_reprocessada.to_csv('../data/cache/viagem_completa_reprocessada.csv', index = False)
            
        viagem_completa_reprocessada = treat_trips(viagem_completa_reprocessada)
        
        
        linhas_condicao = check_trips(linhas_condicao, viagem_completa_reprocessada,
                                    "Viagem deferida com novo serviço")
        
        linhas_condicao.to_excel('../data/treated/viagem_completa_reprocessada.xlsx', index = False)

        # Baixar e classificar viagens conformidade reprocessadas
        
        if args.cache:
            viagem_conformidade_reprocessada = pd.read_csv('../data/cache/viagem_conformidade_reprocessada.csv')   
            
        else:
            viagem_conformidade_reprocessada = query_viagem_conformidade(data, id_veiculo, reprocessed=True)
            viagem_conformidade_reprocessada.to_csv('../data/cache/viagem_conformidade_reprocessada.csv', index = False)
            
        viagem_conformidade_reprocessada = treat_trips(viagem_conformidade_reprocessada)
        
        
        linhas_condicao = check_trips(linhas_condicao, viagem_conformidade_reprocessada,
                                        "Viagem indeferida - Não atingiu % de GPS ou trajeto correto após o reprocessamento")
        
        viagem_completa_reprocessada = pd.concat([linhas_condicao, demais_linhas], ignore_index=True)
        
        viagem_completa_reprocessada.to_excel('../data/treated/viagem_completa_reprocessada.xlsx', index = False)
    

        return viagem_completa_reprocessada
    
    else:
        log_info('Não existem casos de reprocessamento.')
        return dados
        