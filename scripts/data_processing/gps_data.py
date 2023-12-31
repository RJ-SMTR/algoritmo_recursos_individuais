import argparse
import pandas as pd
import sys

from utils import *
from queries_functions import *
from categorize_trips import *
from treat_data import *

parser = argparse.ArgumentParser(description="Execute o script run.py com opções.")

# permite usar o cache para não baixar novamente os dados da última consulta no big query
parser.add_argument('--cache', action='store_true', help="Ative o cache")
args = parser.parse_args()
cache = "on" if args.cache else "off"


def gps_data(todas_as_viagens: pd.DataFrame) -> pd.DataFrame:
    
    """
    Esta função realiza o acesso, o tratamento e a classificação dos dados de GPS.   
    """

    linhas_nan = todas_as_viagens[pd.isna(todas_as_viagens['status'])]

    datas_unicas = len(linhas_nan['data'].drop_duplicates())
    estimativa_custo = (datas_unicas * 400) / 1000 

    proceed = False
    
    # Confirmar se a query dos dados de GPS deve ser feita
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
        
        ### --- 7.1 Acessar os sinais de GPS --- ###
        if args.cache:
            dados_gps = pd.read_csv('../data/cache/dados_gps.csv') 
        
        else:
            dados_gps = query_gps(linhas_nan)
            dados_gps.to_csv('../data/cache/dados_gps.csv', index = False)

        log_info('Acesso aos sinais de GPS concluído com sucesso.')
        
        ### --- 7.2 Tratar os sinais de GPS --- ###
        dados_gps = treat_gps(dados_gps)
        log_info('Tratamento de dados de GPS concluído com sucesso.')


        ### --- 7.3 Comparar amostra com os sinais de GPS --- ###
        viagens_gps_classificadas_nan = todas_as_viagens[todas_as_viagens['status'].isna()]
        viagens_gps_classificadas_not_nan = todas_as_viagens[todas_as_viagens['status'].notna()]    

        # Aplicar a função quando o status da viagem for nan
        results = viagens_gps_classificadas_nan.apply(lambda row: check_gps(row, dados_gps), axis=1)
        viagens_gps_classificadas_nan['status'] = results.apply(lambda x: x[0])
        viagens_gps_classificadas_nan['servico_apurado'] = results.apply(lambda x: x[1])

        # Juntar tabela com status nan e status não nan
        viagens_gps_classificadas = pd.concat([viagens_gps_classificadas_nan, viagens_gps_classificadas_not_nan], ignore_index=True)

        # Quando houver sinal de GPS para um serviço além do serviço da amostra, deixar apenas o
        # serviço diferente da amostra em serviço apurado (para realizar o reprocessamento de forma correta)
        
        def remover_valor(row): 
            if pd.isna(row['servico_apurado']) or pd.isna(row['servico_amostra']):
                return ""
            elif str(row['servico_amostra']) != str(row['servico_apurado']):
                valor_a_remover = str(row['servico_amostra']) 

                # Divide o servico_apurado em partes, separadas por ','
                partes = row['servico_apurado'].split(',')

                # Remove o valor a ser removido de cada parte, verificando condições
                partes_modificadas = [p.strip().replace(valor_a_remover, '') 
                                    if (
                                        p.strip()[0].isdigit() or 
                                        p.strip() == valor_a_remover
                                    ) else p.strip() 
                                    for p in partes]

                # Junta as partes modificadas de volta em uma string e retorna
                # Garantindo que não há espaços e vírgulas extras
                resultado = ', '.join([p for p in partes_modificadas if p]).strip()
                
                # Removendo possíveis vírgulas extras no final
                return resultado.rstrip(', ')
            else:
                return row['servico_apurado']
    

        viagens_gps_classificadas['servico_apurado'] = viagens_gps_classificadas.apply(remover_valor, axis=1)

        viagens_gps_classificadas.to_excel('../data/treated/viagens_gps_classificadas.xlsx', index = False)
        
        return viagens_gps_classificadas 

    else: # caso a resposta seja n na pergunta "Deseja continuar? (y/n):"
        log_info('Execução do algoritmo finalizada pelo usuário.')
        sys.exit()