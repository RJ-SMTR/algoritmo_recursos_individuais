import logging
import pandas as pd
from datetime import timedelta, datetime

### --- 1 - Config do arquivo de log ---###
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"./log/log_{current_time}.txt"
logging.basicConfig(
    filename=log_filename, 
    encoding='utf-8', 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # This format excludes milliseconds
)

### --- Função de log --- ###
def log_info(message: str):
    """
    Mostra a mensagem do log no console e salva no arquivo de log.
    """
    logging.debug(message)
    print(message)  
    
def export_data(viagens_gps_classificadas: pd.DataFrame) -> pd.DataFrame:
    
    protocolo = viagens_gps_classificadas.pop('protocolo')
    viagens_gps_classificadas.insert(0, 'protocolo', protocolo)
        
    viagens_gps_classificadas.to_excel('../data/output/amostra_classificada.xlsx', index=False)
    log_info('Arquivo com os status exportado com sucesso em xlsx no diretório data/output.')
    viagens_gps_classificadas.to_json('../data/output/amostra_classificada.json')
    log_info('Arquivo com os status exportado com sucesso em json no diretório data/output.')
    
def generate_report(viagens_gps_classificadas: pd.DataFrame, amostra: pd.DataFrame) -> pd.DataFrame:
    
    log_info('Relatório da execução do algoritmo:')

    # Criar um dataframe para armazenar os resultados
    tabela = pd.DataFrame(index=['Parecer definido','Parecer indefinido'], columns=['Contagem', 'Porcentagem'])

    # Contar as ocorrências de cada caso
    total_rows = len(viagens_gps_classificadas)
    
    print('Total de recursos classificadas analisados pelo algoritmo:', total_rows)
    
    # Contar o número de ocorrências de ambas as condições
    condicao = (viagens_gps_classificadas['status'] == 'Viagem não classificada pelo algoritmo')
    contagem_total = sum(condicao)

    # Atualizar a contagem na linha "Parecer indefinido"
    tabela.loc['Parecer indefinido', 'Contagem'] = contagem_total

    # Calcular a contagem em "Parecer definido"
    tabela.loc['Parecer definido', 'Contagem'] = total_rows - contagem_total

    # Calcular as porcentagens
    tabela['Porcentagem'] = (tabela['Contagem'] / total_rows) * 100
    logging.debug(tabela)
    print(tabela)  

    # Checar número de viagens do arquivo raw vs o número de viagens na tabela final do algoritmo:
    if len(amostra) == len(viagens_gps_classificadas):
        log_info("Check: OK. Quantidade de viagens do input igual a quantidade de viagens no output.")
    else:
        log_info("Check: ATENÇÃO. Quantidade de viagens do input DIFERENTE da quantidade de viagens no output.")
    
        