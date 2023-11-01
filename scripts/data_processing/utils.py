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

    # 1 - Mostrar o total de recursos avaliados
    total_rows = len(viagens_gps_classificadas)
    
    print('Total de recursos classificadas analisados pelo algoritmo:', total_rows)
    
       
    # 2 - Contar o número de ocorrências em que o algoritmo foi capaz de gerar um parecer
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
    
    
    # 3 - Verificar se o recurso foi deferido ou indeferido    
    status_deferido = 'Viagem deferida após o reprocessamento'
    status_indeferidos = ['Viagem identificada e já paga', 'Viagem indeferida']

    # Calculando a contagem para recursos deferidos
    condicao_deferido = (viagens_gps_classificadas['status'] == status_deferido)
    contagem_deferido = sum(condicao_deferido)
    tabela.loc['Recursos deferidos', 'Contagem'] = contagem_deferido

    # Calculando a contagem para recursos indeferidos
    contagem_indeferido = 0
    for status in status_indeferidos:
        condicao = (viagens_gps_classificadas['status'] == status)
        contagem = sum(condicao)
        contagem_indeferido += contagem
    tabela.loc['Recursos indeferidos', 'Contagem'] = contagem_indeferido

    # Calculando a soma total das categorias de interesse
    soma_categorias = contagem_deferido + contagem_indeferido

    # Calculando o percentual
    tabela.loc['Recursos deferidos', 'Porcentagem'] = (contagem_deferido / soma_categorias) * 100
    tabela.loc['Recursos indeferidos', 'Porcentagem'] = (contagem_indeferido / soma_categorias) * 100

    # Exibindo o resultado
    log_info(tabela.loc[['Recursos deferidos', 'Recursos indeferidos']])


    # 4 - Listar tipos de casos segundo a coluna "observacao"
    
    categorias_interesse = viagens_gps_classificadas['observacao'].dropna().unique().tolist()

    for categoria in categorias_interesse:
        tabela.loc[categoria, 'Contagem'] = 0
        tabela.loc[categoria, 'Porcentagem'] = 0

    # Calculando a contagem para cada categoria de interesse
    for categoria in categorias_interesse:
        condicao = (viagens_gps_classificadas['observacao'] == categoria)
        contagem = sum(condicao)
        tabela.loc[categoria, 'Contagem'] = contagem

    soma_categorias = tabela.loc[categorias_interesse, 'Contagem'].sum()

    # Verificando se a soma das categorias de interesse é zero para evitar divisão por zero
    if soma_categorias > 0:
        # Calculando o percentual
        for categoria in categorias_interesse:
            tabela.loc[categoria, 'Porcentagem'] = (tabela.loc[categoria, 'Contagem'] / soma_categorias) * 100
    else:
        log_info("A soma das categorias de interesse é zero, não é possível calcular a porcentagem.")

    # Exibindo o resultado
    log_info(tabela.loc[categorias_interesse])
    
    
    
    # 5 - Checar número de viagens do arquivo raw vs o número de viagens na tabela final do algoritmo:
    if len(amostra) == len(viagens_gps_classificadas):
        log_info("Check: OK. Quantidade de viagens do input igual a quantidade de viagens no output.")
    else:
        log_info("Check: ATENÇÃO. Quantidade de viagens do input DIFERENTE da quantidade de viagens no output.")
    
        