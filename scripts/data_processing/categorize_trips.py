### --- 1. Carregar bibliotecas --- ###

import pandas as pd
import numpy as np


### --- 2. Função de verificação de viagens sobrepostas --- ###

def remove_overlapping_trips(df: pd.DataFrame) -> pd.DataFrame:
    
    """
    Classifica o status das viagens da amostra como "Viagem inválida - sobreposição de viagem" nos casos 
    em que um mesmo veículo tem duas viagens no mesmo intervalo de tempo.
    
    Parâmetros:
    df (dataframe): Dataframe contendo a amostra.
    
    Retorna:
    dataframe: Um dataframe com a coluna status preenchida para casos de viagens sobrepostas.
    
    Exemplos:
    >>> remove_overlapping_trips(amostra)
    
    Notas:
    Caso o mesmo veículo inicie uma viagem no mesmo minuto do término da viagem anterior, não é considerada sobreposição de viagem.
    """
    
    df_processed = df.copy()
    
    # Converter as colunas para o tipo datetime
    df_processed['datetime_partida'] = pd.to_datetime(df_processed['datetime_partida'])
    df_processed['datetime_chegada'] = pd.to_datetime(df_processed['datetime_chegada'])
    df_processed['id_veiculo'] = df_processed['id_veiculo'].astype(str)
    df_processed['servico'] = df_processed['servico'].astype(str)
    df_processed['status'] = np.nan

    # Verificação de sobreposição
    for index, row in df_processed.iterrows():
        mask = (
            (df_processed['id_veiculo'] == row['id_veiculo']) & 
            (df_processed['datetime_partida'] <= row['datetime_chegada']) & 
            (df_processed['datetime_chegada'] >= row['datetime_partida']) &
            (df_processed.index != index)
        )
    
        overlapping_rows = df_processed[mask]
    
        if overlapping_rows.shape[0] > 0:
            for overlapping_index in overlapping_rows.index:
                if df_processed.at[overlapping_index, 'datetime_partida'] == row['datetime_partida']:
                    if overlapping_index > index:
                        df_processed.at[overlapping_index, 'status'] = 'Viagem duplicada na amostra'
                elif df_processed.at[overlapping_index, 'datetime_partida'] == row['datetime_chegada']:
                    df_processed.at[index, 'status'] = np.nan
                elif df_processed.at[overlapping_index, 'datetime_partida'] < row['datetime_chegada'] and df_processed.at[overlapping_index, 'datetime_chegada'] > row['datetime_partida']:
                    if overlapping_index > index:
                        df_processed.at[overlapping_index, 'status'] = 'Viagem duplicada na amostra'
                        
    return df_processed


### --- 3. Função de classificação de viagens --- ###

def check_trips(amostra: pd.DataFrame, query_trip_table: pd.DataFrame, status: str) -> pd.DataFrame:
        
    """
    Verifica quais viagens do dataframe query_trip_table têm o datetime_partida dentro de um intervalo
    de + ou - 10 minutos do datetime do dataframe amostra. Para viagens com duração menor do que 10 minutos,
    o intervalo de comparação é de + ou - 5 minutos. A função também classifica o status de acordo com a string
    que é passada no argumento status da função.
    
    Parâmetros:
    amostra (dataframe): Dataframe contendo a amostra.
    query_trip_table (dataframe): Dataframe contendo viagens completas ou viagem conformidade.
    status(str): Valor que deve ser atribuido na coluna status caso a condição seja satisfeita.
    
    Retorna:
    dataframe: Um dataframe com a coluna status preenchida para casos de viagens encontradas.
    
    Exemplos:
    >>> check_trips(amostra, viagem_completa, "Viagem circular identificada e já paga")
    """        
        
    # Identificar colunas que iniciam com os prefixos abaixo
    for prefix in ['id_veiculo', 'datetime_partida', 'datetime_chegada', 'servico', 'sentido']:
        amostra_cols = [col for col in amostra.columns if col.startswith(prefix)]
        query_cols = [col for col in query_trip_table.columns if col.startswith(prefix)]
        
        # Erro caso a coluna não exista
        if not amostra_cols or not query_cols:
            raise ValueError(f"O DataFrame deve ter colunas que começam com {prefix}")
        
        # Renomear para um nome padrão
        amostra.rename(columns={amostra_cols[0]: prefix}, inplace=True)
        query_trip_table.rename(columns={query_cols[0]: prefix}, inplace=True)  
        
    # Separando linhas que serão classificadas (aquelas em que a coluna status é NaN)
    amostra_nan = amostra[amostra['status'].isna()]
    amostra_not_nan = amostra[~amostra['status'].isna()]
    
    # selecionar apenas as colunas da amostra_nan que serão usadas:
    # Encontre as posições da coluna inicial e final
    start_col = amostra_nan.columns.get_loc('data')
    end_col = amostra_nan.columns.get_loc('status')
    # Selecione as colunas do DataFrame
    amostra_nan = amostra_nan.iloc[:, start_col:end_col + 1]

    # Adicionar uma chave temporária
    amostra_nan['tmp_key'] = amostra_nan['id_veiculo']
    query_trip_table['tmp_key'] = query_trip_table['id_veiculo']

    # Fazer o merge usando a chave temporária
    tabela_comparativa = pd.merge(amostra_nan, query_trip_table, 
                                  on='tmp_key', 
                                  suffixes=('_amostra', '_apurado')) 
    
    
    # Definir o intervalo do join:
    # caso a viagem seja muito curta e dure menos de 10 minutos, o join será feito com uma margem de 5 minutos, e não 10 minutos
    condition = (tabela_comparativa['datetime_chegada_amostra'] - tabela_comparativa['datetime_partida_amostra'] < pd.Timedelta(minutes=10))
    tabela_comparativa['intervalo'] = np.where(condition, 5, 10)
    
    
    # Filtrar os resultados com base no critério do intervalo de tempo
    # condition = (tabela_comparativa['datetime_partida_apurado'] >= (tabela_comparativa['datetime_partida_amostra'] - pd.Timedelta(minutes=intervalo))) & \
    #             (tabela_comparativa['datetime_partida_apurado'] <= (tabela_comparativa['datetime_partida_amostra'] + pd.Timedelta(minutes=intervalo)))
        
    condition = (tabela_comparativa['datetime_partida_apurado'] >= 
                (tabela_comparativa['datetime_partida_amostra'] - 
                pd.to_timedelta(tabela_comparativa['intervalo'], unit="m"))) & \
                (tabela_comparativa['datetime_partida_apurado'] <= 
                (tabela_comparativa['datetime_partida_amostra'] + 
                pd.to_timedelta(tabela_comparativa['intervalo'], unit="m")))

    tabela_comparativa = tabela_comparativa[condition]  
     
    # Remover a chave temporária e outras colunas desnecessárias
    tabela_comparativa.drop(columns=['tmp_key'], inplace=True)
    
    # Atualizar a coluna 'status' baseada nas condições
    condition = (tabela_comparativa['id_veiculo_amostra'] == tabela_comparativa['id_veiculo_apurado']) & \
            (tabela_comparativa['servico_amostra'] == tabela_comparativa['servico_apurado'])

    tabela_comparativa.loc[condition, 'status'] = status
    
    condition = (tabela_comparativa['id_veiculo_amostra'] == tabela_comparativa['id_veiculo_apurado']) & \
            (tabela_comparativa['servico_amostra'] != tabela_comparativa['servico_apurado'])

    tabela_comparativa.loc[condition, 'status'] = status + " para serviço diferente da amostra"
        
    
    # Verificar se existem dados duplicados no cruzamento de dados
    unique_data = tabela_comparativa[['id_veiculo_apurado', 'datetime_partida_apurado']].drop_duplicates()
    
    if tabela_comparativa.shape[0] == unique_data.shape[0]:
        print("Não existem casos duplicados no cruzamento de dados.")
    else:        
        duplicated_rows = tabela_comparativa[tabela_comparativa.duplicated(['id_veiculo_apurado', 'datetime_partida_apurado'])]        
        matching_rows = pd.merge(tabela_comparativa, duplicated_rows[['id_veiculo_apurado', 'datetime_partida_apurado']], 
                                 on=['id_veiculo_apurado', 'datetime_partida_apurado'], how='inner')
        print("\nCasos duplicados encontrados no cruzamento de dados:")
        print(matching_rows)
       
    # formatar tabelas para retornar todas as linhas da amostra que foi inserida
    new_column_names = {
        'id_veiculo': 'id_veiculo_amostra',
        'sentido': 'sentido_amostra',
        'servico': 'servico_amostra',
        'datetime_partida': 'datetime_partida_amostra',
        'datetime_chegada': 'datetime_chegada_amostra'
    }
    
    amostra_not_nan = amostra_not_nan.rename(columns=new_column_names)  

    amostra_nan.drop(columns=['tmp_key'], inplace=True)
    
    amostra_nan = amostra_nan.rename(columns=new_column_names) 
     
    # excluir da amostra_nan aquelas linhas que o status era nan e foram classificadas em tabela_comparativa
    amostra_nan['unique_key'] = amostra_nan['id_veiculo_amostra'].astype(str) + '_' + amostra_nan['datetime_partida_amostra'].astype(str)
    tabela_comparativa['unique_key'] = tabela_comparativa['id_veiculo_amostra'].astype(str) + '_' + tabela_comparativa['datetime_partida_amostra'].astype(str)
    
    
    # Identificando as linhas que estão apenas em amostra_nan e não em tabela_comparativa
    amostra_nan = amostra_nan.loc[~amostra_nan['unique_key'].isin(tabela_comparativa['unique_key'])]
    
    # Removendo a coluna 'unique_key' se não for mais necessária
    amostra_nan.drop(columns=['unique_key'], inplace=True)
    tabela_comparativa.drop(columns=['unique_key'], inplace=True)
                

    final_data = pd.concat([amostra_not_nan, amostra_nan, tabela_comparativa], axis=0).reset_index(drop=True)
    
    final_data['data'] = final_data['datetime_partida_amostra'].dt.date # atribuir coluna data, caso ela não esteja presente

    final_data.drop(['intervalo', 'data_amostra'], axis=1, inplace=True)


    return final_data 



### --- 4. Função de classificação de dados de GPS --- ###

def check_gps(row, df_check):
    """
    Verifica se o veículo teve sinal de GPS no momento da viagem e retorna o status informando
    se ele operou no serviço correto ou se não houve sinal de GPS no momento da viagem.
        
    Parâmetros:
    row: Linha do dataframe da amostra com as viagens não identificadas.
    df_check (dataframe): Dataframe contendo os dados de GPS.   

    """     
    
    # Filtrar df_check pelo id_veiculo e pelo intervalo
    filtered_df = df_check[
        (df_check['id_veiculo'] == row['id_veiculo_amostra']) & 
        (df_check['timestamp_gps'] >= row['datetime_partida_amostra']) & 
        (df_check['timestamp_gps'] <= row['datetime_chegada_amostra'])
    ]
    
    # Identificar serviços
    unique_servicos = filtered_df['servico'].unique()
    servico_apurado = ', '.join(unique_servicos)

    if not filtered_df.empty and np.isnan(row['status']):
        if filtered_df.iloc[0]['servico'] == row['servico_amostra']:
            return ("Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra", servico_apurado)
        else:
            return ("Sinal de GPS encontrado para o veículo operando em serviço diferente da amostra", servico_apurado)
    else:
        return ("Sinal de GPS não encontrado para o veículo no horário da viagem", np.nan)


### --- 5. Função de classificação de viagens circulares --- ###

# Classificar meias viagens da amostra

def check_circular_trip(viagens_circulares_sem_status, viagem_completa, viagem_conformidade):
    """ 
    Esta função deve ser usada apenas nos casos em que a amostra recebida contém dados de uma viagem
    circular, mas está separada em ida e volta. Ou seja, a amostra (viagens_circulares_sem_status)
    deve conter o sentido "I" ou "V", enquanto os dados de viagem_completa e viagem_conformidade
    devem conter o sentido "C". Esta função faz a equivalência entre as viagens da amostra e as
    viagens completas e de conformidade.
    
    Parâmetros:
    viagens_circulares_sem_status (dataframe): Dataframe da amostra com as viagens não identificadas.
    viagem_completa (dataframe): Dataframe contendo os dados das viagens completas.
    viagem_conformidade (dataframe): Dataframe contendo os dados das viagens conformidade.
    """  
    
    # Setar tipos de dados das colunas
    viagens_circulares_sem_status['datetime_partida_amostra'] = pd.to_datetime(viagens_circulares_sem_status['datetime_partida_amostra'])
    viagens_circulares_sem_status['id_veiculo_amostra'] = viagens_circulares_sem_status['id_veiculo_amostra'].astype(str)
    
    viagem_completa['datetime_partida'] = pd.to_datetime(viagem_completa['datetime_partida'])
    viagem_completa['datetime_chegada'] = pd.to_datetime(viagem_completa['datetime_chegada'])
    
    viagem_conformidade['datetime_partida'] = pd.to_datetime(viagem_conformidade['datetime_partida'])
    viagem_conformidade['datetime_chegada'] = pd.to_datetime(viagem_conformidade['datetime_chegada'])        
    
    
    for index, row in viagens_circulares_sem_status.iterrows():
        # Primeira verificação com viagem_completa
        mask_completa = (
            (viagem_completa['id_veiculo'] == row['id_veiculo_amostra']) &
            (viagem_completa['data'] == row['data']) & 
            (viagem_completa['datetime_partida'] <= row['datetime_partida_amostra']) & 
            (viagem_completa['datetime_chegada'] >= row['datetime_partida_amostra'])
        )

        # Se existir uma linha correspondente, atualize o status e copie os valores, depois continue para a próxima iteração
        if viagem_completa[mask_completa].shape[0] > 0:
            viagens_circulares_sem_status.loc[index, 'status'] = "Viagem identificada e já paga"
            matching_row = viagem_completa[mask_completa].iloc[0]
            viagens_circulares_sem_status.loc[index, ['data_apurado', 'id_veiculo_apurado', 'servico_apurado', 
                                            'sentido_apurado', 'datetime_partida_apurado', 
                                            'datetime_chegada_apurado']] = matching_row[['data', 'id_veiculo', 
                                                                                        'servico', 'sentido', 
                                                                                        'datetime_partida', 
                                                                                        'datetime_chegada']].values
            
            
            continue
        
        # Segunda verificação com viagem_conformidade se a primeira falhar
        mask_conformidade = (
            (viagem_conformidade['id_veiculo'] == row['id_veiculo_amostra']) &
            (viagem_conformidade['data'] == row['data']) & 
            (viagem_conformidade['datetime_partida'] <= row['datetime_partida_amostra']) & 
            (viagem_conformidade['datetime_chegada'] >= row['datetime_partida_amostra'])
        )
        
        # Se existir uma linha correspondente, atualize o status e copie os valores
        if viagem_conformidade[mask_conformidade].shape[0] > 0:
            viagens_circulares_sem_status.loc[index, 'status'] = "Viagem inválida - Não atingiu % de GPS ou trajeto correto"
            matching_row = viagem_conformidade[mask_conformidade].iloc[0]
            viagens_circulares_sem_status.loc[index, ['data_apurado', 'id_veiculo_apurado', 'servico_apurado', 
                                            'sentido_apurado', 'datetime_partida_apurado', 
                                            'datetime_chegada_apurado']] = matching_row[['data', 'id_veiculo', 
                                                                                        'servico', 'sentido', 
                                                                                        'datetime_partida', 
                                                                                        'datetime_chegada']].values
            
            
    return viagens_circulares_sem_status
