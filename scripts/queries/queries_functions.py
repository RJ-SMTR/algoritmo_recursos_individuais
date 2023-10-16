
### --- 1. Carregar bibliotecas --- ###
import basedosdados as bd
import pandas as pd



### --- 2. Consultar do Big Query as viagens completas --- ###

def query_viagem_completa(data, id_veiculo, reprocessed=False):
    
    """
    Faz a consulta no Big Query na tabela de viagem_completa em produção ou na versão reprocessada em dev
    com a alteração do serviço no sinal de GPS para casos antes de 16/11/2022.
        
    """     
    
    table = f"""
    `rj-smtr.projeto_subsidio_sppo.viagem_completa` 
    WHERE 
      data IN ({data})
      AND SUBSTRING(id_veiculo, 2) IN ({id_veiculo})
      """
    if reprocessed:
      table = "`rj-smtr-dev.projeto_subsidio_sppo_recursos_reprocessado.viagem_completa`"
    
    q = f"""
    SELECT
      data,
      SUBSTRING(id_veiculo, 2) as id_veiculo,
      servico_informado,
      sentido,
      datetime_partida,
      datetime_chegada
    FROM
      {table}
    """   
    dados = bd.read_sql(q, from_file=True)        
    
    if dados.empty:
        print("Não foram encontradas viagens completas.")
    else:
        pass
  
    return dados


### --- 3. Consultar do Big Query as viagens conformidade --- ###

    """
    Faz a consulta no Big Query na tabela de viagem_conformidade em produção ou na versão reprocessada em dev
    com a alteração do serviço no sinal de GPS para casos antes de 16/11/2022.
        
    """   
def query_viagem_conformidade(data, id_veiculo, reprocessed=False):
  
    table = f"""
    `rj-smtr.projeto_subsidio_sppo.viagem_conformidade` 
    WHERE 
      data IN ({data})
      AND SUBSTRING(id_veiculo, 2) IN ({id_veiculo})
      """
    if reprocessed:
      table = "`rj-smtr-dev.projeto_subsidio_sppo_recursos_reprocessado.viagem_conformidade`"
      
    q = f"""
    SELECT
      data,
      SUBSTRING(id_veiculo, 2)  as id_veiculo,
      servico_informado,
      sentido,
      datetime_partida,
      datetime_chegada
    FROM
      {table}
    """   
    dados = bd.read_sql(q, from_file=True)      
    
    if dados.empty:
        print("Não foram encontradas viagens em conformidade.")
    else:
        pass
    return dados


### --- 4. Consultar do Big Query os dados de GPS --- ###



def query_gps(df_conditions):
    """
    Realiza uma query SQL nos dados de GPS para cada viagem do dataframe inserido como parâmetro.

    Parâmetros:
        df_conditions (pd.DataFrame): DataFrame contendo as colunas necessárias para 
                                      criar as condições SQL.

    Retorna:
        pd.DataFrame: DataFrame contendo os resultados da query SQL.
    """
    # Criando as condições SQL diretamente dentro da função.
    conditions = []
    for _, row in df_conditions.iterrows():
        condition = (
            f"(DATA = '{row['data']}' "
            f"AND SUBSTRING(id_veiculo, 2) = '{row['id_veiculo_amostra']}' "
            f"AND timestamp_gps BETWEEN '{row['datetime_partida_amostra']}' "
            f"AND '{row['datetime_chegada_amostra']}')"
        )
        conditions.append(condition)
    
    sql_conditions = " OR ".join(conditions)

    # Query SQL usando as condições geradas.
    q = f"""
    SELECT
      SUBSTRING(id_veiculo, 2) as id_veiculo,
      servico,
      timestamp_gps,
      ST_GEOGPOINT(longitude, latitude) as posicao_veiculo_geo
    FROM
      `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    WHERE
      {sql_conditions}
    """   
    # Suponho que `bd.read_sql` seja uma função definida em seu ambiente.
    dados = bd.read_sql(q, from_file=True)
    dados = dados.sort_values(by='timestamp_gps')
    
    if dados.empty:
        print("Não foram encontrados dados de GPS.")
    else:
        pass
    
    return dados
  
### --- 5. Consultar do Big Query os dados de viagem planejada --- ###   
  
# query shape - viagem planejada
def query_shape(data, servico):
    q = f"""
    SELECT
    shape_id,
    shape,
    servico,
    start_pt,
    end_pt
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
    WHERE
      DATA IN ({data})
      AND servico IN ({servico})
    """   
    dados = bd.read_sql(q, from_file=True)
    if dados.empty:
        print("Não foram encontrados dados do planejado.")
    else:
        pass    
    return dados



### --- 6. Verificar o tipo de servico (circular ou ida e volta) --- ###


def query_tipo_linha(data, servico, include_sentido_shape=False):
  # usar include_sentido_shape para não pegar dados de sentido_shape
    # Verificando se deve incluir a coluna 'sentido_shape' na query.
    select_clause = "data, servico, sentido"
    if include_sentido_shape:
        select_clause += ", sentido_shape"
    
    # Construindo a query.
    q = f"""
    SELECT
    {select_clause}
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
    WHERE
      DATA IN ({data})
      AND servico IN ({servico})
    """   
    # Executando a query e retornando os dados.
    dados = bd.read_sql(q, from_file=True)
    if dados.empty:
        print("Não foram encontrados dados do planejados para o dia.")
    else:
        pass    
    return dados
