
import basedosdados as bd
import pandas as pd

def query_viagem_completa(data, id_veiculo):
    q = f"""
    SELECT
      data,
      id_veiculo,
      servico_informado,
      sentido,
      datetime_partida,
      datetime_chegada
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_completa`
    WHERE
      data IN ({data})
      AND id_veiculo IN ({id_veiculo})
    """   
    dados = bd.read_sql(q, from_file=True)        
    
    return dados

# query viagem_conformidade
def query_viagem_conformidade(data, id_veiculo):
    q = f"""
    SELECT
      data,
      id_veiculo,
      servico_informado,
      sentido,
      datetime_partida,
      datetime_chegada
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_conformidade`
    WHERE
      data IN ({data})
      AND id_veiculo IN ({id_veiculo})
    """   
    dados = bd.read_sql(q, from_file=True)      
    
    return dados


# query gps_sppo
def query_gps(data, id_veiculo):
    q = f"""
    SELECT
      id_veiculo,
      servico,
      timestamp_gps,
      ST_GEOGPOINT(longitude, latitude) as posicao_veiculo_geo
    FROM
      `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    WHERE
      DATA IN ({data})
      AND id_veiculo IN ({id_veiculo})
    """   
    dados = bd.read_sql(q, from_file=True)
    dados = dados.sort_values(by = 'timestamp_gps')
    
    return dados

# query registros_status_viagem

def query_gps_status(data, id_veiculo):
    q = f"""
    SELECT
      id_veiculo,
      servico_informado as servico,
      timestamp_gps,
      posicao_veiculo_geo,
      status_viagem      
    FROM
      `rj-smtr.projeto_subsidio_sppo.registros_status_viagem`
    WHERE
      DATA IN ({data})
      AND id_veiculo IN ({id_veiculo})
    """   
  
    dados = bd.read_sql(q, from_file=True)        
    dados = dados.sort_values(by = 'timestamp_gps')
    
    return dados
  
  
# query shape - viagem planejada
def query_shape(data, servico):
    q = f"""
    SELECT
    shape_id,
    shape,
    start_pt,
    end_pt
    FROM
      `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
    WHERE
      DATA IN ({data})
      AND servico IN ({servico})
    """   
    dados = bd.read_sql(q, from_file=True)
        
    return dados
