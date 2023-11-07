from prefect import Flow, Parameter
from tasks import *

with Flow("Classificar recursos") as flow:

    amostra = run_sample()

    viagem_completa_classificada = run_complete_trips(amostra)

    viagem_completa_reprocessada = run_reprocessed_trips(viagem_completa_classificada)

    viagem_conformidade_classificada = run_conformity_trips(viagem_completa_reprocessada)

    viagens_circulares_classificadas = run_circular_trips(viagem_conformidade_classificada, amostra)

    viagens_check_gps = run_gps_data(viagens_circulares_classificadas)

    create_html_maps(viagens_check_gps)

    final_adjusts(viagens_check_gps, amostra)