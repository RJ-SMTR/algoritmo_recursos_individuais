from graphs import *

def automate_map(row, df_check, dados_shape, viagens_gps_to_map):
    """
    Automatiza e salva em HTML os mapas das viagens.
    
    """         
   
    filtered_df = df_check[
        (df_check['id_veiculo'] == row['id_veiculo_amostra']) &
        (df_check['timestamp_gps'] >= row['datetime_partida_amostra']) &
        (df_check['timestamp_gps'] <= row['datetime_chegada_amostra'])
    ]

    unique_veiculos = filtered_df['id_veiculo'].unique()

    if not filtered_df.empty and row['status'] == 'Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra':
        for veiculo in unique_veiculos:
            gps_mapa = filtered_df[filtered_df['id_veiculo'] == veiculo]
            servico_do_veiculo = gps_mapa['servico'].iloc[0]
            shape_mapa = dados_shape[dados_shape['servico'] == servico_do_veiculo]

            map = create_trip_map(gps_mapa, shape_mapa)
            partida = viagens_gps_to_map[viagens_gps_to_map['id_veiculo_amostra'] == veiculo]['datetime_partida_amostra'].iloc[0]
            hora_formatada = partida.strftime('%Hh%M')
            filename = f"./../data/output/maps/{veiculo} {partida.date()} {hora_formatada}.html"
            map.save(filename)
            print(f"Gerando mapa: {veiculo} {partida.date()} {hora_formatada}")

    return "Os mapas foram gerados."  

          