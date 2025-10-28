# main.py
"""
Orquestrador Principal do Pipeline de Previsão de Temperatura.
"""
import os
import pandas as pd

# Importações de pacote: "do pacote 'rail_predictor', importe os módulos..."
from rail_predictor.config import Config
from rail_predictor.data_io import load_locations, load_history, save_output
from rail_predictor.api_client import fetch_weather_data_parallel
from rail_predictor.processing import run_processing_pipeline

def main():
    """
    Executa o pipeline de ETL de ponta a ponta.
    """
    print(f"\n--- 1/5: Carregando Dados de Localização ---")
    # A função load_locations já sabe o caminho do config
    locations_df = load_locations() 
    if locations_df.empty:
        print("❌ Encerrando: Não foi possível carregar os locais.")
        return
    print(f"✅ {len(locations_df)} locais únicos carregados.")

    
    print(f"\n--- 2/5: Preparando Parâmetros da API ---")
    api_params = {
        'hourly': Config.API_HOURLY_VARS,
        'timezone': Config.API_TIMEZONE
    }
    
    # A função os.path.exists checa o arquivo de saída definido no Config
    if not os.path.exists(Config.OUTPUT_FILE):
        print("Histórico não encontrado. Buscando 7 dias de histórico.")
        api_params['past_days'] = 7
    else:
        print("Histórico encontrado. Buscando 3 dias de previsão.")
        api_params['forecast_days'] = 3
        
    print(f"\n--- 3/5: Coletando Dados da API (Paralelo) ---")
    new_df = fetch_weather_data_parallel(locations_df, api_params)
    if new_df.empty:
        print("❌ Encerrando: Nenhum dado foi obtido da API.")
        return
    print(f"\n✅ Dados da API coletados. {len(new_df)} linhas recebidas.")
    
    print(f"\n--- 4/5: Processando Dados e Aplicando Modelo ---")
    new_processed_df = run_processing_pipeline(new_df)
    
    new_data_dates = new_processed_df['datetime'].dt.date.unique()
    clean_history_df = load_history(new_data_dates=new_data_dates)
    
    final_df = pd.concat([clean_history_df, new_processed_df], ignore_index=True)
    final_df.sort_values(by=[Config.ID_COLUMN, 'datetime'], inplace=True)
    print("✅ Modelo de inércia térmica aplicado e dados combinados.")

    print(f"\n--- 5/5: Salvando Histórico Atualizado ---")
    # A função save_output já sabe o caminho do config
    save_output(final_df) 

if __name__ == "__main__":
    main()