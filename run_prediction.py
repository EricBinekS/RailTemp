# run_prediction.py
import pandas as pd
import requests
import os
import json
from typing import List

INPUT_JSON_FILE = "coordenadas.json" 
OUTPUT_FILE = "rail_prediction_history.xlsx"

ID_COLUMN = "SB"
LAT_COLUMN = "Lat Decimal"
LON_COLUMN = "Long Decimal"

# API Settings
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
API_HOURLY_VARS = "temperature_2m,precipitation,weather_code,wind_speed_10m,direct_normal_irradiance"

# Model Parameters
RADIATION_TO_CELSIUS_FACTOR = 0.056
SOLAR_ADJUSTMENT_CEILING = 20.0
WIND_ADJUSTMENT_FACTOR = 8.5
RAIN_ADDITION_CELSIUS = 1.5
THERMAL_RETENTION_FACTOR = 0.6

def read_and_process_json_locations(filepath: str) -> pd.DataFrame:
    """
    Lê um arquivo JSON com uma lista simples de locais e o prepara para as chamadas da API.
    """
    print(f"Reading and processing location data from '{filepath}'...")
    try:
        df = pd.read_json(filepath, encoding='utf-8')

        df.rename(columns={
            'SB': ID_COLUMN,
            'Mediana Latitude': LAT_COLUMN,
            'Mediana Longitude': LON_COLUMN
        }, inplace=True)

        required_cols = [ID_COLUMN, LAT_COLUMN, LON_COLUMN]
        if not all(col in df.columns for col in required_cols):
            raise ValueError("O JSON deve conter as colunas: 'SB', 'Mediana Latitude' e 'Mediana Longitude'")

        df[ID_COLUMN] = df[ID_COLUMN].astype(str).str.strip()
        df.dropna(subset=required_cols, inplace=True)
        df.drop_duplicates(subset=[ID_COLUMN], keep='first', inplace=True)

        print(f"✅ Processing complete. {len(df)} unique SBs are ready for prediction.")
        return df[required_cols]

    except FileNotFoundError:
        print(f"❌ ERRO: O arquivo não foi encontrado no caminho: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERRO: Falha ao ler ou processar o arquivo JSON. Detalhes: {e}")
        return pd.DataFrame()

def translate_weather_code(code: int) -> str:
    code_map = {0: 'Céu limpo', 1: 'Principalmente limpo', 2: 'Parcialmente nublado', 3: 'Nublado', 45: 'Nevoeiro', 61: 'Chuva fraca', 63: 'Chuva moderada', 65: 'Chuva forte', 80: 'Pancadas de chuva fracas', 95: 'Trovoada'}
    return code_map.get(code, 'Unclassified')

def calculate_equilibrium_temperature(forecast_hour: pd.Series) -> float:
    air_temp = forecast_hour['temperature_celsius']
    wind_kmh = forecast_hour['wind_speed_kmh']
    precipitation_mm = forecast_hour['precipitation_mm']
    solar_radiation = forecast_hour['solar_radiation_wm2']
    if precipitation_mm > 0:
        return air_temp + RAIN_ADDITION_CELSIUS
    solar_adjustment = solar_radiation * RADIATION_TO_CELSIUS_FACTOR
    solar_adjustment = min(solar_adjustment, SOLAR_ADJUSTMENT_CEILING)
    wind_adjustment = wind_kmh / WIND_ADJUSTMENT_FACTOR
    return (air_temp + solar_adjustment) - wind_adjustment

def apply_thermal_inertia(sb_dataframe: pd.DataFrame) -> pd.Series:
    new_effects_factor = 1 - THERMAL_RETENTION_FACTOR
    estimated_temperatures = []
    previous_rail_temp = None
    sb_dataframe = sb_dataframe.sort_values(by='datetime')
    for _, current_hour in sb_dataframe.iterrows():
        air_temp = current_hour['temperature_celsius']
        if previous_rail_temp is None:
            previous_rail_temp = air_temp
        equilibrium_temp = calculate_equilibrium_temperature(current_hour)
        current_rail_temp = (previous_rail_temp * THERMAL_RETENTION_FACTOR) + (equilibrium_temp * new_effects_factor)
        estimated_temperatures.append(round(current_rail_temp, 2))
        previous_rail_temp = current_rail_temp
    return pd.Series(estimated_temperatures, index=sb_dataframe.index)

# --- MAIN EXECUTION BLOCK ---
def main():
    try:
        locations_df = read_and_process_json_locations(INPUT_JSON_FILE)
        
        # Adicionado um verificador para sair caso o arquivo de locais não seja carregado
        if locations_df.empty:
            print("Não foi possível carregar os dados de localização. Encerrando o programa.")
            return
            
        params = {'hourly': API_HOURLY_VARS, 'timezone': 'America/Sao_Paulo'}
        history_df = pd.DataFrame()
        if not os.path.exists(OUTPUT_FILE):
            print("\nHistory file not found. Fetching initial 7-day history...")
            params['past_days'] = 7
        else:
            print(f"\nHistory file found. Fetching forecast for today and tomorrow...")
            params['forecast_days'] = 3
            history_df = pd.read_excel(OUTPUT_FILE)

        all_weather_data = []
        for _, location in locations_df.iterrows():
            location_id = str(location[ID_COLUMN])
            print(f"Fetching data for location: {location_id}...")
            params.update({'latitude': location[LAT_COLUMN], 'longitude': location[LON_COLUMN]})
            try:
                response = requests.get(API_BASE_URL, params=params)
                response.raise_for_status()
                weather_df = pd.DataFrame(response.json().get('hourly', {}))
                weather_df[ID_COLUMN] = location_id
                weather_df[LAT_COLUMN] = location[LAT_COLUMN]
                weather_df[LON_COLUMN] = location[LON_COLUMN]
                all_weather_data.append(weather_df)
            except requests.exceptions.RequestException as e:
                print(f"⚠️ API call failed for {location_id}: {e}")

        if not all_weather_data:
            print("No new weather data was obtained. Exiting.")
            return
            
        new_df = pd.concat(all_weather_data, ignore_index=True).dropna()
        new_df.rename(columns={'time': 'datetime', 'temperature_2m': 'temperature_celsius', 'precipitation': 'precipitation_mm', 'weather_code': 'weather_code', 'wind_speed_10m': 'wind_speed_kmh', 'direct_normal_irradiance': 'solar_radiation_wm2'}, inplace=True)
        new_df['datetime'] = pd.to_datetime(new_df['datetime'])
        
        clean_history_df = pd.DataFrame()
        if not history_df.empty:
            history_df['datetime'] = pd.to_datetime(history_df['datetime'])
            new_forecast_dates = new_df['datetime'].dt.date.unique()
            clean_history_df = history_df[~history_df['datetime'].dt.date.isin(new_forecast_dates)]
        
        final_df = pd.concat([clean_history_df, new_df], ignore_index=True)
        final_df.sort_values(by=[ID_COLUMN, 'datetime'], inplace=True)
        
        print("\nApplying thermal inertia model to all data...")
        final_df['sky_condition'] = final_df['weather_code'].apply(translate_weather_code)
        estimations = final_df.groupby(ID_COLUMN, group_keys=False).apply(apply_thermal_inertia)
        final_df['estimated_rail_temp'] = estimations
        
        ordered_columns = [
            ID_COLUMN,
            'datetime',
            LAT_COLUMN,
            LON_COLUMN,
            'estimated_rail_temp',
            'sky_condition',
            'temperature_celsius',
            'precipitation_mm',
            'wind_speed_kmh',
            'solar_radiation_wm2'
        ]
        final_df = final_df[ordered_columns]
        
        final_df.to_excel(OUTPUT_FILE, index=False)
        print(f"\n✅ Success! Prediction history updated and saved to '{OUTPUT_FILE}'.")
    except Exception as e:
        print(f"\n❌ A fatal error occurred during execution: {e}")

if __name__ == "__main__":
    main()