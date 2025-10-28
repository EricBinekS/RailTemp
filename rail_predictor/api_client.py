# rail_predictor/api_client.py
"""
Módulo de Cliente de API.
"""
import pandas as pd
import requests
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Importação relativa - "do mesmo pacote, importe config"
from .config import Config 

def fetch_single_location(location_data: Dict[str, Any], base_params: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Busca dados meteorológicos para uma única localização geográfica."""
    location_id = str(location_data[Config.ID_COLUMN])
    params = base_params.copy()
    params.update({
        'latitude': location_data[Config.LAT_COLUMN],
        'longitude': location_data[Config.LON_COLUMN]
    })
    
    try:
        response = requests.get(Config.API_BASE_URL, params=params)
        response.raise_for_status()
        
        weather_df = pd.DataFrame(response.json().get('hourly', {}))
        if weather_df.empty:
            print(f"⚠️ Aviso: Nenhum dado 'hourly' retornado para {location_id}.")
            return None
            
        weather_df[Config.ID_COLUMN] = location_id
        weather_df[Config.LAT_COLUMN] = location_data[Config.LAT_COLUMN]
        weather_df[Config.LON_COLUMN] = location_data[Config.LON_COLUMN]
        return weather_df
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO API: Falha na chamada para {location_id}: {e}")
        return None

def fetch_weather_data_parallel(locations_df: pd.DataFrame, api_params: Dict[str, Any]) -> pd.DataFrame:
    """Orquestra a coleta de dados da API em paralelo para múltiplas localizações."""
    all_weather_data = []
    locations_list = locations_df.to_dict('records')
    
    with tqdm(total=len(locations_list), desc="Coletando dados da API", unit="local") as pbar:
        with ThreadPoolExecutor(max_workers=Config.MAX_API_WORKERS) as executor:
            futures = {
                executor.submit(fetch_single_location, loc, api_params): loc
                for loc in locations_list
            }
            
            for future in as_completed(futures):
                result_df = future.result()
                if result_df is not None:
                    all_weather_data.append(result_df)
                pbar.update(1)

    if not all_weather_data:
        return pd.DataFrame()
        
    return pd.concat(all_weather_data, ignore_index=True)