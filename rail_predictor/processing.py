# rail_predictor/processing.py
"""
Módulo de Processamento e Modelagem.
"""
import pandas as pd
import numpy as np

# Importação relativa
from .config import Config

def translate_weather_code(code: int) -> str:
    """Decodifica o WMO weather code (código de tempo) em uma string legível."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    code_map = {
        0: 'Céu limpo', 1: 'Principalmente limpo', 2: 'Parcialmente nublado', 3: 'Nublado',
        45: 'Nevoeiro',
        61: 'Chuva fraca', 63: 'Chuva moderada', 65: 'Chuva forte',
        80: 'Pancadas de chuva fracas', 95: 'Trovoada'
    }
    return code_map.get(code, 'Não classificado')

def calculate_equilibrium_temperature_vectorized(df: pd.DataFrame) -> pd.Series:
    """Calcula a temperatura de equilíbrio (sem inércia) de forma vetorizada."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    air_temp = df['temperature_celsius']
    wind_kmh = df['wind_speed_kmh']
    precipitation_mm = df['precipitation_mm']
    solar_radiation = df['solar_radiation_wm2']

    solar_adjustment = solar_radiation * Config.RADIATION_TO_CELSIUS_FACTOR
    solar_adjustment = np.minimum(solar_adjustment, Config.SOLAR_ADJUSTMENT_CEILING) 

    wind_adjustment = wind_kmh / Config.WIND_ADJUSTMENT_FACTOR
    
    equilibrium_temp = (air_temp + solar_adjustment) - wind_adjustment
    
    final_equilibrium_temp = np.where(
        precipitation_mm > 0, 
        air_temp + Config.RAIN_ADDITION_CELSIUS, 
        equilibrium_temp
    )
    return pd.Series(final_equilibrium_temp, index=df.index)

def apply_thermal_inertia_fast(sb_dataframe: pd.DataFrame) -> pd.Series:
    """Aplica o modelo de inércia térmica iterativamente sobre um grupo (SB)."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    new_effects_factor = 1 - Config.THERMAL_RETENTION_FACTOR
    
    equilibrium_temps = sb_dataframe['equilibrium_temp'].to_numpy()
    air_temps = sb_dataframe['temperature_celsius'].to_numpy()
    
    n = len(equilibrium_temps)
    estimated_temperatures = np.zeros(n)
    
    if n > 0:
        previous_rail_temp = air_temps[0] 
        for i in range(n):
            equilibrium_temp = equilibrium_temps[i]
            current_rail_temp = (previous_rail_temp * Config.THERMAL_RETENTION_FACTOR) + \
                                (equilibrium_temp * new_effects_factor)
            estimated_temperatures[i] = round(current_rail_temp, 2)
            previous_rail_temp = current_rail_temp
            
    return pd.Series(estimated_temperatures, index=sb_dataframe.index)

def run_processing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """Executa o pipeline de transformação completo nos dados meteorológicos brutos."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    df.rename(columns={
        'time': 'datetime', 
        'temperature_2m': 'temperature_celsius', 
        'precipitation': 'precipitation_mm', 
        'weather_code': 'weather_code', 
        'wind_speed_10m': 'wind_speed_kmh', 
        'direct_normal_irradiance': 'solar_radiation_wm2'
    }, inplace=True)
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.dropna()

    df.sort_values(by=[Config.ID_COLUMN, 'datetime'], inplace=True)

    print("Calculando temperaturas de equilíbrio (vetorizado)...")
    df['equilibrium_temp'] = calculate_equilibrium_temperature_vectorized(df)

    print("Aplicando modelo de inércia térmica (otimizado)...")
    estimations = df.groupby(Config.ID_COLUMN, group_keys=False).apply(apply_thermal_inertia_fast)
    df['estimated_rail_temp'] = estimations
    
    df['sky_condition'] = df['weather_code'].apply(translate_weather_code)

    ordered_columns = [
        Config.ID_COLUMN, 'datetime', Config.LAT_COLUMN, Config.LON_COLUMN,
        'estimated_rail_temp', 'sky_condition', 'temperature_celsius',
        'precipitation_mm', 'wind_speed_kmh', 'solar_radiation_wm2'
    ]
    
    final_columns = [col for col in ordered_columns if col in df.columns]
    return df[final_columns]