# rail_predictor/data_io.py
"""
Módulo de Entrada/Saída (I/O) de Dados.
"""
import pandas as pd

# Importação relativa
from .config import Config

def load_locations(filepath: str = Config.INPUT_JSON_FILE) -> pd.DataFrame:
    """Carrega e valida o arquivo JSON de locais de entrada."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    try:
        df = pd.read_json(filepath, encoding='utf-8')
        df.rename(columns={
            'SB': Config.ID_COLUMN,
            'Mediana Latitude': Config.LAT_COLUMN,
            'Mediana Longitude': Config.LON_COLUMN
        }, inplace=True)

        required_cols = [Config.ID_COLUMN, Config.LAT_COLUMN, Config.LON_COLUMN]
        if not all(col in df.columns for col in required_cols):
            raise ValueError("O JSON deve conter as colunas: 'SB', 'Mediana Latitude' e 'Mediana Longitude'")

        df[Config.ID_COLUMN] = df[Config.ID_COLUMN].astype(str).str.strip()
        df.dropna(subset=required_cols, inplace=True)
        df.drop_duplicates(subset=[Config.ID_COLUMN], keep='first', inplace=True)

        return df[required_cols]

    except FileNotFoundError:
        print(f"❌ ERRO CRÍTICO: Arquivo de locais não encontrado em: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERRO CRÍTICO: Falha ao ler ou processar o JSON de locais. Detalhes: {e}")
        return pd.DataFrame()

def load_history(filepath: str = Config.OUTPUT_FILE, new_data_dates: pd.Series = None) -> pd.DataFrame:
    """Carrega o histórico de previsões (Parquet) e remove dados sobrepostos."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    try:
        history_df = pd.read_parquet(filepath)
        history_df['datetime'] = pd.to_datetime(history_df['datetime'])
        
        if new_data_dates is not None:
            clean_history_df = history_df[~history_df['datetime'].dt.date.isin(new_data_dates)]
            return clean_history_df
        
        return history_df

    except FileNotFoundError:
        print("Arquivo de histórico (parquet) não encontrado. Um novo será criado.")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERRO: Falha ao ler o histórico parquet. Detalhes: {e}")
        return pd.DataFrame()

def save_output(df: pd.DataFrame, filepath: str = Config.OUTPUT_FILE):
    """Salva o DataFrame final no arquivo de saída Parquet."""
    # ... (Nenhuma outra mudança neste arquivo) ...
    try:
        df.to_parquet(filepath, index=False, engine='pyarrow')
        print(f"\n✅ Sucesso! Histórico salvo em '{filepath}'.")
    except Exception as e:
        print(f"❌ ERRO CRÍTICO: Falha ao salvar o arquivo Parquet. Detalhes: {e}")