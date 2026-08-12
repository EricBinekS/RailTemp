# tests/test_processing.py
"""
Testes unitários para o módulo processing.py
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pandas.testing import assert_series_equal, assert_frame_equal
import pytest # Importa o pytest

# Importa as funções que queremos testar do nosso pacote
from rail_predictor.processing import (
    translate_weather_code,
    calculate_equilibrium_temperature_vectorized,
    apply_thermal_inertia_fast,
    apply_rolling_window
)
# Precisamos da classe Config para testar com as constantes corretas
from rail_predictor.config import Config


def test_translate_weather_code():
    """Testa a tradução de códigos de tempo conhecidos e desconhecidos."""
    # Teste 1: Código conhecido (Chuva fraca)
    assert translate_weather_code(61) == "Chuva fraca"
    
    # Teste 2: Código conhecido (Céu limpo)
    assert translate_weather_code(0) == "Céu limpo"
    
    # Teste 3: Código desconhecido (fallback)
    assert translate_weather_code(9999) == "Não classificado"


def test_calculate_equilibrium_temperature_vectorized():
    """
    Testa o cálculo da temperatura de equilíbrio vetorizada,
    especialmente as condições de chuva e teto de radiação.
    """
    # 1. ARRANGE (Organizar): Cria dados de teste
    
    test_data = pd.DataFrame({
        'temperature_celsius': [
            10.0,  # Caso 1
            10.0,  # Caso 2
            10.0   # Caso 3
        ],
        'wind_speed_kmh': [
            Config.WIND_ADJUSTMENT_FACTOR,  # Caso 1 (Vento = 8.5)
            8.5,                            # Caso 2
            0.0                             # Caso 3
        ],
        'precipitation_mm': [
            0.0,  # Caso 1
            1.0,  # Caso 2 (Chovendo)
            0.0   # Caso 3
        ],
        'solar_radiation_wm2': [
            100.0,    # Caso 1
            1000.0,   # Caso 2
            10000.0   # Caso 3 (Valor extremo)
        ],
    })

    # 2. ACT (Agir): Executa a função
    result_series = calculate_equilibrium_temperature_vectorized(test_data)

    # 3. ASSERT (Verificar): Define os resultados esperados

    # Caso 1: (10.0 + (100.0 * 0.056)) - (8.5 / 8.5) = (10.0 + 5.6) - 1.0 = 14.6
    expected_case_1 = 14.6

    # Caso 2: 10.0 + RAIN_ADDITION_CELSIUS = 10.0 + 1.5 = 11.5
    expected_case_2 = 11.5

    # Caso 3: solar_adj = 10000.0 * 0.056 = 560. Teto é 20.0.
    # (10.0 + 20.0) - (0.0 / 8.5) = 30.0
    expected_case_3 = 30.0

    expected_series = pd.Series([expected_case_1, expected_case_2, expected_case_3])

    assert_series_equal(
        result_series.astype(float), 
        expected_series.astype(float), 
        check_names=False
    )

def test_apply_thermal_inertia_fast():
    """
    Testa o coração do modelo: a aplicação da inércia térmica
    Verifica se o cálculo iterativo está correto.
    """
    # 1. ARRANGE: Define as constantes do modelo
    retention_factor = Config.THERMAL_RETENTION_FACTOR # 0.6
    new_effects_factor = 1 - retention_factor          # 0.4
    
    # Cria dados de teste para um único SB
    test_data = pd.DataFrame({
        'temperature_celsius': [10.0, 12.0, 14.0],  # Temp. do Ar (Usada para t=0)
        'equilibrium_temp':    [20.0, 30.0, 10.0],  # Temp. de Equilíbrio
    })

    # 2. ACT: Executa a função
    result_series = apply_thermal_inertia_fast(test_data)

    # 3. ASSERT: Calcula o resultado esperado manualmente
    
    # t=0:
    # previous_rail_temp = air_temp[0] = 10.0
    # rail_temp(0) = (10.0 * 0.6) + (20.0 * 0.4) = 6.0 + 8.0 = 14.0
    expected_t0 = 14.0
    
    # t=1:
    # previous_rail_temp = 14.0
    # rail_temp(1) = (14.0 * 0.6) + (30.0 * 0.4) = 8.4 + 12.0 = 20.4
    expected_t1 = 20.4
    
    # t=2:
    # previous_rail_temp = 20.4
    # rail_temp(2) = (20.4 * 0.6) + (10.0 * 0.4) = 12.24 + 4.0 = 16.24
    expected_t2 = 16.24

    expected_series = pd.Series([
        round(expected_t0, 2), 
        round(expected_t1, 2), 
        round(expected_t2, 2)
    ])
    
    assert_series_equal(result_series, expected_series, check_names=False)


def test_apply_rolling_window_filters_correctly():
    """
    Testa se a janela móvel mantém apenas D-3 a D+3 e descarta o resto,
    usando uma data de referência fixa para o teste ser determinístico.
    """
    # 1. ARRANGE: D0 fixo para o teste
    reference_date = datetime(2026, 8, 12)

    dates = [
        reference_date - timedelta(days=10),  # fora (muito antigo)
        reference_date - timedelta(days=4),   # fora (D-4)
        reference_date - timedelta(days=3),   # dentro (D-3, borda)
        reference_date - timedelta(days=1),   # dentro
        reference_date,                       # dentro (D0)
        reference_date + timedelta(days=3),   # dentro (D+3, borda)
        reference_date + timedelta(days=4),   # fora (D+4)
        reference_date + timedelta(days=15),  # fora (muito no futuro)
    ]

    test_data = pd.DataFrame({
        'SB': ['A'] * len(dates),
        'datetime': dates,
        'estimated_rail_temp': range(len(dates))
    })

    # 2. ACT
    result_df = apply_rolling_window(
        test_data,
        reference_date=reference_date,
        days_past=3,
        days_future=3
    )

    # 3. ASSERT: apenas as 4 linhas dentro de [D-3, D+3] devem sobrar
    assert len(result_df) == 4
    expected_dates = {
        (reference_date - timedelta(days=3)).date(),
        (reference_date - timedelta(days=1)).date(),
        reference_date.date(),
        (reference_date + timedelta(days=3)).date(),
    }
    result_dates = set(pd.to_datetime(result_df['datetime']).dt.date)
    assert result_dates == expected_dates


def test_apply_rolling_window_empty_dataframe():
    """Testa se um DataFrame vazio é tratado sem erros."""
    empty_df = pd.DataFrame(columns=['SB', 'datetime'])
    result_df = apply_rolling_window(empty_df)
    assert result_df.empty


def test_apply_rolling_window_default_reference_is_today():
    """Testa se, sem data de referência explícita, a função usa 'hoje'."""
    today = datetime.now()

    test_data = pd.DataFrame({
        'SB': ['A', 'A', 'A'],
        'datetime': [
            today - timedelta(days=100),  # fora
            today,                        # dentro
            today + timedelta(days=100),  # fora
        ]
    })

    result_df = apply_rolling_window(test_data)

    assert len(result_df) == 1