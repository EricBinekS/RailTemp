# Pipeline de Previsão de Temperatura de Trilhos

Este projeto é uma pipeline de dados ETL automatizada, projetada para prever a temperatura de trilhos (baseado em um modelo de inércia térmica) para várias seções de bloco (SBs) de uma ferrovia.

O processo é executado diariamente via GitHub Actions, gerando um arquivo de histórico (`.parquet`) que pode ser consumido diretamente por ferramentas de BI como o Power BI.

---

##  Funcionalidades

* **Processamento Paralelo:** Coleta dados de 650+ locais da API Open-Meteo em menos de 5 minutos, usando `ThreadPoolExecutor`.
* **Modelo de Inércia Térmica:** Aplica um modelo físico para calcular a `estimated_rail_temp` com base na temperatura do ar, radiação solar, vento e chuva.
* **Testes Unitários:** A lógica de negócios principal é validada usando `pytest`, garantindo a precisão dos cálculos.
* **ETL Automatizado:** Uma pipeline completa que extrai (API), transforma (Pandas/NumPy) e carrega (Parquet) os dados.
* **Pronto para BI:** A saída em formato Parquet é otimizada para ingestão rápida no Power BI ou outros data warehouses.
* **CI/CD:** A pipeline é executada automaticamente em um agendamento (`cron`) usando GitHub Actions, com o histórico de dados sendo salvo de volta no repositório.

##  Como Funciona (Pipeline ETL)

1.  **Extract (Extrair):**
    * O `main.py` é iniciado pelo agendador do GitHub Actions.
    * O `data_io.py` lê o `data/coordenadas.json` para obter a lista de SBs.
    * O `api_client.py` busca dados meteorológicos para todos os 650+ SBs em paralelo.

2.  **Transform (Transformar):**
    * O `processing.py` aplica o modelo físico.
    * `calculate_equilibrium_temperature_vectorized` calcula a temperatura de equilíbrio (sem inércia) para todas as linhas de uma vez (vetorizado).
    * `apply_thermal_inertia_fast` aplica o modelo de inércia (dependente do tempo) em cada SB usando `groupby().apply()` otimizado com NumPy.
    * Os novos dados são mesclados com o histórico carregado pelo `data_io.py`.

3.  **Load (Carregar):**
    * O `data_io.py` salva o DataFrame final e completo como `data/rail_prediction_history.parquet`.
    * O GitHub Action faz o *commit* desse novo arquivo `.parquet` de volta ao repositório.

##  Estrutura do Projeto

O projeto é estruturado como um pacote Python para aderir ao Princípio da Responsabilidade Única (SRP).

##  Comandos Úteis (Ambiente Local)

Guia rápido para configurar e executar o projeto manualmente em sua máquina local.

### 1. Configuração do Ambiente

Execute estes comandos apenas uma vez para configurar o projeto.

```bash
# 1. Crie o ambiente virtual (venv)
python -m venv .venv

# 2. Ative o ambiente (Windows - CMD/PowerShell)
.\.venv\Scripts\activate
# OU (macOS/Linux)
# source .venv/bin/activate

# 3. Instale todas as dependências necessárias
pip install -r requirements.txt
```

### 2. Execução e Testes

```bash
# 1. Para rodar a pipeline principal de ETL
python main.py

# 2. Para rodar a suíte de testes unitários
pytest

# 3. Para rodar os testes em modo detalhado (verbose)
pytest -v
```
