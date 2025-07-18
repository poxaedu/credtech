import pandas as pd
import os
from typing import List, Dict, Any

# --- 1. CONFIGURAÇÃO ---
# Edite esta lista para adicionar ou remover os ficheiros que deseja processar.
# Cada dicionário representa um ficheiro a ser tratado.
CONFIG_INDICADORES: List[Dict[str, Any]] = [
    {
        'arquivo_bronze': 'sgs_432_taxa_selic_meta.csv',
        'arquivo_silver': 'silver_selic', # Nome base para o ficheiro de saída
        'nome_coluna_valor': 'taxa_selic_meta'
    },
    {
        'arquivo_bronze': 'sgs_13522_ipca_inflacao.csv',
        'arquivo_silver': 'silver_ipca',
        'nome_coluna_valor': 'valor_ipca'
    },
    {
        'arquivo_bronze': 'sgs_24369_taxa_desemprego.csv',
        'arquivo_silver': 'silver_desemprego',
        'nome_coluna_valor': 'taxa_desemprego'
    },
    {
        'arquivo_bronze': 'sgs_21082_inadimplencia_pf.csv',
        'arquivo_silver': 'silver_inadimplencia',
        'nome_coluna_valor': 'taxa_inadimplencia_pf'
    }
]

# --- CAMINHOS DAS PASTAS ---
# Caminho de origem ajustado de acordo com a sua estrutura de ficheiros.
BRONZE_DIR = os.path.join('bronze', 'dados_sg') 
SILVER_DIR = os.path.join('silver')

# Garante que a pasta de destino (Silver) exista
os.makedirs(SILVER_DIR, exist_ok=True)

print("--- INICIANDO PIPELINE BRONZE -> SILVER (FICHEIROS) ---")
print(f"Pasta de origem (Bronze): {BRONZE_DIR}")
print(f"Pasta de destino (Silver): {SILVER_DIR}")

# --- 2. LOOP DE PROCESSAMENTO ---
# Itera sobre cada configuração de indicador
for config in CONFIG_INDICADORES:
    try:
        caminho_bronze = os.path.join(BRONZE_DIR, config['arquivo_bronze'])
        
        print(f"\n----------------------------------------------------")
        print(f"Processando: {config['arquivo_bronze']}")
        
        # Carrega o ficheiro CSV original
        df_raw = pd.read_csv(caminho_bronze, sep=';', decimal=',')

        # Limpeza e Normalização
        df_silver = df_raw.copy()
        df_silver.rename(columns={
            df_silver.columns[0]: 'data_referencia',
            df_silver.columns[1]: config['nome_coluna_valor']
        }, inplace=True)
        
        # Converte a data de forma robusta, lidando com formatos mistos ('dd/mm/aaaa' e 'aaaa-mm-dd')
        df_silver['data_referencia'] = pd.to_datetime(
            df_silver['data_referencia'], 
            format='mixed', 
            dayfirst=True
        )
        
        # Converte a coluna de valor para float e remove linhas com valores nulos
        df_silver[config['nome_coluna_valor']] = df_silver[config['nome_coluna_valor']].astype(float)
        df_silver.dropna(inplace=True)
        
        print(f"Dados limpos e tratados. {len(df_silver)} linhas válidas.")

        # Constrói o caminho de saída para o novo ficheiro CSV na pasta Silver
        caminho_silver = os.path.join(SILVER_DIR, f"{config['arquivo_silver']}.csv")

        # Salva o DataFrame limpo como um novo ficheiro CSV
        df_silver.to_csv(
            caminho_silver, 
            index=False,          # Não salva o índice do DataFrame como uma coluna
            sep=';',              # Usa ponto e vírgula como separador
            decimal=','           # Usa vírgula como separador decimal
        )
        
        print(f"SUCESSO! Dados salvos no ficheiro '{caminho_silver}'.")

    except FileNotFoundError:
        print(f"ERRO: Ficheiro não encontrado em '{caminho_bronze}'. A processar o próximo.")
    except Exception as e:
        print(f"ERRO inesperado ao processar '{config['arquivo_bronze']}': {e}")

print("\n--- PIPELINE BRONZE -> SILVER CONCLUÍDO ---")