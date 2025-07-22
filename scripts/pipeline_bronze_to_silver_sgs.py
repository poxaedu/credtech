import logging  # Importar logging para uma abordagem mais robusta
import os
from typing import Any, Dict, List

import pandas as pd

# --- Configuração de Logging ---
# Define o caminho para o arquivo de log, colocando-o na raiz do projeto
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etl_pipeline_outros.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path), # Salva logs em um arquivo
        logging.StreamHandler() # Exibe logs no console
    ]
)
logging.info("Script ETL: Bronze to Silver (outros data) - Iniciado.")


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

# --- CAMINHOS DAS PASTAS (Corrigido para a estrutura do seu projeto) ---
# Define o diretório base como o diretório PAI do diretório do script.
# Se o script está em /MeuProjeto/scripts/, BASE_DIR será /MeuProjeto/.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminhos de origem e destino, agora relativos ao BASE_DIR (raiz do projeto)
BRONZE_DIR = os.path.join(BASE_DIR, 'raw_data', 'outros')
SILVER_DIR = os.path.join(BASE_DIR, 'data', 'silver', 'outros') # 'silver' está dentro de 'data'

# Garante que a pasta de destino (Silver) exista
os.makedirs(SILVER_DIR, exist_ok=True)

# --- Mensagens de Log Aprimoradas ---
logging.info("--- INICIANDO PIPELINE BRONZE -> SILVER (FICHEIROS DE OUTROS) ---")
logging.info(f"Pasta de origem (Bronze): {BRONZE_DIR}")
logging.info(f"Pasta de destino (Silver): {SILVER_DIR}")

# --- 2. LOOP DE PROCESSAMENTO ---
# Itera sobre cada configuração de indicador
for config in CONFIG_INDICADORES:
    try:
        caminho_bronze = os.path.join(BRONZE_DIR, config['arquivo_bronze'])

        logging.info(f"\n----------------------------------------------------")
        logging.info(f"Processando: {config['arquivo_bronze']}")

        # Verifica se o arquivo de origem existe antes de tentar carregar
        if not os.path.exists(caminho_bronze):
            logging.warning(f"AVISO: Ficheiro não encontrado em '{caminho_bronze}'. Pulando este arquivo.")
            continue # Pula para o próximo item no loop

        # Carrega o ficheiro CSV original
        df_raw = pd.read_csv(caminho_bronze, sep=';', decimal=',')
        logging.info(f"Arquivo '{config['arquivo_bronze']}' carregado. Linhas: {len(df_raw)}")

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
            dayfirst=True # Assume que 'dd/mm/aaaa' é o formato primário se houver ambiguidade
        )

        # Converte a coluna de valor para float e remove linhas com valores nulos
        df_silver[config['nome_coluna_valor']] = df_silver[config['nome_coluna_valor']].astype(float)
        df_silver.dropna(inplace=True)

        logging.info(f"Dados limpos e tratados. {len(df_silver)} linhas válidas.")

        # Constrói o caminho de saída para o novo ficheiro CSV na pasta Silver
        caminho_silver = os.path.join(SILVER_DIR, f"{config['arquivo_silver']}.csv")

        # Salva o DataFrame limpo como um novo ficheiro CSV
        df_silver.to_csv(
            caminho_silver,
            index=False,        # Não salva o índice do DataFrame como uma coluna
            sep=';',            # Usa ponto e vírgula como separador
            decimal=','         # Usa vírgula como separador decimal
        )

        logging.info(f"SUCESSO! Dados salvos no ficheiro '{caminho_silver}'.")

    except FileNotFoundError:
        # Este bloco agora é redundante se a verificação `if not os.path.exists` estiver acima
        logging.error(f"ERRO CRÍTICO: Ficheiro não encontrado em '{caminho_bronze}'. (Erro FileNotFoundError)")
    except Exception as e:
        logging.error(f"ERRO inesperado ao processar '{config['arquivo_bronze']}': {e}", exc_info=True) # exc_info=True para detalhes do erro

logging.info("\n--- PIPELINE BRONZE -> SILVER CONCLUÍDO ---")