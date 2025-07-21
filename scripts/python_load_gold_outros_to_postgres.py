import logging
import os
import urllib.parse  # Para codificar a senha na URL de conexão para o BD

import pandas as pd
import psycopg2  # Necessário para capturar exceções específicas de conexão/SQL
from sqlalchemy import (  # Importar 'text' para comandos SQL brutos
    create_engine, text, types)

# --- Configuração de Logging ---
# Define o caminho para o arquivo de log, colocando-o na raiz do projeto
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etl_indicadores_gold_to_db.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logging.info("Script ETL: Indicadores (Gold -> PostgreSQL) - Iniciado.")

# --- Configurações Locais de Pastas (Corrigido para a estrutura do seu projeto) ---
# Define o diretório base como o diretório PAI do diretório do script.
# Se o script está em /MeuProjeto/scripts/, BASE_DIR será /MeuProjeto/.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminho completo para a pasta de entrada dos dados consolidados da Gold (outros-tratado)
# GOLD_OUTROS_INPUT_PATH será: seu_projeto_raiz/data/gold/outros-tratado
GOLD_OUTROS_INPUT_PATH = os.path.join(BASE_DIR, 'data', 'gold', 'outros-tratado')

# Não é necessário criar esta pasta aqui, pois ela já deveria ter sido criada
# pelo script anterior (pipeline_silver_to_gold_outros.py) que gera o arquivo.
# os.makedirs(GOLD_OUTROS_INPUT_PATH, exist_ok=True) # Removido, pois é uma pasta de LEITURA aqui.

# --- Adicionado para Depuração (Remova após confirmar que está funcionando) ---
logging.info(f"DEBUG: Caminho absoluto do script: {os.path.abspath(__file__)}")
logging.info(f"DEBUG: Diretório do script: {os.path.dirname(os.path.abspath(__file__))}")
logging.info(f"DEBUG: BASE_DIR (raiz do projeto): {BASE_DIR}")
logging.info(f"DEBUG: Caminho completo para GOLD (Origem): {GOLD_OUTROS_INPUT_PATH}")
logging.info("-" * 50)


# --- Configurações de Conexão com PostgreSQL Local ---
DB_USER = 'rogerym'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

# Crie a string de conexão (url-encoded para a senha)
CONN_STR = f"postgresql+psycopg2://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# --- Funções de Processamento ---

def carregar_indicadores_gold_to_postgresql(caminho_arquivo_gold_local: str, db_engine, nome_tabela_db: str, if_exists_mode: str = 'replace'):
    """
    Carrega dados de um arquivo CSV da camada GOLD (outros-tratado) para uma tabela no PostgreSQL local.
    Assume que o arquivo CSV já está limpo e pronto para uso.
    """
    filename = os.path.basename(caminho_arquivo_gold_local)
    logging.info(f"Iniciando carregamento do arquivo '{filename}' para a tabela '{nome_tabela_db}' no PostgreSQL.")
    try:
        df_gold = pd.read_csv(
            caminho_arquivo_gold_local,
            sep=';',
            decimal=',',
            parse_dates=['data_referencia']
        )
        logging.info(f"Arquivo '{filename}' carregado. Linhas: {len(df_gold)}")

        original_columns = df_gold.columns.tolist()
        df_gold.columns = [
            col.lower()
            .replace('.', '_')
            .replace(' ', '_')
            .replace('/', '_')
            .replace('-', '_')
            .replace('(', '')
            .replace(')', '')
            for col in df_gold.columns
        ]
        logging.info(f"Colunas do DataFrame ajustadas para PostgreSQL: {original_columns} -> {df_gold.columns.tolist()}")

        # Renomeia data_referencia para data_base para consistência com outras tabelas
        if 'data_referencia' in df_gold.columns:
            df_gold.rename(columns={'data_referencia': 'data_base'}, inplace=True)
            logging.info("Coluna 'data_referencia' renomeada para 'data_base' para consistência.")

        # Garante que 'data_base' seja do tipo Date no Pandas antes de mapear para SQL
        if 'data_base' in df_gold.columns:
            df_gold['data_base'] = pd.to_datetime(df_gold['data_base']).dt.normalize()

        # Mapeamento de tipos para PostgreSQL
        table_dypes = {
            'data_base': types.Date,
            'taxa_desemprego': types.DECIMAL(precision=10, scale=4),
            'taxa_inadimplencia_pf': types.DECIMAL(precision=10, scale=4),
            'valor_ipca': types.DECIMAL(precision=10, scale=4),
            'taxa_selic_meta': types.DECIMAL(precision=10, scale=4),
        }

        # Filtra apenas os dtypes que existem no DataFrame para o to_sql
        table_dypes = {k: v for k, v in table_dypes.items() if k in df_gold.columns}

        df_gold.to_sql(
            nome_tabela_db,
            con=db_engine,
            if_exists=if_exists_mode,
            index=False,
            chunksize=5000,
            dtype=table_dypes
        )
        logging.info(f"Dados carregados no PostgreSQL com sucesso para a tabela '{nome_tabela_db}' usando modo '{if_exists_mode}'.")

    except FileNotFoundError:
        logging.error(f"Arquivo Gold de indicadores não encontrado em '{caminho_arquivo_gold_local}'.", exc_info=True)
    except psycopg2.Error as e:
        logging.error(f"Erro de banco de dados ao carregar '{filename}' para o PostgreSQL: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Erro inesperado ao carregar '{filename}' para o PostgreSQL: {e}", exc_info=True)


# --- Lógica Principal de Automação ---
if __name__ == '__main__':
    logging.info("--- INICIANDO CARREGAMENTO: Indicadores para PostgreSQL ---")

    # O nome do arquivo consolidado gerado pelo script anterior
    INDICADORES_FILE_NAME = "indicadores_consolidados.csv" # Usar o nome que definimos no script anterior
    INDICADORES_FILE_PATH = os.path.join(GOLD_OUTROS_INPUT_PATH, INDICADORES_FILE_NAME)

    TABELA_INDICADORES_POSTGRESQL = 'ft_indicadores_economicos_mensal'

    try:
        engine = create_engine(CONN_STR)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
    except Exception as e:
        logging.critical(f"Falha ao conectar ao banco de dados: {e}", exc_info=True)
        logging.critical("Verifique as credenciais do banco, host, porta e se o serviço do PostgreSQL está rodando.")
        exit(1)

    if os.path.exists(INDICADORES_FILE_PATH):
        carregar_indicadores_gold_to_postgresql(INDICADORES_FILE_PATH, engine, TABELA_INDICADORES_POSTGRESQL, if_exists_mode='replace')
    else:
        logging.error(f"Arquivo de indicadores '{INDICADORES_FILE_NAME}' não encontrado em '{GOLD_OUTROS_INPUT_PATH}'.")
        logging.error("Por favor, certifique-se de que o script de ETL para 'outros-agregados' foi executado e gerou o arquivo.")

    logging.info("--- Processamento e Carga de Indicadores para PostgreSQL CONCLUÍDOS ---")
    logging.info("--- Script ETL: Indicadores (Gold -> PostgreSQL) - FINALIZADO. ---")