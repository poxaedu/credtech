import pandas as pd
import os
import logging
from sqlalchemy import create_engine, types, text
import urllib.parse
import psycopg2

# --- Configuração de Logging ---
log_file_path = 'etl_indicadores_gold_to_db.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logging.info("Script ETL: Indicadores (Gold -> PostgreSQL) - Iniciado em ambiente LOCAL.")

# --- Configurações Locais de Pastas ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOLD_OUTROS_INPUT_PATH = os.path.join(BASE_DIR, 'gold', 'outros-agregados')

os.makedirs(GOLD_OUTROS_INPUT_PATH, exist_ok=True)


# --- Configurações de Conexão com PostgreSQL Local ---
DB_USER = 'jjguilherme'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# --- Funções de Processamento ---

def carregar_indicadores_gold_to_postgresql(caminho_arquivo_gold_local: str, db_engine, nome_tabela_db: str, if_exists_mode: str = 'replace'):
    """
    Carrega dados de um arquivo CSV da camada GOLD (outros-agregados) para uma tabela no PostgreSQL local.
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

        if 'data_referencia' in df_gold.columns:
            df_gold.rename(columns={'data_referencia': 'data_base'}, inplace=True)
            logging.info("Coluna 'data_referencia' renomeada para 'data_base' para consistência.")

        table_dypes = {
            'data_base': types.Date,
            'taxa_desemprego': types.DECIMAL(precision=10, scale=4),
            'taxa_inadimplencia_pf': types.DECIMAL(precision=10, scale=4),
            'valor_ipca': types.DECIMAL(precision=10, scale=4),
            'taxa_selic_meta': types.DECIMAL(precision=10, scale=4),
        }
        
        if 'data_base' in df_gold.columns:
            df_gold['data_base'] = pd.to_datetime(df_gold['data_base']).dt.normalize()

        table_dypes = {k: v for k, v in table_dypes.items() if v is not None}

        df_gold.to_sql(
            nome_tabela_db,
            con=db_engine,
            if_exists=if_exists_mode, 
            index=False, 
            chunksize=5000,
            dtype=table_dypes 
        )
        # ESTA É A ÚNICA LINHA DE LOG QUE DEVE ESTAR AQUI. 
        # A LINHA DUPLICADA/PROBLEMÁTICA FOI REMOVIDA.
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
    
    INDICADORES_FILE_NAME = "indicadores.csv"
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