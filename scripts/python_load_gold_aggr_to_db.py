import logging
import os
import urllib.parse  # Para codificar a senha na URL de conexão para o BD
from datetime import date, timedelta

import pandas as pd
import psycopg2  # Necessário para capturar exceções específicas de conexão/SQL
from sqlalchemy import (  # Importar 'text' para comandos SQL brutos
    create_engine, text, types)

# --- Configuração de Logging ---
# Define o caminho para o arquivo de log, colocando-o na raiz do projeto
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'load_gold_aggr_to_db.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logging.info("Script de Carregamento Gold Agregado para PostgreSQL - Iniciado.")

# --- Configurações Locais de Pastas (Corrigido para a estrutura do seu projeto) ---
# Define o diretório base como o diretório PAI do diretório do script.
# Se o script está em /MeuProjeto/scripts/, BASE_DIR será /MeuProjeto/.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminho completo para a pasta de entrada dos dados agregados da Gold
# GOLD_AGGR_INPUT_PATH será: seu_projeto_raiz/data/gold/scr-tratado
GOLD_AGGR_INPUT_PATH = os.path.join(BASE_DIR, 'data', 'gold', 'scr-tratado')

# --- Configurações de Conexão com PostgreSQL Local (SEUS VALORES REAIS) ---
DB_USER = 'jjguilherme'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

# Crie a string de conexão (url-encoded para a senha)
CONN_STR = f"postgresql+psycopg2://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Adicionado para Depuração (Remova após confirmar que está funcionando) ---
logging.info(f"DEBUG: Caminho absoluto do script: {os.path.abspath(__file__)}")
logging.info(f"DEBUG: Diretório do script: {os.path.dirname(os.path.abspath(__file__))}")
logging.info(f"DEBUG: BASE_DIR (raiz do projeto): {BASE_DIR}")
logging.info(f"DEBUG: Caminho completo para GOLD (Origem): {GOLD_AGGR_INPUT_PATH}")
logging.info("-" * 50)


# --- Funções de Carregamento ---

def carregar_gold_to_postgresql(df_gold: pd.DataFrame, db_engine, table_name: str, if_exists_mode: str):
    """
    Carrega dados de um DataFrame Pandas para uma tabela no PostgreSQL.
    Realiza ajuste de nomes de colunas e mapeamento de tipos.
    """
    logging.info(f"Carregando {len(df_gold)} linhas para a tabela '{table_name}' no PostgreSQL.")
    try:
        # --- Adaptação para Nomes de Colunas SQL ---
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

        # --- Mapeamento de Tipos de Dados (Recomendado para robustez) ---
        dtype_mapping = {
            'object': types.VARCHAR(255),
            'category': types.VARCHAR(255),
            'datetime64[ns]': types.Date,
            'int64': types.BigInteger,
            'int32': types.Integer,
            'float64': types.Float(precision=53),
            'float32': types.Float(precision=24),
            'bool': types.Boolean
        }

        table_dypes = {col: dtype_mapping.get(str(df_gold[col].dtype), None) for col in df_gold.columns}

        if 'taxa_inadimplencia_final_segmento' in df_gold.columns:
            table_dypes['taxa_inadimplencia_final_segmento'] = types.DECIMAL(precision=10, scale=8)
        if 'perc_ativo_problematico_final_segmento' in df_gold.columns:
            table_dypes['perc_ativo_problematico_final_segmento'] = types.DECIMAL(precision=10, scale=8)

        # Remove None values from dtype mapping for to_sql (it doesn't like None)
        table_dypes = {k: v for k, v in table_dypes.items() if v is not None}

        # Carrega para o PostgreSQL
        df_gold.to_sql(
            table_name,
            con=db_engine,
            if_exists=if_exists_mode,
            index=False,
            chunksize=5000,
            dtype=table_dypes
        )
        logging.info(f"Dados carregados no PostgreSQL com sucesso para a tabela '{table_name}' usando modo '{if_exists_mode}'.")
    except Exception as e:
        logging.error(f"Erro ao carregar dados para o PostgreSQL na tabela '{table_name}': {e}", exc_info=True)

# --- Lógica Principal de Automação ---
if __name__ == '__main__':
    logging.info("--- INICIANDO CARREGAMENTO: Camada Gold Agregada para PostgreSQL ---")

    # Nome da tabela de destino no PostgreSQL
    TABELA_DESTINO_POSTGRESQL = 'ft_scr_agregado_mensal' # Nome padrão recomendado para fatos agregados

    # Definindo o período de processamento (deve ser o mesmo que foi para Silver -> Gold)
    # Ajuste estas datas para corresponder aos seus arquivos reais
    start_date_process = date(2024, 1, 1) # Primeiro mês de dados
    end_date_process = date(2025, 5, 1)   # Último mês de dados (18 de julho de 2025 é antes de maio de 2025)

    # Cria a engine de conexão com o banco de dados
    try:
        engine = create_engine(CONN_STR)
        # Testar a conexão
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")) # Usar text() para compatibilidade com SQLAlchemy 2.0+
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
    except Exception as e:
        logging.critical(f"Falha ao conectar ao banco de dados: {e}")
        logging.critical("Verifique as credenciais do banco, host, porta e se o serviço do PostgreSQL está rodando.")
        exit() # Sai do script se não conseguir conectar

    # --- Estratégia de Carregamento: PRIMEIRA CARGA com 'replace', depois 'append' ---

    # Lista todos os arquivos Parquet na pasta Gold/scr-agregado/ANO/ para o período definido
    files_to_load = []
    current_date_loop = start_date_process
    while current_date_loop <= end_date_process:
        year = current_date_loop.year
        month = current_date_loop.month
        month_str = f"{month:02d}"

        # Ajustado para considerar a estrutura de pastas GOLD_AGGR_INPUT_PATH/ano/aggr_segmentos_YYYYMM.parquet
        gold_output_year_dir = os.path.join(GOLD_AGGR_INPUT_PATH, str(year))
        gold_file_name_aggr = f"aggr_segmentos_{year}{month_str}.parquet"
        gold_file_path_aggr = os.path.join(gold_output_year_dir, gold_file_name_aggr)

        if os.path.exists(gold_file_path_aggr):
            files_to_load.append(gold_file_path_aggr)
        else:
            logging.warning(f"Arquivo Gold agregado não encontrado: {gold_file_path_aggr}. Pulando.")

        if month == 12:
            current_date_loop = date(year + 1, 1, 1)
        else:
            current_date_loop = date(year, month + 1, 1)

    if not files_to_load:
        logging.warning("Nenhum arquivo Gold agregado encontrado para o período especificado. Nada para carregar.")
    else:
        # Garante que os arquivos sejam processados em ordem cronológica
        files_to_load.sort()

        # Carrega o PRIMEIRO arquivo com 'replace' para criar ou recriar a tabela
        first_file_path = files_to_load[0]
        logging.info(f"Carregando o primeiro arquivo ('{os.path.basename(first_file_path)}') com modo 'replace' para criar/recriar a tabela '{TABELA_DESTINO_POSTGRESQL}'.")
        try:
            df_first_load = pd.read_parquet(first_file_path)
            # Normaliza a coluna 'data_base' antes de carregar
            if 'data_base' in df_first_load.columns:
                df_first_load['data_base'] = pd.to_datetime(df_first_load['data_base']).dt.normalize()
            carregar_gold_to_postgresql(df_first_load, engine, TABELA_DESTINO_POSTGRESQL, 'replace')

            # Remove o primeiro arquivo da lista para que não seja carregado novamente
            remaining_files_to_load = files_to_load[1:]

        except Exception as e:
            logging.error(f"Erro crítico ao carregar o primeiro arquivo para criar a tabela: {e}", exc_info=True)
            logging.error("O carregamento será interrompido. Verifique o problema e tente novamente.")
            exit()

        # Carrega os arquivos restantes com 'append'
        if remaining_files_to_load:
            logging.info(f"Carregando os {len(remaining_files_to_load)} arquivos restantes com modo 'append'.")
            for file_path in remaining_files_to_load:
                df_rest = pd.read_parquet(file_path)
                if 'data_base' in df_rest.columns:
                    df_rest['data_base'] = pd.to_datetime(df_rest['data_base']).dt.normalize()
                carregar_gold_to_postgresql(df_rest, engine, TABELA_DESTINO_POSTGRESQL, 'append')
        else:
            logging.info("Apenas um arquivo agregado encontrado e carregado.")

    logging.info("--- Carregamento de Dados Gold Agregados para PostgreSQL CONCLUÍDO ---")
    logging.info("--- Script ETL: Gold to PostgreSQL (SCR.data) - FINALIZADO. ---")