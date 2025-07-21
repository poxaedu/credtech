import logging
import os
import urllib.parse

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text, types

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Script de Clusterização (Gold -> PostgreSQL) - Iniciado.")

# --- Configurações de Conexão com PostgreSQL (SUAS CREDENCIAIS) ---
DB_USER = 'rogerym'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Nomes das Tabelas ---
SOURCE_TABLE_NAME = 'ft_scr_agregado_mensal'
TARGET_TABLE_NAME = 'ft_scr_segmentos_clusters' # Tabela para os resultados da clusterização (cluster_id por segmento)
PROFILE_TABLE_NAME = 'dim_cluster_profiles' # Nova tabela para os perfis dos clusters (centróides/modas)

# --- Parâmetros de Clusterização ---
N_CLUSTERS = 4 # Número de clusters (K) - ajuste conforme sua análise exploratória
RANDOM_STATE = 42 # Para reprodutibilidade do K-Means

# --- Funções Auxiliares ---

def get_db_engine():
    """Cria e retorna uma engine SQLAlchemy para a conexão com o banco de dados."""
    try:
        engine = create_engine(CONN_STR)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logger.critical(f"Erro crítico ao conectar ao banco de dados: {e}", exc_info=True)
        logger.critical("Verifique as credenciais do banco, host, porta e se o serviço do PostgreSQL está rodando.")
        raise

def load_data_from_db(engine, table_name):
    """Carrega dados de uma tabela do PostgreSQL."""
    logger.info(f"Carregando dados da tabela '{table_name}'...")
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()
        logger.info(f"Dados da tabela '{table_name}' carregados. {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados da tabela '{table_name}': {e}", exc_info=True)
        raise

def save_to_db(df: pd.DataFrame, engine, table_name: str, if_exists_mode: str = 'replace'):
    """Salva um DataFrame no PostgreSQL."""
    logger.info(f"Salvando {len(df)} linhas na tabela '{table_name}' no PostgreSQL (modo: {if_exists_mode}).")
    try:
        # Adaptação para Nomes de Colunas SQL (se necessário, já deve estar limpo da Gold)
        df.columns = [col.lower().replace('.', '_').replace(' ', '_').replace('/', '_').replace('-', '_').replace('(', '').replace(')', '') for col in df.columns]

        # Mapeamento de tipos para garantir consistência no BD
        dtype_mapping = {
            'object': types.VARCHAR(255), 'category': types.VARCHAR(255),
            'datetime64[ns]': types.Date, 'int64': types.BigInteger, 'int32': types.Integer,
            'float64': types.Float(precision=53), 'float32': types.Float(precision=24),
            'bool': types.Boolean
        }
        table_dypes = {col: dtype_mapping.get(str(df[col].dtype), None) for col in df.columns}
        table_dypes = {k: v for k, v in table_dypes.items() if v is not None}

        # Adicionar tipo específico para o cluster_id se existir
        if 'cluster_id' in df.columns:
            table_dypes['cluster_id'] = types.Integer

        df.to_sql(
            table_name,
            con=engine,
            if_exists=if_exists_mode,
            index=False,
            chunksize=5000,
            dtype=table_dypes
        )
        logger.info(f"Dados salvos com sucesso na tabela '{table_name}'.")
    except Exception as e:
        logger.error(f"Erro ao salvar dados na tabela '{table_name}': {e}", exc_info=True)
        raise

# --- Lógica Principal de Clusterização ---
if __name__ == '__main__':
    logger.info("--- INICIANDO PROCESSO DE CLUSTERIZAÇÃO ---")

    engine = get_db_engine()

    # 1. Carregar dados da camada Gold (ft_scr_agregado_mensal)
    df_gold = load_data_from_db(engine, SOURCE_TABLE_NAME)

    if df_gold.empty:
        logger.error("DataFrame carregado vazio. Impossível prosseguir com a clusterização.")
        exit(1)

    # 2. Seleção de Features para Clusterização
    # Colunas numéricas para K-Means
    features_for_clustering_numeric = [
        'total_carteira_ativa_segmento',
        'taxa_inadimplencia_final_segmento',
        'perc_ativo_problematico_final_segmento',
        'contagem_subsegmentos',
        # Adicione outras features numéricas relevantes aqui
        # Ex: 'numero_de_operacoes',
    ]

    # Colunas categóricas para perfil (serão tratadas com moda)
    features_for_profiling_categorical = [
        'uf',
        'cliente', # Tipo de cliente (PF/PJ)
        'modalidade',
        'ocupacao',
        'porte',
        'cnae_secao',
        'cnae_subclasse',
    ]

    # Filtrar o DataFrame apenas com as features selecionadas e remover nulos
    # Apenas as features numéricas são usadas para o K-Means
    df_clustering_input = df_gold[features_for_clustering_numeric].copy()
    initial_rows = len(df_clustering_input)
    df_clustering_input.dropna(inplace=True) # K-Means não lida com nulos
    if len(df_clustering_input) < initial_rows:
        logger.warning(f"Removidas {initial_rows - len(df_clustering_input)} linhas com valores nulos nas features de clusterização.")
    if df_clustering_input.empty:
        logger.error("DataFrame de clusterização vazio após remoção de nulos. Impossível prosseguir.")
        exit(1)

    # 3. Pré-processamento: Escalonamento das Features Numéricas
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df_clustering_input)
    logger.info("Features numéricas escalonadas com sucesso.")

    # 4. Aplicação do K-Means
    logger.info(f"Aplicando K-Means com {N_CLUSTERS} clusters...")
    kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE, n_init=10, verbose=0)
    df_gold['cluster_id'] = kmeans.fit_predict(scaled_features) # Atribui o cluster_id ao DataFrame original
    logger.info("Clusterização concluída. IDs de cluster atribuídos ao DataFrame principal.")

    # 5. Salvar os resultados da clusterização por segmento (ft_scr_segmentos_clusters)
    df_to_save_segments = df_gold.copy()
    try:
        save_to_db(df_to_save_segments, engine, TARGET_TABLE_NAME, if_exists_mode='replace')
        logger.info(f"Resultados da clusterização por segmento salvos na tabela '{TARGET_TABLE_NAME}'.")
    except Exception as e:
        logger.error("Falha ao salvar resultados da clusterização por segmento no banco de dados.", exc_info=True)
        exit(1)

    # 6. Calcular e Salvar os Perfis dos Clusters (dim_cluster_profiles)
    logger.info("Calculando perfis dos clusters...")

    # Perfis numéricos (centróides invertidos)
    cluster_centers_original_scale = scaler.inverse_transform(kmeans.cluster_centers_)
    df_profile_numeric = pd.DataFrame(cluster_centers_original_scale, columns=features_for_clustering_numeric)
    df_profile_numeric['cluster_id'] = range(N_CLUSTERS)

    # Perfis categóricos (moda)
    df_profile_categorical = pd.DataFrame()
    if features_for_profiling_categorical:
        for col in features_for_profiling_categorical:
            # Calcula a moda para cada cluster. A moda pode retornar múltiplos valores, pegamos o primeiro.
            # Usamos value_counts().idxmax() para a moda mais frequente
            mode_series = df_gold.groupby('cluster_id')[col].apply(lambda x: x.mode()[0] if not x.mode().empty else None)
            df_profile_categorical[col] = mode_series

        df_profile_categorical['cluster_id'] = df_profile_categorical.index # cluster_id já é o índice
        df_profile_categorical.reset_index(drop=True, inplace=True) # Resetar para merge

    # Juntar perfis numéricos e categóricos
    if not df_profile_categorical.empty:
        df_cluster_profiles = pd.merge(df_profile_numeric, df_profile_categorical, on='cluster_id', how='left')
    else:
        df_cluster_profiles = df_profile_numeric.copy()

    # Salvar os perfis dos clusters
    try:
        save_to_db(df_cluster_profiles, engine, PROFILE_TABLE_NAME, if_exists_mode='replace')
        logger.info(f"Perfis dos clusters salvos na tabela '{PROFILE_TABLE_NAME}'.")
    except Exception as e:
        logger.error("Falha ao salvar perfis dos clusters no banco de dados.", exc_info=True)
        exit(1)

    logger.info("--- PROCESSO DE CLUSTERIZAÇÃO E PERFILAGEM CONCLUÍDO ---")