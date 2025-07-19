import logging
import urllib.parse

import pandas as pd
import psycopg2
import streamlit as st
from sqlalchemy import create_engine, text

# --- Configuração de Logging para este módulo ---
logger = logging.getLogger(__name__)

# --- Configurações de Conexão com PostgreSQL (SUAS CREDENCIAIS) ---
DB_USER = 'jjguilherme'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Funções de Carregamento de Dados ---

@st.cache_resource
def get_db_engine():
    """
    Cria e retorna uma engine SQLAlchemy para a conexão com o banco de dados PostgreSQL.
    A engine é cacheada para ser reutilizada em toda a sessão da aplicação.
    """
    try:
        engine = create_engine(CONN_STR)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Conexão com o banco de dados estabelecida e cacheada com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"Erro crítico ao conectar ao banco de dados: {e}", exc_info=True)
        st.error("Erro ao conectar ao banco de dados. Por favor, verifique as configurações e se o PostgreSQL está rodando.")
        st.stop()

@st.cache_data(ttl=3600) # Dados em cache por 1 hora
# REMOVIDOS OS PARÂMETROS DE FILTRO
def load_scr_aggr_data(_engine):
    """
    Carrega TODOS os dados agregados da tabela 'ft_scr_agregado_mensal' do PostgreSQL.
    Os dados são cacheados para evitar recargas desnecessárias.
    """
    logger.info("Carregando TODOS os dados de ft_scr_agregado_mensal (sem filtros na query).")
    try:
        query_str = "SELECT * FROM ft_scr_agregado_mensal" # Query simples, sem WHERE
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        logger.info(f"Dados da tabela 'ft_scr_agregado_mensal' carregados do BD. {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados de 'ft_scr_agregado_mensal': {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de inadimplência agregados (ft_scr_agregado_mensal).")
        return pd.DataFrame()

@st.cache_data(ttl=3600) # Dados em cache por 1 hora
# REMOVIDOS OS PARÂMETROS DE FILTRO
def load_indicadores_data(_engine):
    """
    Carrega TODOS os dados de indicadores econômicos da tabela 'ft_indicadores_economicos_mensal' do PostgreSQL.
    Os dados são cacheados para evitar recargas desnecessárias.
    """
    logger.info("Carregando TODOS os dados de ft_indicadores_economicos_mensal (sem filtros na query).")
    try:
        query_str = "SELECT * FROM ft_indicadores_economicos_mensal" # Query simples, sem WHERE
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        logger.info(f"Dados da tabela 'ft_indicadores_economicos_mensal' carregados do BD. {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados de 'ft_indicadores_economicos_mensal': {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de indicadores econômicos (ft_indicadores_economicos_mensal).")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_cluster_data(_engine):
    """
    Carrega os dados com os resultados da clusterização da tabela 'ft_scr_segmentos_clusters' do PostgreSQL.
    """
    logger.info("Carregando dados da tabela 'ft_scr_segmentos_clusters'.")
    try:
        query = "SELECT * FROM ft_scr_segmentos_clusters"
        df = pd.read_sql(query, _engine)

        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        if 'cluster_id' in df.columns:
            df['cluster_id'] = df['cluster_id'].astype(int)

        logger.info(f"Dados da tabela 'ft_scr_segmentos_clusters' carregados do BD. {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados de clusterização: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de clusterização (ft_scr_segmentos_clusters).")
        return pd.DataFrame()

@st.cache_data(ttl=3600) # Dados em cache por 1 hora
def load_cluster_profiles(_engine):
    """
    Carrega os perfis (centróides/modas) dos clusters da tabela 'dim_cluster_profiles' do PostgreSQL.
    """
    logger.info("Carregando perfis dos clusters da tabela 'dim_cluster_profiles'.")
    try:
        query = "SELECT * FROM dim_cluster_profiles"
        df = pd.read_sql(query, _engine)

        if 'cluster_id' in df.columns:
            df['cluster_id'] = df['cluster_id'].astype(int)

        logger.info(f"Perfis dos clusters carregados do BD. {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar perfis dos clusters: {e}", exc_info=True)
        st.error("Não foi possível carregar os perfis dos clusters (dim_cluster_profiles).")
        return pd.DataFrame()
# ... (outras funções, se houver, também sem parâmetros de filtro) ...