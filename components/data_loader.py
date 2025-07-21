import logging
import urllib.parse

import pandas as pd
import psycopg2
import streamlit as st
from sqlalchemy import create_engine, text

# --- Configuração de Logging para este módulo ---
logger = logging.getLogger(__name__)

# --- Configurações de Conexão com PostgreSQL (SUAS CREDENCIAIS) ---
DB_USER = 'rogerym'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Funções de Carregamento de Dados OTIMIZADAS ---

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

# === FUNÇÕES OTIMIZADAS USANDO VIEWS MATERIALIZADAS ===

@st.cache_data(ttl=1800)  # Cache por 30 minutos
def load_scr_aggregated_by_uf(_engine, uf_filter=None, date_filter=None):
    """
    Carrega dados pré-agregados por UF da view materializada.
    Muito mais rápido que a consulta original.
    """
    logger.info("Carregando dados agregados por UF da view materializada.")
    try:
        conditions = []
        if uf_filter and len(uf_filter) > 0:
            uf_list = "', '".join(uf_filter)
            conditions.append(f"uf IN ('{uf_list}')")
        
        if date_filter and len(date_filter) == 2:
            conditions.append(f"data_base >= '{date_filter[0]}'")
            conditions.append(f"data_base <= '{date_filter[1]}'")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query_str = f"SELECT * FROM mv_scr_agregado_uf{where_clause} ORDER BY uf, data_base"
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        logger.info(f"Dados agregados por UF carregados: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados agregados por UF: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados agregados por UF.")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def load_scr_temporal_trend(_engine, date_filter=None):
    """
    Carrega tendência temporal pré-agregada da view materializada.
    """
    logger.info("Carregando tendência temporal da view materializada.")
    try:
        conditions = []
        if date_filter and len(date_filter) == 2:
            conditions.append(f"mes >= '{date_filter[0]}'")
            conditions.append(f"mes <= '{date_filter[1]}'")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query_str = f"SELECT * FROM mv_scr_tendencia_mensal{where_clause} ORDER BY mes"
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        if 'mes' in df.columns:
            df['mes'] = pd.to_datetime(df['mes']).dt.normalize()
            # Renomeia para compatibilidade com código existente
            df.rename(columns={'mes': 'data_base'}, inplace=True)

        logger.info(f"Tendência temporal carregada: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar tendência temporal: {e}", exc_info=True)
        st.error("Não foi possível carregar a tendência temporal.")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
@st.cache_data(ttl=1800)
def load_scr_by_segments(_engine, segment_filter=None, limit=1000):
    """
    Carrega dados por segmento da view materializada.
    """
    logger.info(f"Carregando dados por segmento (limite: {limit}).")
    try:
        conditions = []
        if segment_filter:
            if 'cliente' in segment_filter:
                conditions.append(f"cliente = '{segment_filter['cliente']}'")
            if 'modalidade' in segment_filter:
                conditions.append(f"modalidade = '{segment_filter['modalidade']}'")
            if 'uf' in segment_filter:
                conditions.append(f"uf = '{segment_filter['uf']}'")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query_str = f"""
        SELECT * FROM mv_scr_agregado_segmentos
        {where_clause}
        ORDER BY total_carteira_ativa_segmento DESC
        LIMIT {limit}
        """
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        logger.info(f"Dados por segmento carregados: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados por segmento: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados por segmento.")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def load_top_risk_combinations(_engine, limit=20):
    """
    Carrega top combinações de risco da view materializada.
    """
    logger.info(f"Carregando top {limit} combinações de risco.")
    try:
        query_str = f"""
        SELECT * FROM mv_scr_top_combinacoes_risco
        ORDER BY taxa_inadimplencia_media DESC
        LIMIT {limit}
        """
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        logger.info(f"Top combinações de risco carregadas: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar top combinações de risco: {e}", exc_info=True)
        st.error("Não foi possível carregar as combinações de risco.")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def load_indicadores_summary(_engine, date_filter=None):
    """
    Carrega resumo dos indicadores econômicos da view materializada.
    """
    logger.info("Carregando resumo dos indicadores econômicos.")
    try:
        conditions = []
        if date_filter and len(date_filter) == 2:
            conditions.append(f"mes >= '{date_filter[0]}'")
            conditions.append(f"mes <= '{date_filter[1]}'")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query_str = f"SELECT * FROM mv_indicadores_economicos_resumo{where_clause} ORDER BY mes"
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        if 'mes' in df.columns:
            df['mes'] = pd.to_datetime(df['mes']).dt.normalize()
            # Renomeia para compatibilidade
            df.rename(columns={'mes': 'data_base'}, inplace=True)

        logger.info(f"Resumo dos indicadores carregado: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar resumo dos indicadores: {e}", exc_info=True)
        st.error("Não foi possível carregar o resumo dos indicadores econômicos.")
        return pd.DataFrame()

# === FUNÇÕES PARA ANÁLISE DETALHADA (quando necessário) ===

@st.cache_data(ttl=1800)
def load_scr_filtered_data(_engine, start_date=None, end_date=None, uf_filter=None, limit=5000):
    """
    Carrega dados filtrados da tabela principal (para análises detalhadas).
    Usa LIMIT para evitar sobrecarga.
    """
    logger.info(f"Carregando dados filtrados (limite: {limit}).")
    try:
        conditions = []
        if start_date:
            conditions.append(f"data_base >= '{start_date}'")
        if end_date:
            conditions.append(f"data_base <= '{end_date}'")
        if uf_filter and len(uf_filter) > 0:
            uf_list = "', '".join(uf_filter)
            conditions.append(f"uf IN ('{uf_list}')")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query_str = f"""
        SELECT uf, data_base, cliente, modalidade, ocupacao, porte,
               taxa_inadimplencia_final_segmento, 
               total_carteira_ativa_segmento,
               perc_ativo_problematico_final_segmento
        FROM ft_scr_agregado_mensal
        {where_clause}
        ORDER BY data_base DESC, total_carteira_ativa_segmento DESC
        LIMIT {limit}
        """
        query = text(query_str)
        df = pd.read_sql(query, _engine)

        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        logger.info(f"Dados filtrados carregados: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados filtrados: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados filtrados.")
        return pd.DataFrame()

# === FUNÇÕES PARA CLUSTERS (mantidas como estavam) ===

@st.cache_data(ttl=3600)
def load_cluster_data(_engine):
    """
    Carrega os dados com os resultados da clusterização.
    """
    logger.info("Carregando dados da tabela 'ft_scr_segmentos_clusters'.")
    try:
        query = "SELECT * FROM ft_scr_segmentos_clusters ORDER BY data_base DESC, cluster_id"
        df = pd.read_sql(query, _engine)

        if 'data_base' in df.columns:
            df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        if 'cluster_id' in df.columns:
            df['cluster_id'] = df['cluster_id'].astype(int)

        logger.info(f"Dados de clusterização carregados: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados de clusterização: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de clusterização.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_cluster_profiles(_engine):
    """
    Carrega os perfis dos clusters.
    """
    logger.info("Carregando perfis dos clusters da tabela 'dim_cluster_profiles'.")
    try:
        query = "SELECT * FROM dim_cluster_profiles ORDER BY cluster_id"
        df = pd.read_sql(query, _engine)

        if 'cluster_id' in df.columns:
            df['cluster_id'] = df['cluster_id'].astype(int)

        logger.info(f"Perfis dos clusters carregados: {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar perfis dos clusters: {e}", exc_info=True)
        st.error("Não foi possível carregar os perfis dos clusters.")
        return pd.DataFrame()

# === FUNÇÃO PARA VERIFICAR VIEWS MATERIALIZADAS ===

@st.cache_data(ttl=300)  # Cache por 5 minutos
def check_materialized_views(_engine):
    """
    Verifica se as views materializadas existem e estão populadas.
    """
    try:
        query = """
        SELECT matviewname, ispopulated 
        FROM pg_matviews 
        WHERE matviewname LIKE 'mv_%'
        ORDER BY matviewname
        """
        df = pd.read_sql(query, _engine)
        return df
    except Exception as e:
        logger.error(f"Erro ao verificar views materializadas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)  # Cache por 5 minutos
def verify_materialized_views(_engine):
    """
    Verifica se as views materializadas existem e retorna status.
    """
    try:
        query = """
        SELECT matviewname, ispopulated 
        FROM pg_matviews 
        WHERE matviewname LIKE 'mv_%'
        ORDER BY matviewname
        """
        df = pd.read_sql(query, _engine)
        
        if len(df) == 0:
            logger.error("Views materializadas não encontradas!")
            st.error("Views materializadas não encontradas! Execute o script create_materialized_views.py primeiro.")
            return False
            
        unpopulated = df[~df['ispopulated']]
        if len(unpopulated) > 0:
            logger.error(f"Views não populadas: {unpopulated['matviewname'].tolist()}")
            st.error(f"Views não populadas: {unpopulated['matviewname'].tolist()}. Execute refresh_materialized_views.py")
            return False
            
        logger.info(f"✓ {len(df)} views materializadas encontradas e populadas")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao verificar views materializadas: {e}")
        st.error("Erro ao verificar views materializadas. Verifique a conexão com o banco.")
        return False