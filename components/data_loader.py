# components/data_loader_bq.py

import logging
import pandas as pd
import json
import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import pearsonr, spearmanr

logger = logging.getLogger(__name__)

# --- Configurações do BigQuery ---
PROJECT_ID = "credtech-1"
# Dataset em uma região padrão do Google Cloud (ex: southamerica-east1)
DATASET_ID = "dataclean" 

# --- Função de Conexão com BigQuery ---
@st.cache_resource
def get_bigquery_client():
    """
    Cria e retorna um cliente BigQuery. A conexão é cacheada.
    """
    try:
        client = bigquery.Client(project=PROJECT_ID)
        client.query("SELECT 1") # Testa a conexão
        logger.info("Cliente BigQuery criado e cacheado com sucesso.")
        return client
    except Exception as e:
        logger.error(f"Erro crítico ao conectar ao BigQuery: {e}", exc_info=True)
        st.error("Não foi possível conectar ao BigQuery. Verifique a autenticação e as permissões.")
        st.stop()

# --- Função de Correção para Dados Corrompidos ---
def substituir_replacement_char(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.replace('�', 'ç', regex=False)
    return df

# --- Funções de Consulta Específicas para cada Análise ---

@st.cache_data(ttl=3600)
def get_dados_visao_geral_uf(_client: bigquery.Client) -> pd.DataFrame:
    """Busca dados já agregados por UF para a página 'Visão Geral por UF'."""
    logger.info("Executando query para Visão Geral por UF no BigQuery...")
    query = f"""
        SELECT
            uf,
            SUM(taxa_inadimplencia_final_segmento * total_carteira_ativa_segmento) / NULLIF(SUM(total_carteira_ativa_segmento), 0) AS taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) AS volume_carteira_total
        FROM
            `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        GROUP BY uf
        ORDER BY uf
    """
    try:
        return _client.query(query).to_dataframe()
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_visao_geral_uf: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de visão geral por UF.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_por_segmento(_client: bigquery.Client, segmento_dim: str) -> pd.DataFrame:
    """Busca dados agregados por uma dimensão de segmento dinâmica."""
    logger.info(f"Executando query agregada por '{segmento_dim}' no BigQuery...")
    colunas_permitidas = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse']
    if segmento_dim not in colunas_permitidas:
        st.error(f"Dimensão de análise '{segmento_dim}' inválida.")
        return pd.DataFrame()

    # Adiciona filtros para excluir combinações inválidas
    where_clause = ""
    if segmento_dim in ['ocupacao']:
        where_clause = "WHERE cliente = 'PF'"  # Ocupação só para PF
    elif segmento_dim in ['cnae_secao', 'cnae_subclasse']:
        where_clause = "WHERE cliente = 'PJ'"  # CNAE só para PJ

    query = f"""
        SELECT
            {segmento_dim},
            SUM(taxa_inadimplencia_final_segmento * total_carteira_ativa_segmento) / NULLIF(SUM(total_carteira_ativa_segmento), 0) AS taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) AS volume_carteira_total
        FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        {where_clause}
        GROUP BY {segmento_dim}
    """
    try:
        return _client.query(query).to_dataframe()
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_por_segmento: {e}", exc_info=True)
        st.error(f"Não foi possível carregar os dados para o segmento '{segmento_dim}'.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_top_n_segmento(_client: bigquery.Client, segmento_dim: str, top_n: int = 20, order_by: str = 'taxa_inadimplencia_media') -> pd.DataFrame:
    """
    Busca os Top N segmentos por uma dimensão, ordenados por uma métrica.
    """
    logger.info(f"Executando query Top {top_n} para '{segmento_dim}' ordenado por '{order_by}'...")

    # Validação de segurança
    colunas_permitidas = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse']
    order_by_permitido = ['taxa_inadimplencia_media', 'volume_carteira_total']
    if segmento_dim not in colunas_permitidas or order_by not in order_by_permitido:
        st.error("Parâmetros de análise inválidos.")
        return pd.DataFrame()

    # Adicionar filtros baseados no tipo de cliente
    where_clause = ""
    if segmento_dim == 'ocupacao':
        where_clause = "WHERE cliente = 'PF'"
    elif segmento_dim in ['cnae_secao', 'cnae_subclasse']:
        where_clause = "WHERE cliente = 'PJ'"

    query = f"""
        SELECT
            {segmento_dim},
            SUM(taxa_inadimplencia_final_segmento * total_carteira_ativa_segmento) / NULLIF(SUM(total_carteira_ativa_segmento), 0) AS taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) AS volume_carteira_total
        FROM
            `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        {where_clause}
        GROUP BY
            {segmento_dim}
        HAVING -- Ignora segmentos com volume irrelevante para uma análise de risco mais limpa
            SUM(total_carteira_ativa_segmento) > 1000 
        ORDER BY
            {order_by} DESC
        LIMIT {top_n}
    """
    try:
        df = _client.query(query).to_dataframe()
        df = substituir_replacement_char(df)
        return df
    except Exception as e:
        logger.error(f"Erro na query get_dados_top_n_segmento: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados do Top N.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_tendencia_temporal(_client: bigquery.Client) -> pd.DataFrame:
    """Busca e junta os dados de SCR e indicadores, já agregados por mês."""
    logger.info("Executando query para Tendência Temporal no BigQuery...")
    query = f"""
        WITH scr_mensal AS (
            SELECT
                DATE_TRUNC(data_base, MONTH) AS mes,
                SUM(taxa_inadimplencia_final_segmento * total_carteira_ativa_segmento) / 
                NULLIF(SUM(total_carteira_ativa_segmento), 0) AS taxa_inadimplencia_media
            FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
            WHERE DATE_TRUNC(data_base, MONTH) >= '2024-05-01'
            GROUP BY mes
        ),
        indicadores_mensal AS (
            SELECT
                DATE_TRUNC(data_base, MONTH) AS mes,
                AVG(taxa_desemprego) as taxa_desemprego,
                AVG(valor_ipca) as valor_ipca,
                AVG(taxa_selic_meta) as taxa_selic_meta
            FROM `{PROJECT_ID}.{DATASET_ID}.ft_indicadores_economicos_mensal`
            WHERE DATE_TRUNC(data_base, MONTH) >= '2024-05-01'
            GROUP BY mes
        )
        SELECT * FROM scr_mensal LEFT JOIN indicadores_mensal USING(mes) ORDER BY mes
    """
    try:
        return _client.query(query).to_dataframe()
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_tendencia_temporal: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados da tendência temporal.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_inadimplencia_por_cluster(_client: bigquery.Client) -> pd.DataFrame:
    """Busca dados de inadimplência agregados por cluster."""
    logger.info("Executando query de inadimplência por cluster no BigQuery...")
    query = f"""
        SELECT
            cluster_id,
            AVG(taxa_inadimplencia_final_segmento) as taxa_inadimplencia_media
        FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_segmentos_clusters`
        GROUP BY cluster_id
        ORDER BY cluster_id
    """
    try:
        df = _client.query(query).to_dataframe()
        df = substituir_replacement_char(df)  # Adicionar esta linha
        return df
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_inadimplencia_por_cluster: {e}", exc_info=True)
        st.error("Não foi possível carregar a análise de clusters.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_full_cluster_data(_client: bigquery.Client) -> pd.DataFrame:
    """Carrega a tabela COMPLETA de segmentos clusterizados (necessário para o gráfico de radar)."""
    logger.info("Carregando TODOS os dados da tabela 'ft_scr_segmentos_clusters'.")
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_segmentos_clusters`"
    try:
        df = _client.query(query).to_dataframe()
        df = substituir_replacement_char(df)  # Adicionar esta linha
        return df
    except GoogleAPICallError as e:
        logger.error(f"Erro ao carregar dados completos de cluster: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de clusterização.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_cluster_profiles(_client: bigquery.Client) -> pd.DataFrame:
    """Carrega a tabela de perfis de cluster (tabela pequena, SELECT * é aceitável)."""
    logger.info("Carregando perfis dos clusters (dim_cluster_profiles)...")
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_cluster_profiles`"
    try:
        df = _client.query(query).to_dataframe()
        df = substituir_replacement_char(df)  # Adicionar esta linha
        return df
    except GoogleAPICallError as e:
        logger.error(f"Erro ao carregar perfis de clusters: {e}", exc_info=True)
        st.error("Não foi possível carregar os perfis dos clusters.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_top_combinacoes_risco(_client: bigquery.Client, top_n: int = 20) -> pd.DataFrame:
    """Busca as top N combinações de risco com maior inadimplência."""
    logger.info(f"Executando query de Top {top_n} Combinações de Risco no BigQuery...")
    query = f"""
        SELECT
            CONCAT(cliente, ' - ', modalidade, ' - ', porte) as combinacao_risco,
            AVG(taxa_inadimplencia_final_segmento) as taxa_inadimplencia_media
        FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_segmentos_clusters`
        GROUP BY combinacao_risco
        ORDER BY taxa_inadimplencia_media DESC
        LIMIT {top_n}
    """
    try:
        df = _client.query(query).to_dataframe()
        df = substituir_replacement_char(df)  # Adicionar esta linha
        return df
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_top_combinacoes_risco: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de Top Combinações de Risco.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_comparativo_riscos(_client: bigquery.Client, comparison_dims: list) -> pd.DataFrame:
    """Busca dados agregados por uma lista de dimensões de comparação."""
    if not comparison_dims:
        return pd.DataFrame()
    logger.info(f"Executando query de comparação por {comparison_dims} no BigQuery...")
    colunas_permitidas = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao']
    for dim in comparison_dims:
        if dim not in colunas_permitidas:
            st.error(f"Dimensão de comparação '{dim}' inválida.")
            return pd.DataFrame()
    dims_sql = ", ".join(comparison_dims)
    query = f"""
        SELECT
            {dims_sql},
            AVG(taxa_inadimplencia_final_segmento) AS taxa_inadimplencia_media
        FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        GROUP BY {dims_sql}
        ORDER BY taxa_inadimplencia_media DESC
    """
    try:
        return _client.query(query).to_dataframe()
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_comparativo_riscos: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados para a comparação.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_top_n_segmento(_client: bigquery.Client, segmento_dim: str, top_n: int = 20, order_by: str = 'taxa_inadimplencia_media') -> pd.DataFrame:
    """
    Busca os Top N segmentos por uma dimensão, ordenados por uma métrica.
    """
    logger.info(f"Executando query Top {top_n} para '{segmento_dim}' ordenado por '{order_by}'...")

    # Validação de segurança
    colunas_permitidas = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse']
    order_by_permitido = ['taxa_inadimplencia_media', 'volume_carteira_total']
    if segmento_dim not in colunas_permitidas or order_by not in order_by_permitido:
        st.error("Parâmetros de análise inválidos.")
        return pd.DataFrame()

    # Adiciona filtros para excluir combinações inválidas
    where_clause = ""
    if segmento_dim in ['ocupacao']:
        where_clause = "WHERE cliente = 'PF'"  # Ocupação só para PF
    elif segmento_dim in ['cnae_secao', 'cnae_subclasse']:
        where_clause = "WHERE cliente = 'PJ'"  # CNAE só para PJ

    query = f"""
        SELECT
            {segmento_dim},
            SUM(taxa_inadimplencia_final_segmento * total_carteira_ativa_segmento) / NULLIF(SUM(total_carteira_ativa_segmento), 0) AS taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) AS volume_carteira_total
        FROM
            `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        {where_clause}
        GROUP BY
            {segmento_dim}
        HAVING -- Ignora segmentos com volume irrelevante para uma análise de risco mais limpa
            SUM(total_carteira_ativa_segmento) > 1000 
        ORDER BY
            {order_by} DESC
        LIMIT {top_n}
    """
    try:
        df = _client.query(query).to_dataframe()
        df = substituir_replacement_char(df)
        return df
    except Exception as e:
        logger.error(f"Erro na query get_dados_top_n_segmento: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados do Top N.")
        return pd.DataFrame()

# Em components/data_loader_bq.py

# Em components/data_loader_bq.py

@st.cache_data(ttl=3600)
def get_kpi_data(_client: bigquery.Client) -> pd.DataFrame:
    """
    Busca os principais KPIs (Big Numbers) para o mês mais recente,
    usando os nomes de colunas corretos da tabela final.
    """
    logger.info("Executando query de KPIs no BigQuery com colunas corrigidas...")
    
    # QUERY FINAL E CORRIGIDA de acordo com o esquema da sua tabela
    query = f"""
        WITH latest_data AS (
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
            WHERE data_base = (SELECT MAX(data_base) FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`)
        )
        SELECT
          -- KPI 1: Volume Total da Carteira
          SUM(total_carteira_ativa_segmento) AS volume_total,

          -- KPI 2: Taxa de Inadimplência Ponderada (calculada a partir dos totais)
          SUM(total_vencido_15d_segmento + total_inadimplida_arrastada_segmento) / NULLIF(SUM(total_carteira_ativa_segmento), 0) AS inadimplencia_geral_ponderada,

          -- KPI 3: Valor Total Inadimplente
          SUM(total_vencido_15d_segmento + total_inadimplida_arrastada_segmento) AS valor_total_inadimplente,

          -- KPI 4: Número Total de Operações
          SUM(contagem_subsegmentos) AS total_operacoes
        FROM
          latest_data
    """
    try:
        df = _client.query(query).to_dataframe()
        return df
    except Exception as e:
        logger.error(f"Erro na query get_kpi_data: {e}", exc_info=True)
        st.error("Não foi possível carregar os KPIs. Verifique os nomes das colunas na query.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_dados_visao_geral_uf(_client: bigquery.Client) -> pd.DataFrame:
    """
    Busca dados já agregados por UF para o mapa coroplético.
    Retorna UF, taxa de inadimplência e volume da carteira.
    """
    logger.info("Executando query para Visão Geral por UF no BigQuery...")
    query = f"""
        SELECT
            uf,
            SUM(taxa_inadimplencia_final_segmento * total_carteira_ativa_segmento) / NULLIF(SUM(total_carteira_ativa_segmento), 0) AS taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) AS volume_carteira_total
        FROM
            `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        GROUP BY uf
        ORDER BY uf
    """
    try:
        df = _client.query(query).to_dataframe()
        logger.info(f"Dados para o mapa carregados. {len(df)} linhas.")
        return df
    except GoogleAPICallError as e:
        logger.error(f"Erro na query get_dados_visao_geral_uf: {e}", exc_info=True)
        st.error("Não foi possível carregar os dados de visão geral por UF.")
        return pd.DataFrame()
@st.cache_data
def load_geojson_data(path: str) -> dict:
    """Carrega o arquivo GeoJSON em cache, tratando o erro de codificação."""
    try:
        with open(path, "r", encoding='latin-1') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"Arquivo GeoJSON não encontrado em '{path}'. Certifique-se de que 'uf.json' está na mesma pasta que Home.py.")
        return None
    except Exception as e:
        st.error(f"Erro ao ler o arquivo GeoJSON: {e}")
        return None
def calcular_correlacoes(df_temporal):
    correlacoes = {}
    df_clean = df_temporal.dropna()
    if len(df_clean) < 3: return correlacoes
    indicadores = ['taxa_desemprego', 'valor_ipca', 'taxa_selic_meta']
    for indicador in indicadores:
        if indicador in df_clean.columns:
            pearson_corr, pearson_p = pearsonr(df_clean['taxa_inadimplencia_media'], df_clean[indicador])
            correlacoes[indicador] = {'pearson': {'corr': pearson_corr, 'p_value': pearson_p}}
    return correlacoes

def interpretar_correlacao(corr_value):
    abs_corr = abs(corr_value)
    if abs_corr >= 0.7: return "Forte"
    elif abs_corr >= 0.5: return "Moderada"
    elif abs_corr >= 0.3: return "Fraca"
    else: return "Muito Fraca"
def calculate_metrics_for_period(df: pd.DataFrame, start_date: datetime, end_date: datetime, main_metric_col: str):
    """
    Calcula o valor médio e a mudança percentual para a métrica principal em um dado período.
    Args:
        df: DataFrame contendo os dados temporais.
        start_date: Data de início do período.
        end_date: Data de fim do período.
        main_metric_col: Nome da coluna que contém a métrica principal a ser analisada.
    Returns:
        Tupla (valor_medio, mudanca_percentual).
    """
    df_filtered = df[(df['mes'] >= start_date) & (df['mes'] <= end_date)].copy()
    if df_filtered.empty:
        return 0, 0

    avg_value = df_filtered[main_metric_col].mean()
    
    if len(df_filtered) >= 2:
        df_filtered = df_filtered.sort_values(by='mes')
        first_value = df_filtered[main_metric_col].iloc[0]
        last_value = df_filtered[main_metric_col].iloc[-1]
        
        if first_value != 0:
            percent_change = ((last_value - first_value) / first_value) * 100
        else:
            percent_change = 0
    else:
        percent_change = 0
    
    return avg_value, percent_change
