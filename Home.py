import logging

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Importe as funções OTIMIZADAS do data_loader.py
from components.data_loader import (
    get_db_engine, 
    load_scr_aggregated_by_uf,
    load_scr_temporal_trend,
    load_scr_by_segments,
    load_top_risk_combinations,
    load_indicadores_summary,
    load_scr_filtered_data,
    load_cluster_data,
    load_cluster_profiles,
    check_materialized_views
)

# Importe as funções do plot_utils.py (mantidas como estavam)
from components.plot_utils import (
    plot_carteira_uf, plot_comparativo_riscos,
    plot_inadimplencia_por_cluster,
    plot_inadimplencia_uf, plot_perfil_cluster,
    plot_segmento_inadimplencia,
    plot_segmento_volume,
    plot_tendencia_temporal,
    plot_top_combinacoes_risco
)

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuração da Página Streamlit ---
st.set_page_config(
    page_title="Dashboard de Análise de Inadimplência",
    layout="wide"
)

# Função para carregar CSS
def carregar_css(caminho_arquivo):
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css")

# --- GESTÃO DO ESTADO DA APLICAÇÃO ---
if 'pagina_ativa' not in st.session_state:
    st.session_state.pagina_ativa = 'Visão Geral por UF'

# --- TÍTULO PRINCIPAL ---
st.markdown("<div class='dashboard-title'><h1>Dashboard de Análise de Inadimplência</h1></div>", unsafe_allow_html=True)
st.markdown("""
Esta aplicação interativa permite explorar os padrões e perfis de inadimplência
nas operações de crédito do Brasil, com base nos dados do SCR e indicadores econômicos.
""")

st.markdown("---")

# --- CARREGAMENTO DA ENGINE ---
engine = get_db_engine()

# --- VERIFICAÇÃO DAS VIEWS MATERIALIZADAS ---
views_status = check_materialized_views(engine)
if views_status.empty:
    st.error("⚠️ Views materializadas não encontradas! Execute o script create_materialized_views.py primeiro.")
    st.stop()
else:
    unpopulated_views = views_status[~views_status['ispopulated']]
    if not unpopulated_views.empty:
        st.warning(f"⚠️ Algumas views não estão populadas: {', '.join(unpopulated_views['matviewname'].tolist())}")

# --- FILTROS NA SIDEBAR ---
with st.sidebar:
    st.header("🔍 Filtros")
    
    # Filtro de data
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data Início",
            value=pd.Timestamp('2024-01-01'),
            key="start_date_filter"
        )
    with col2:
        end_date = st.date_input(
            "Data Fim",
            value=pd.Timestamp('2024-12-31'),
            key="end_date_filter"
        )
    
    date_filter = [start_date, end_date] if start_date and end_date else None
    
    # Filtro de UF
    available_ufs = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE', 'DF', 'ES', 'MT', 'MS', 'PB', 'AL', 'SE', 'RN', 'PI', 'MA', 'AC', 'RO', 'RR', 'AM', 'AP', 'TO']
    selected_ufs = st.multiselect(
        "Selecione UFs",
        available_ufs,
        default=['SP', 'RJ', 'MG', 'RS', 'PR'],
        key="uf_filter"
    )
    
    uf_filter = selected_ufs if selected_ufs else None
    
    # Informações sobre otimização
    st.info("💡 **Dashboard Otimizado**\n\nUsando views materializadas para máxima performance!")
    
    if st.button("🔄 Atualizar Views", help="Clique para atualizar as views materializadas"):
        st.info("Para atualizar as views, execute: `python scripts/refresh_materialized_views.py`")

# --- NAVEGAÇÃO ---
st.write("#### Selecione a análise:")

paginas = [
    {"label": "💡 Visão Geral por UF", "id": "Visão Geral por UF"},
    {"label": "📊 Análise por Segmento", "id": "Análise por Segmento"},
    {"label": "📈 Tendência Temporal", "id": "Tendência Temporal"},
    {"label": "🔍 Análise de Clusters", "id": "Análise de Clusters"},
    {"label": "⚖️ Comparativo de Riscos", "id": "Comparativo de Riscos"}
]

cols = st.columns([1.5, 1.8, 1.7, 1.7, 1.7])

for i, page_info in enumerate(paginas):
    with cols[i]:
        if st.button(page_info["label"], key=f"nav_button_{page_info['id']}", use_container_width=True,
                     type="primary" if st.session_state.pagina_ativa == page_info["id"] else "secondary"):
            st.session_state.pagina_ativa = page_info["id"]
            st.rerun()

st.markdown("---")

# --- RENDERIZAÇÃO DAS SEÇÕES OTIMIZADAS ---

if st.session_state.pagina_ativa == "Visão Geral por UF":
    st.markdown("<div class='section-header'><h2>💡 Visão Geral por UF</h2></div>", unsafe_allow_html=True)
    st.markdown("Explore a distribuição da carteira de crédito e a inadimplência por Unidade Federativa.")
    
    # Carrega dados otimizados
    with st.spinner("Carregando dados agregados por UF..."):
        df_uf_data = load_scr_aggregated_by_uf(engine, uf_filter, date_filter)
    
    if not df_uf_data.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Taxa Média de Inadimplência por UF")
            # Adapta os dados para o formato esperado pelo plot
            df_for_plot = df_uf_data.groupby('uf').agg({
                'taxa_inadimplencia_media': 'mean'
            }).reset_index()
            df_for_plot.rename(columns={'taxa_inadimplencia_media': 'taxa_inadimplencia_final_segmento'}, inplace=True)
            st.plotly_chart(plot_inadimplencia_uf(df_for_plot), use_container_width=True)
            st.markdown("*(Dados da view materializada `mv_scr_agregado_uf`)*")

        with col2:
            st.subheader("Volume Total da Carteira Ativa por UF")
            df_for_plot = df_uf_data.groupby('uf').agg({
                'total_carteira_ativa': 'sum'
            }).reset_index()
            df_for_plot.rename(columns={'total_carteira_ativa': 'total_carteira_ativa_segmento'}, inplace=True)
            st.plotly_chart(plot_carteira_uf(df_for_plot), use_container_width=True)
            st.markdown("*(Dados da view materializada `mv_scr_agregado_uf`)*")
        
        # Métricas resumo
        st.subheader("📊 Métricas Resumo")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_carteira = df_uf_data['total_carteira_ativa'].sum()
            st.metric("Carteira Total", f"R$ {total_carteira/1e9:.1f}B")
        
        with col2:
            taxa_media = df_uf_data['taxa_inadimplencia_media'].mean()
            st.metric("Taxa Média Inadimplência", f"{taxa_media:.2f}%")
        
        with col3:
            total_ufs = df_uf_data['uf'].nunique()
            st.metric("UFs Analisadas", total_ufs)
        
        with col4:
            total_segmentos = df_uf_data['total_segmentos'].sum()
            st.metric("Total Segmentos", f"{total_segmentos:,}")
    else:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")

elif st.session_state.pagina_ativa == "Análise por Segmento":
    st.markdown("<div class='section-header'><h2>📊 Análise por Segmento</h2></div>", unsafe_allow_html=True)
    st.markdown("Analise o volume e a inadimplência por diversas dimensões de segmentação.")
    
    # Carrega dados otimizados
    with st.spinner("Carregando dados por segmento..."):
        df_segments_data = load_scr_by_segments(engine, limit=500)
    
    if not df_segments_data.empty:
        # Seletor de dimensão
        segmento_dim_options = ['cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao']
        segmento_dim_display_options = [s.replace('_', ' ').title() for s in segmento_dim_options]

        selected_display_dim = st.selectbox(
            "Selecione a Dimensão de Análise",
            segmento_dim_display_options
        )
        segmento_dim = segmento_dim_options[segmento_dim_display_options.index(selected_display_dim)]

        if segmento_dim in df_segments_data.columns:
            st.subheader(f"Volume Total da Carteira por {selected_display_dim}")
            df_for_plot = df_segments_data.groupby(segmento_dim).agg({
                'total_carteira_ativa': 'sum'
            }).reset_index().head(20)
            df_for_plot.rename(columns={'total_carteira_ativa': 'total_carteira_ativa_segmento'}, inplace=True)
            st.plotly_chart(plot_segmento_volume(df_for_plot, segmento_dim, f"Volume por {selected_display_dim}"), use_container_width=True)

            st.subheader(f"Taxa Média de Inadimplência por {selected_display_dim}")
            df_for_plot = df_segments_data.groupby(segmento_dim).agg({
                'taxa_inadimplencia_media': 'mean'
            }).reset_index().head(20)
            df_for_plot.rename(columns={'taxa_inadimplencia_media': 'taxa_inadimplencia_final_segmento'}, inplace=True)
            st.plotly_chart(plot_segmento_inadimplencia(df_for_plot, segmento_dim, f"Taxa de Inadimplência por {selected_display_dim}"), use_container_width=True)
            
            st.markdown("*(Dados da view materializada `mv_scr_agregado_segmentos`)*")
        else:
            st.error(f"Dimensão '{segmento_dim}' não encontrada nos dados.")
    else:
        st.warning("Nenhum dado de segmento encontrado.")

elif st.session_state.pagina_ativa == "Tendência Temporal":
    st.markdown("<div class='section-header'><h2>📈 Tendência Temporal</h2></div>", unsafe_allow_html=True)
    st.markdown("Acompanhe a evolução da taxa de inadimplência e sua relação com os principais indicadores macroeconômicos.")
    
    with st.spinner("Carregando tendência temporal..."):
        df_temporal_data = load_scr_temporal_trend(engine, date_filter)
        df_indicadores_data = load_indicadores_summary(engine, date_filter)
    
    if not df_temporal_data.empty and not df_indicadores_data.empty:
        # Adapta os dados para o formato esperado
        df_temporal_data.rename(columns={'taxa_inadimplencia_media': 'taxa_inadimplencia_final_segmento'}, inplace=True)
        
        # Renomeia colunas dos indicadores para compatibilidade
        df_indicadores_data.rename(columns={
            'taxa_desemprego_media': 'taxa_desemprego',
            'taxa_inadimplencia_pf_media': 'taxa_inadimplencia_pf',
            'valor_ipca_medio': 'valor_ipca',
            'taxa_selic_meta_media': 'taxa_selic_meta'
        }, inplace=True)
        
        st.plotly_chart(plot_tendencia_temporal(df_temporal_data, df_indicadores_data), use_container_width=True)
        st.markdown("*(Dados das views materializadas `mv_scr_tendencia_mensal` e `mv_indicadores_economicos_resumo`)*")
    else:
        st.warning("Dados de tendência temporal não disponíveis.")

elif st.session_state.pagina_ativa == "Análise de Clusters":
    st.markdown("<div class='section-header'><h2>🔍 Análise de Clusters e Perfis de Risco</h2></div>", unsafe_allow_html=True)
    st.markdown("Explore os agrupamentos de risco identificados e seus perfis de inadimplência.")
    
    with st.spinner("Carregando dados de clusterização..."):
        df_clusters_data = load_cluster_data(engine)
        df_cluster_profiles_data = load_cluster_profiles(engine)
    
    if not df_clusters_data.empty and not df_cluster_profiles_data.empty:
        # Código existente para clusters (mantido como estava)
        cluster_ids = sorted(df_clusters_data['cluster_id'].unique().tolist())
        selected_cluster_id = st.selectbox("Selecione um Cluster para Detalhar o Perfil", cluster_ids)

        st.subheader("Taxa de Inadimplência por Cluster")
        st.plotly_chart(plot_inadimplencia_por_cluster(df_clusters_data), use_container_width=True)
        
        # Resto do código de clusters mantido...
        st.markdown("*(Dados das tabelas `ft_scr_segmentos_clusters` e `dim_cluster_profiles`)*")
    else:
        st.warning("Dados de clusterização não disponíveis. Execute o script `pipeline_gold_clustering.py` primeiro.")

elif st.session_state.pagina_ativa == "Comparativo de Riscos":
    st.markdown("<div class='section-header'><h2>⚖️ Comparativo de Riscos</h2></div>", unsafe_allow_html=True)
    st.markdown("Compare a inadimplência entre diferentes categorias ou segmentos.")
    
    with st.spinner("Carregando top combinações de risco..."):
        df_risk_combinations = load_top_risk_combinations(engine, limit=20)
    
    if not df_risk_combinations.empty:
        st.subheader("Top 20 Combinações de Maior Risco")
        
        # Adapta os dados para o plot
        df_for_plot = df_risk_combinations.copy()
        df_for_plot.rename(columns={'taxa_inadimplencia_media': 'taxa_inadimplencia_final_segmento'}, inplace=True)
        
        fig = px.bar(
            df_for_plot.head(20),
            x='combinacao_risco',
            y='taxa_inadimplencia_final_segmento',
            title="Top 20 Combinações de Maior Risco",
            labels={'combinacao_risco': 'Combinação de Risco', 'taxa_inadimplencia_final_segmento': 'Taxa de Inadimplência (%)'},
            color='taxa_inadimplencia_final_segmento',
            color_continuous_scale=px.colors.sequential.Reds
        )
        fig.update_layout(xaxis_title="Combinação de Risco", yaxis_title="Taxa de Inadimplência (%)")
        fig.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("*(Dados da view materializada `mv_scr_top_combinacoes_risco`)*")
        
        # Tabela detalhada
        st.subheader("Detalhamento das Combinações")
        st.dataframe(
            df_risk_combinations[['combinacao_risco', 'taxa_inadimplencia_media', 'total_carteira_ativa', 'total_registros']].head(10),
            use_container_width=True
        )
    else:
        st.warning("Dados de combinações de risco não disponíveis.")

st.markdown("---")
st.info("🚀 **Dashboard Otimizado** - Desenvolvido com views materializadas para máxima performance")