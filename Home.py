# Home.py

import logging
import pandas as pd
import streamlit as st

# Importa as novas funções do data_loader do BigQuery
from components.data_loader import (
    get_bigquery_client,
    get_dados_visao_geral_uf,
    get_dados_por_segmento,
    get_dados_tendencia_temporal,
    get_dados_inadimplencia_por_cluster,
    get_top_combinacoes_risco,
    load_full_cluster_data,
    load_cluster_profiles,
    get_top_combinacoes_risco,
    get_dados_comparativo_riscos,
    get_kpi_data,
    get_dados_top_n_segmento,
)
# Importa as funções de plotagem já simplificadas
from components.plot_utils import (
    plot_carteira_uf, 
    plot_inadimplencia_uf,
    plot_segmento_inadimplencia,
    plot_segmento_volume,
    plot_tendencia_temporal,
    plot_inadimplencia_por_cluster,
    plot_perfil_cluster,
    plot_top_combinacoes_risco,
    plot_top_segmento_horizontal,
    plot_comparativo_riscos
)

# --- Configuração de Logging e Página ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Dashboard de Análise de Risco", layout="wide")

def carregar_css(caminho_arquivo):
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css")

if 'pagina_ativa' not in st.session_state:
    st.session_state.pagina_ativa = 'Visão Geral por UF'

# --- TÍTULO E NAVEGAÇÃO ---
st.markdown("<div class='dashboard-title'><h1>Dashboard de Análise de Risco de Crédito</h1></div>", unsafe_allow_html=True)
st.markdown("<div class='dashboard-subtitle' style= text-align=center><h2>Uma visão abrangente dos riscos de crédito no Brasil</h2></div>", unsafe_allow_html=True)
st.markdown("<br></br>", unsafe_allow_html=True)
st.write("### Selecione a Análise:")

paginas = [
    {"label": "🏡 Home", "id": "Home"},
    {"label": "💡 Visão Geral por UF", "id": "Visão Geral por UF"},
    {"label": "📊 Análise por Segmento", "id": "Análise por Segmento"},
    {"label": "📈 Tendência Temporal", "id": "Tendência Temporal"},
    {"label": "🔍 Análise de Clusters", "id": "Análise de Clusters"},
    {"label": "⚖️ Comparativo de Riscos", "id": "Comparativo de Riscos"}
]
cols = st.columns(len(paginas))
for i, page_info in enumerate(paginas):
    with cols[i]:
        if st.button(page_info["label"], key=f"nav_button_{page_info['id']}", use_container_width=True, type="primary" if st.session_state.pagina_ativa == page_info["id"] else "secondary"):
            st.session_state.pagina_ativa = page_info["id"]
            st.rerun()

# Conecta ao BigQuery uma única vez no início
try:
    client = get_bigquery_client()
except Exception as e:
    st.error("Falha na inicialização do App. Não foi possível conectar ao BigQuery.")
    st.stop()


# --- RENDERIZAÇÃO DAS PÁGINAS ---
if st.session_state.pagina_ativa == "Home":
    def format_big_number(num):
        """Formata um número e retorna o valor e o sufixo (K, Mi, Bi, Tri) separadamente."""
        if num is None or pd.isna(num):
            return "N/A", ""
        num = float(num)
        if abs(num) >= 1e12:
            return f"{num / 1e12:.2f}", "Tri"
        if abs(num) >= 1e9:
            return f"{num / 1e9:.2f}", "Bi"
        if abs(num) >= 1e6:
            return f"{num / 1e6:.2f}", "Mi"
        if abs(num) >= 1e3:
            return f"{num / 1e3:,.0f}".replace(",", "."), "Mil"
        return f"{num:,.0f}".replace(",", "."), ""

    st.markdown("<div class='section-header'><h2>Resumo Executivo</h2></div>", unsafe_allow_html=True)
    with st.spinner("Buscando dados agregados no BigQuery..."):
            kpi_data = get_kpi_data(client)
    # Verifica se os dados foram carregados antes de tentar exibi-los
    kpi_data = get_kpi_data(client)

    if not kpi_data.empty:
        # Extrai os valores
        volume = kpi_data['volume_total'].iloc[0]
        taxa_inadimplencia = kpi_data['inadimplencia_geral_ponderada'].iloc[0]
        valor_inadimplente = kpi_data['valor_total_inadimplente'].iloc[0]
        operacoes = kpi_data['total_operacoes'].iloc[0]

        # Formata os números usando a nova função
        volume_val, volume_sufixo = format_big_number(volume)
        inadimplente_val, inadimplente_sufixo = format_big_number(valor_inadimplente)
        operacoes_val, operacoes_sufixo = format_big_number(operacoes)

        # Cria as 4 colunas
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        with kpi1:
            st.markdown(f"""
            <div class="financial-metric-item">
                <div class="financial-metric-title">Volume Total da Carteira</div>
                <div class="financial-metric-value-container">
                    <div class="financial-metric-value">R$ {volume_val}</div>
                    <div class="unit-pill">{volume_sufixo}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with kpi2:
            st.markdown(f"""
            <div class="financial-metric-item">
                <div class="financial-metric-title">Taxa de Inadimplência Geral</div>
                <div class="financial-metric-value-container">
                    <div class="financial-metric-value">{taxa_inadimplencia:.2%}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with kpi3:
            st.markdown(f"""
        <div class="financial-metric-item">
            <div class="financial-metric-title">Valor Total Inadimplente</div>
            <div class="financial-metric-value-container">
                <div class="financial-metric-value">R$ {inadimplente_val}</div>
                <div class="unit-pill">{inadimplente_sufixo}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
            
        with kpi4:
            st.markdown(f"""
        <div class="financial-metric-item">
            <div class="financial-metric-title">Nº Total de Operações</div>
            <div class="financial-metric-value-container">
                <div class="financial-metric-value">{operacoes_val}</div>
                <div class="unit-pill">{operacoes_sufixo}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    else:
        st.warning("Não foi possível calcular o resumo executivo.")


elif st.session_state.pagina_ativa == "Visão Geral por UF":
    st.markdown("<div class='section-header'><h2>💡 Visão Geral por UF</h2></div>", unsafe_allow_html=True)
    with st.spinner("Buscando dados agregados no BigQuery..."):
        df_mapa = get_dados_visao_geral_uf(client)

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Taxa Média de Inadimplência por UF")
            st.plotly_chart(plot_inadimplencia_uf(df_mapa), use_container_width=True)
    with col2:
        with st.container(border=True):
            st.subheader("Volume Total da Carteira Ativa por UF")
            st.plotly_chart(plot_carteira_uf(df_mapa), use_container_width=True)

elif st.session_state.pagina_ativa == "Análise por Segmento":
    st.markdown("<div class='section-header'><h2>📊 Análise por Segmento</h2></div>", unsafe_allow_html=True)
    
    # Adicione a nova função de dados aos imports no topo do Home.py
    # from components.data_loader_bq import get_dados_top_n_segmento
    # from components.plot_utils import plot_top_segmento_horizontal
    
    segmento_dim_options = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse']
    selected_dim_display = st.selectbox("Selecione a Dimensão de Análise", [s.replace('_', ' ').title() for s in segmento_dim_options])
    segmento_dim = segmento_dim_options[[s.replace('_', ' ').title() for s in segmento_dim_options].index(selected_dim_display)]

    # LÓGICA INTELIGENTE: Se a dimensão for complexa, mostra a análise de Top 20.
    if segmento_dim in ['cnae_secao', 'cnae_subclasse', 'modalidade']:
        st.markdown("---")
        analise_tipo = st.radio(
            "Escolha o tipo de análise Top 20:",
            ('Maiores Riscos (Inadimplência)', 'Maiores Volumes (Carteira)'),
            horizontal=True
        )
        st.markdown("---")
        
        if 'Riscos' in analise_tipo:
            with st.spinner(f"Buscando Top 20 {selected_dim_display} por Risco..."):
                
                df_top = get_dados_top_n_segmento(client, segmento_dim, top_n=20, order_by='taxa_inadimplencia_media')
            st.plotly_chart(plot_top_segmento_horizontal(df_top, segmento_dim, 'taxa_inadimplencia_media', f"Top 20 {selected_dim_display} por Taxa de Inadimplência Média"), use_container_width=True)
        
        else: # Maiores Volumes
            with st.spinner(f"Buscando Top 20 {selected_dim_display} por Volume..."):
                df_top = get_dados_top_n_segmento(client, segmento_dim, top_n=20, order_by='volume_carteira_total')
            st.plotly_chart(plot_top_segmento_horizontal(df_top, segmento_dim, 'volume_carteira_total', f"Top 20 {selected_dim_display} por Volume da Carteira"), use_container_width=True)

    # LÓGICA ANTIGA: Se a dimensão for simples, mostra os gráficos completos como antes.
    else:
        with st.spinner(f"Analisando por {selected_dim_display}..."):
            df_segmento = get_dados_por_segmento(client, segmento_dim)
            
        st.subheader(f"Volume da Carteira por {selected_dim_display}")
        st.plotly_chart(plot_segmento_volume(df_segmento, segmento_dim, f"Volume por {selected_dim_display}"), use_container_width=True)
        st.subheader(f"Inadimplência Média por {selected_dim_display}")
        st.plotly_chart(plot_segmento_inadimplencia(df_segmento, segmento_dim, f"Inadimplência por {selected_dim_display}"), use_container_width=True)

elif st.session_state.pagina_ativa == "Tendência Temporal":
    st.markdown("<div class='section-header'><h2>📈 Tendência Temporal</h2></div>", unsafe_allow_html=True)
    with st.spinner("Buscando dados temporais..."):
        df_tendencia = get_dados_tendencia_temporal(client)
    st.plotly_chart(plot_tendencia_temporal(df_tendencia), use_container_width=True)

elif st.session_state.pagina_ativa == "Análise de Clusters":
    st.markdown("<div class='section-header'><h2>🔍 Análise de Clusters</h2></div>", unsafe_allow_html=True)
    with st.spinner("Carregando dados de clusterização..."):
        df_clusters_inadimplencia = get_dados_inadimplencia_por_cluster(client)
        df_cluster_profiles = load_cluster_profiles(client)
        df_full_clusters = load_full_cluster_data(client)
        df_top_combinacoes = get_top_combinacoes_risco(client)

    if not df_full_clusters.empty and not df_cluster_profiles.empty:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("Inadimplência por Cluster")
            st.plotly_chart(plot_inadimplencia_por_cluster(df_clusters_inadimplencia), use_container_width=True)
            st.subheader("Top 5 Combinações de Risco")
            st.plotly_chart(plot_top_combinacoes_risco(df_top_combinacoes.head(5)), use_container_width=True)

        with col2:
            cluster_ids = sorted(df_cluster_profiles['cluster_id'].unique().tolist())
            selected_cluster_id = st.selectbox("Selecione um Cluster para Detalhar", cluster_ids)
            features_para_perfil_numericas = ['total_carteira_ativa_segmento', 'taxa_inadimplencia_final_segmento', 'perc_ativo_problematico_final_segmento', 'contagem_subsegmentos']
            st.subheader(f"Perfil Detalhado do Cluster {selected_cluster_id}")
            st.plotly_chart(plot_perfil_cluster(df_full_clusters, selected_cluster_id, features_para_perfil_numericas), use_container_width=True)
            st.caption("Este gráfico mostra a posição relativa do cluster em cada característica numérica, normalizada de 0 a 1.")

    else:
        st.warning("Dados de clusterização não disponíveis. Execute a pipeline de clusterização.")

elif st.session_state.pagina_ativa == "Comparativo de Riscos":
    st.markdown("<div class='section-header'><h2>⚖️ Comparativo de Riscos</h2></div>", unsafe_allow_html=True)
    st.markdown("Selecione uma ou mais dimensões para agrupar e comparar os dados de inadimplência.")
    
    segmento_dim_options = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao']
    selected_dims = st.multiselect("Selecione as dimensões para comparar:", options=segmento_dim_options, default=['porte', 'modalidade'])

    if selected_dims:
        with st.spinner(f"Analisando por {', '.join(selected_dims)}..."):
            df_comparativo = get_dados_comparativo_riscos(client, selected_dims)
        
        st.subheader(f"Taxa Média de Inadimplência por {', '.join(selected_dims)}")
        st.plotly_chart(
            plot_comparativo_riscos(df_comparativo, selected_dims, f"Inadimplência por {', '.join(selected_dims)}"), 
            use_container_width=True
        )
    else:
        st.warning("Por favor, selecione pelo menos uma dimensão para análise.")

st.markdown("---")
st.info("Desenvolvido por JJ Guilherme")