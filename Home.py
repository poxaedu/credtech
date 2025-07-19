import logging

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Importe as funções do seu novo data_loader.py
from components.data_loader import (get_db_engine, load_cluster_data,
                                    load_cluster_profiles,
                                    load_indicadores_data, load_scr_aggr_data)
# Importe as funções do seu plot_utils.py
from components.plot_utils import (plot_carteira_uf, plot_comparativo_riscos,
                                   plot_inadimplencia_por_cluster,
                                   plot_inadimplencia_uf, plot_perfil_cluster,
                                   plot_segmento_inadimplencia,
                                   plot_segmento_volume,
                                   plot_tendencia_temporal,
                                   plot_top_combinacoes_risco)

# --- Configuração de Logging para o app Streamlit ---
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
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado. O estilo pode não ser aplicado.")

carregar_css("style.css")


# --- GESTÃO DO ESTADO DA APLICAÇÃO ---
if 'pagina_ativa' not in st.session_state:
    st.session_state.pagina_ativa = 'Visão Geral por UF'


# --- TÍTULO PRINCIPAL E INTRODUÇÃO ---
st.markdown("<div class='dashboard-title'><h1>Dashboard de Análise de Inadimplência</h1></div>", unsafe_allow_html=True)
st.markdown("""
Esta aplicação interativa permite explorar os padrões e perfis de inadimplência
nas operações de crédito do Brasil, com base nos dados do SCR e indicadores econômicos.
""")

st.markdown("---")


# --- NAVEGAÇÃO COM BOTÕES NO CORPO PRINCIPAL ---
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

# Carrega a engine do banco de dados
engine = get_db_engine()

# --- CHAMA AS FUNÇÕES DE CARREGAMENTO SEM OS FILTROS ---
df_scr_full = load_scr_aggr_data(engine)
df_indicadores_full = load_indicadores_data(engine)
df_clusters_full = load_cluster_data(engine) # Carrega os dados de cluster (segmentos com cluster_id)
df_cluster_profiles_full = load_cluster_profiles(engine) # Carrega os perfis dos clusters

# df_scr_filtered e df_indicadores_filtered agora são os DataFrames completos
# Se você precisar de um filtro de data *fixo* para as análises, aplique-o aqui
# Exemplo: df_scr_filtered = df_scr_full[df_scr_full['data_base'].dt.year == 2024].copy()
df_scr_filtered = df_scr_full.copy() # Usando o DataFrame completo
df_indicadores_filtered = df_indicadores_full.copy() # Usando o DataFrame completo
df_clusters_filtered = df_clusters_full.copy()


# --- RENDERIZAÇÃO DAS SEÇÕES (CONTEÚDO PRINCIPAL) ---

if st.session_state.pagina_ativa == "Visão Geral por UF":
    st.markdown("<div class='section-header'><h2>💡 Visão Geral por UF</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    Explore a distribuição da carteira de crédito e a inadimplência por Unidade Federativa.
    """)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Taxa Média de Inadimplência por UF")
        st.plotly_chart(plot_inadimplencia_uf(df_scr_filtered), use_container_width=True)
        st.markdown("*(Dados da tabela `ft_scr_agregado_mensal`)*")

    with col2:
        st.subheader("Volume Total da Carteira Ativa por UF")
        st.plotly_chart(plot_carteira_uf(df_scr_filtered), use_container_width=True)
        st.markdown("*(Dados da tabela `ft_scr_agregado_mensal`)*")


elif st.session_state.pagina_ativa == "Análise por Segmento":
    st.markdown("<div class='section-header'><h2>📊 Análise por Segmento</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    Analise o volume e a inadimplência por diversas dimensões de segmentação.
    """)

    segmento_dim_options = ['uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse']
    segmento_dim_display_options = [s.replace('_', ' ').title() for s in segmento_dim_options]

    selected_display_dim = st.selectbox(
        "Selecione a Dimensão de Análise",
        segmento_dim_display_options
    )
    segmento_dim = segmento_dim_options[segmento_dim_display_options.index(selected_display_dim)]

    st.subheader(f"Volume Total da Carteira por {selected_display_dim}")
    st.plotly_chart(plot_segmento_volume(df_scr_filtered, segmento_dim, f"Volume por {selected_display_dim}"), use_container_width=True)
    st.markdown("*(Dados da tabela `ft_scr_agregado_mensal`)*")

    st.subheader(f"Taxa Média de Inadimplência por {selected_display_dim}")
    st.plotly_chart(plot_segmento_inadimplencia(df_scr_filtered, segmento_dim, f"Taxa de Inadimplência por {selected_display_dim}"), use_container_width=True)
    st.markdown("*(Dados da tabela `ft_scr_agregado_mensal`)*")

    if segmento_dim in ['cnae_secao', 'cnae_subclasse']:
        st.subheader("Análise Detalhada por CNAE")
        st.info("Gráficos de Top 20 por Volume e Top 20 por Risco (Serão Plotly Bar Charts)")
        st.markdown("*(Dados da tabela `ft_scr_agregado_mensal`)*")


elif st.session_state.pagina_ativa == "Tendência Temporal":
    st.markdown("<div class='section-header'><h2>📈 Tendência Temporal</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    Acompanhe a evolução da taxa de inadimplência e sua relação com os principais indicadores macroeconômicos.
    """)

    st.plotly_chart(plot_tendencia_temporal(df_scr_filtered, df_indicadores_filtered), use_container_width=True)


elif st.session_state.pagina_ativa == "Análise de Clusters":
    st.markdown("<div class='section-header'><h2>🔍 Análise de Clusters e Perfis de Risco</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    Explore os agrupamentos de risco identificados e seus perfis de inadimplência.
    """)

    if not df_clusters_filtered.empty and not df_cluster_profiles_full.empty:
        # Opções de cluster para seleção
        cluster_ids = sorted(df_clusters_filtered['cluster_id'].unique().tolist())
        selected_cluster_id = st.selectbox("Selecione um Cluster para Detalhar o Perfil", cluster_ids)

        # Taxa de Inadimplência por Cluster
        st.subheader("Taxa de Inadimplência por Cluster")
        st.plotly_chart(plot_inadimplencia_por_cluster(df_clusters_filtered), use_container_width=True)
        st.markdown("*(Resultados do Modelo K-Means)*")

        # Perfil Detalhado do Cluster Selecionado
        st.subheader(f"Perfil Detalhado do Cluster {selected_cluster_id}")
        st.markdown(f"As características do **Cluster {selected_cluster_id}** são:")

        # Obter o perfil do cluster selecionado
        profile_data = df_cluster_profiles_full[df_cluster_profiles_full['cluster_id'] == selected_cluster_id].iloc[0]

        # Features numéricas usadas para clusterização (para o gráfico de radar e descrição)
        features_para_perfil_numericas = [
            'total_carteira_ativa_segmento',
            'taxa_inadimplencia_final_segmento',
            'perc_ativo_problematico_final_segmento',
            'contagem_subsegmentos',
            # Adicione aqui as mesmas features numéricas que você usou em pipeline_gold_clustering.py
        ]
        # Features categóricas para descrição textual
        features_para_perfil_categoricas = [
            'uf', 'cliente', 'modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse'
            # Adicione aqui as mesmas features categóricas que você usou em pipeline_gold_clustering.py para a moda
        ]

        # Geração da Descrição Textual do Perfil
        st.markdown("---")
        st.markdown("### Descrição Textual do Perfil")
        st.write(f"O **Cluster {selected_cluster_id}** apresenta as seguintes características médias:")

        for feature in features_para_perfil_numericas:
            if feature in profile_data:
                value = profile_data[feature]
                if 'taxa' in feature or 'perc' in feature:
                    st.write(f"- **{feature.replace('_', ' ').title()}:** {value:.2f}%")
                elif 'volume' in feature or 'total' in feature or 'carteira' in feature:
                    st.write(f"- **{feature.replace('_', ' ').title()}:** R$ {value:,.2f}")
                else:
                    st.write(f"- **{feature.replace('_', ' ').title()}:** {value:,.0f}")

        for feature in features_para_perfil_categoricas:
            if feature in profile_data and profile_data[feature] is not None:
                st.write(f"- **{feature.replace('_', ' ').title()}:** {profile_data[feature]}")

        st.markdown("---")

        # Gráfico de Radar (Spider Chart) para o perfil numérico
        st.subheader(f"Gráfico de Radar do Cluster {selected_cluster_id}")
        # A função plot_perfil_cluster precisa do DataFrame completo (df_clusters_filtered)
        # e das features numéricas para normalizar os eixos do radar
        st.plotly_chart(plot_perfil_cluster(df_clusters_filtered, selected_cluster_id, features_para_perfil_numericas), use_container_width=True)
        st.markdown("*(Este gráfico mostra a posição relativa do cluster em cada característica numérica, normalizada de 0 a 1.)*")

        # Top Combinações de Risco (usando o df_clusters_filtered)
        st.subheader("Top Combinações de Risco")
        st.plotly_chart(plot_top_combinacoes_risco(df_clusters_filtered), use_container_width=True)
        st.markdown("*(Dados da tabela `ft_scr_agregado_mensal`)*")
    else:
        st.warning("Dados de clusterização ou perfis de cluster não disponíveis. Por favor, execute o script `pipeline_gold_clustering.py` primeiro.")

elif st.session_state.pagina_ativa == "Comparativo de Riscos":
    st.markdown("<div class='section-header'><h2>⚖️ Comparativo de Riscos</h2></div>", unsafe_allow_html=True)
    st.markdown("""
    Compare a inadimplência entre diferentes categorias ou segmentos de forma personalizada.
    """)

    st.info("Esta seção permitirá selecionar dois ou mais grupos/segmentos e comparar suas métricas de inadimplência lado a lado (Gráficos/Tabelas de Comparação).")


st.markdown("---")
st.info("Desenvolvido por JJ Guilherme e XMen")