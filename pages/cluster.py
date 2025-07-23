import streamlit as st
import pandas as pd
# Importações ajustadas de acordo com os arquivos fornecidos
from components.data_loader import (
    get_bigquery_client, 
    get_dados_inadimplencia_por_cluster, 
    load_cluster_profiles, 
    load_full_cluster_data, 
    get_top_combinacoes_risco
)
from components.plot_utils import plot_top_combinacoes_risco

# --- 1. SETUP INICIAL DA PÁGINA ---
st.set_page_config(page_title="Análise de Clusters", layout="wide")

def carregar_css(caminho_arquivo):
    """Lê um arquivo CSS e o aplica ao app Streamlit."""
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css")

# --- 2. CARREGAMENTO DOS DADOS (CACHEADO) ---
# Usando a função de conexão do seu data_loader
client = get_bigquery_client() 

@st.cache_data(ttl=3600)
def load_all_cluster_data(_client):
    """Carrega todos os DataFrames necessários para a página de uma só vez."""
    df_inadimplencia = get_dados_inadimplencia_por_cluster(_client)
    df_profiles = load_cluster_profiles(_client)
    df_full = load_full_cluster_data(_client)
    df_combinacoes = get_top_combinacoes_risco(_client)
    return df_inadimplencia, df_profiles, df_full, df_combinacoes

try:
    with st.spinner("Carregando e otimizando dados de clusterização..."):
        df_clusters_inadimplencia, df_cluster_profiles, df_full_clusters, df_top_combinacoes = load_all_cluster_data(client)
except Exception as e:
    st.error("Ocorreu um erro ao carregar os dados.")
    st.exception(e)
    st.stop()

# --- 3. RENDERIZAÇÃO DA PÁGINA ---
st.markdown("<div class='dashboard-title'><h1>🔍 Análise de Clusters</h1></div>", unsafe_allow_html=True)
st.markdown('<br>', unsafe_allow_html=True)  # Espaço entre o título e o conteúdo
if not df_full_clusters.empty and not df_cluster_profiles.empty:

    if not df_clusters_inadimplencia.empty:
        df_cards = df_clusters_inadimplencia.copy()

        # REPLICANDO O "MÉTODO ANTIGO" DO GRÁFICO DE PIZZA
        # 1. Somar os valores da coluna 'taxa_inadimplencia_media'
        soma_das_taxas_medias = df_cards['taxa_inadimplencia_media'].sum()

        # 2. Calcular o percentual de cada cluster em relação a essa soma
        if soma_das_taxas_medias > 0:
            df_cards['percentual_calculado'] = (df_cards['taxa_inadimplencia_media'] / soma_das_taxas_medias)
        else:
            df_cards['percentual_calculado'] = 0
        
        # 3. Exibir os cards com o novo percentual
        df_cards = df_cards.sort_values('cluster_id').reset_index(drop=True)
        cols = st.columns(len(df_cards))
        for i, row in df_cards.iterrows():
            with cols[i]:
                cluster_id = int(row['cluster_id'])
                percent_value = row['percentual_calculado'] # Usando o valor calculado
                
                card_html = f"""
                <div class="segment-metric-item" style="height: 100%;">
                    <div class="segment-metric-title">Cluster {cluster_id}</div>
                    <div class="segment-metric-value">{percent_value:.2%}</div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)
    
st.markdown('<br>', unsafe_allow_html=True)  # Espaço entre o título e o conteúdo

with st.container(border=True):
            st.markdown("<h5 style='text-align: center;'>Perfil Detalhado do Cluster</h5>", unsafe_allow_html=True)
            
            all_features = [col for col in df_cluster_profiles.columns if col != 'cluster_id']
            features_num = df_cluster_profiles[all_features].select_dtypes(include='number').columns.tolist()
            features_cat = df_cluster_profiles[all_features].select_dtypes(include=['object', 'category']).columns.tolist()

            cluster_ids = sorted(df_cluster_profiles['cluster_id'].unique())
            selected_cluster_id = st.selectbox("Selecione um Cluster:", options=cluster_ids, format_func=lambda x: f"Cluster {x}", label_visibility="collapsed")

            if selected_cluster_id is not None:
                profile_data = df_cluster_profiles[df_cluster_profiles['cluster_id'] == selected_cluster_id].iloc[0]
                
                html_numerico = ""
                for feature in features_num:
                    if feature in profile_data and pd.notna(profile_data[feature]):
                        value = profile_data[feature]
                        label = feature.replace('_', ' ').title()
                        if 'taxa' in feature or 'perc' in feature: formatted_value = f"{value:.2%}"
                        elif 'volume' in feature or 'carteira' in feature: formatted_value = f"R$ {value:,.2f}"
                        else: formatted_value = f"{int(value)}"
                        html_numerico += f'<div class="feature-row"><span class="feature-label">{label}</span><span class="feature-value">{formatted_value}</span></div>'
                
                html_categorico = ""
                for feature in features_cat:
                    if feature in profile_data and pd.notna(profile_data[feature]):
                        value = profile_data[feature]
                        label = feature.replace('_', ' ').title()
                        html_categorico += f'<div class="feature-row"><span class="feature-label">{label}</span><span class="categorical-pill">{value}</span></div>'
                
                card_html = f"""
                <div class="profile-card">
                    <div class="profile-section"><h6 class="profile-section-title">Métricas Principais</h6>{html_numerico}</div>
                    <div class="profile-section"><h6 class="profile-section-title">Atributos Dominantes</h6>{html_categorico}</div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)

with st.container(border=True):
            st.markdown("<h5 style='text-align: center'>Top 5 Combinações de Risco</h5>", unsafe_allow_html=True)
            st.plotly_chart(plot_top_combinacoes_risco(df_top_combinacoes.head(5)), use_container_width=True)
