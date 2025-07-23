# pages/1_💡_Visao_Geral_por_UF.py

import streamlit as st
import json
import pandas as pd # Adicionado para manipulação de dados

# Importe as funções necessárias dos seus módulos
from components.data_loader import get_bigquery_client, get_dados_visao_geral_uf
from components.plot_utils import plot_choropleth_brasil, plot_carteira_uf
from pages.Home import carregar_css # Reutiliza a função de CSS

st.set_page_config(page_title="Visão Geográfica", layout="wide", initial_sidebar_state="expanded")
carregar_css("style.css")

@st.cache_data
def load_geojson_data(path: str) -> dict:
    """Carrega o arquivo GeoJSON em cache."""
    try:
        with open(path, "r", encoding='latin-1') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"Arquivo GeoJSON não encontrado em '{path}'.")
        return None

# --- CONTEÚDO DA PÁGINA ---

st.markdown("<div class='dashboard-title'><h1>💡 Visão Geral por UF</h1></div>", unsafe_allow_html=True)
st.markdown("""<div class='dashboard-subtitle' style='text-align: center;'>
    <h4>Análise geográfica da inadimplência e do volume da carteira de crédito no Brasil.</h4>
</div>
""", unsafe_allow_html=True)
st.divider()
try:
    client = get_bigquery_client()

    # Carrega todos os dados necessários de uma única vez, de forma eficiente
    with st.spinner("Buscando dados agregados no BigQuery..."):
        df_mapa = get_dados_visao_geral_uf(client)
        geojson_brasil = load_geojson_data('uf.json')

    # --- NOVO: Destaques com os Top 3 e Bottom 3 Estados ---
    if not df_mapa.empty:
        
        # Ordena o DataFrame para encontrar os extremos da inadimplência
        # Ordena por taxa de inadimplência
    # Ordena por taxa de inadimplência (do maior para o menor)
        df_sorted = df_mapa.sort_values(by='taxa_inadimplencia_media', ascending=False)

    top3_piores = df_sorted.head(3)
    top3_melhores = df_sorted.tail(3).iloc[::-1]

    # Layout com 2 colunas
    col1, col2 = st.columns(2)
    # --- Card: Top 3 Maiores Riscos ---
    with col1:
        html_card_piores = f"""
        <div class="custom-card-section">
            <h2 class="card-title">📉 Maiores Riscos</h2>
            <hr style="width: 80%; margin: 0 auto 1rem auto; border: none; border-top: 1px solid #e2e8f0;">
            <div style="display: flex; justify-content: space-around;">
                <div style="display: flex; flex-direction: column; align-items: center;">
                    <div class="uf-unit-pill">1º {top3_piores.iloc[0]['uf']}</div>
                    <h2 style="card-metric-value">{top3_piores.iloc[0]['taxa_inadimplencia_media']:.2%}</h2>
                </div>
                <div style="display: center; flex-direction: column; align-items: center;">
                    <div class="uf-unit-pill">2º {top3_piores.iloc[1]['uf']}</div>
                    <h2 class="card-metric-value">{top3_piores.iloc[1]['taxa_inadimplencia_media']:.2%}</h2>
                </div>
                <div style="display: flex; flex-direction: column; align-items: center;">
                    <div class="uf-unit-pill">3º {top3_piores.iloc[2]['uf']}</div>
                    <h3 class="card-metric-value">{top3_piores.iloc[2]['taxa_inadimplencia_media']:.2%}</h2>
                </div>
            </div>
        </div>
        """
        st.markdown(html_card_piores, unsafe_allow_html=True)

# --- Card: Top 3 Menores Riscos ---
    with col2:
        html_card_melhores = f"""
        <div class="custom-card-section">
            <h2 class="card-title">📈 Menores Riscos</h2>
            <hr style="width: 80%; margin: 0 auto 1rem auto; border: none; border-top: 1px solid #e2e8f0;">
            <div style="display: flex; justify-content: space-around;">
                <div class="single-metric-item">
                    <div class="uf-unit-pill">1º {top3_melhores.iloc[0]['uf']}</div>
                    <h3 class="card-metric-value">{top3_melhores.iloc[0]['taxa_inadimplencia_media']:.2%}</h3>
                </div>
                <div class="single-metric-item">
                    <div class="uf-unit-pill">2º {top3_melhores.iloc[1]['uf']}</div>
                    <h3 class="card-metric-value">{top3_melhores.iloc[1]['taxa_inadimplencia_media']:.2%}</h3>
                </div>
                <div class="single-metric-item">
                    <div class="uf-unit-pill">3º {top3_melhores.iloc[2]['uf']}</div>
                    <h3 class="card-metric-value">{top3_melhores.iloc[2]['taxa_inadimplencia_media']:.2%}</h3>
                </div>
            </div>
        </div>
        """
        st.markdown(html_card_melhores, unsafe_allow_html=True)
    st.divider()
    st.markdown("<div class='section-header'><h3>Analise por UF</h3></div>", unsafe_allow_html=True)

    # --- Os dois gráficos principais, mantidos como você pediu ---
    if geojson_brasil and not df_mapa.empty:
        col_mapa, col_barra = st.columns(2)

        with col_mapa:
            with st.container(border=True):
                # Corrigi a sintaxe do HTML para centralizar o título
                st.markdown('<h3 style="text-align: center;">Taxa Média de Inadimplência por UF</h3>', unsafe_allow_html=True)
                st.plotly_chart(plot_choropleth_brasil(df_mapa, geojson_brasil, ""), use_container_width=True)

        with col_barra:
            with st.container(border=True):
                st.markdown('<h3 style="text-align: center;">Volume Total da Carteira Ativa por UF</h3>', unsafe_allow_html=True)
                st.plotly_chart(plot_carteira_uf(df_mapa), use_container_width=True)

        # --- NOVO: Tabela de dados detalhados em um expander ---
        with st.expander("Visualizar dados em tabela"):
            st.markdown("Dados detalhados por Unidade Federativa.")
            # Formata as colunas para melhor visualização na tabela
            df_display = df_mapa.copy()
            df_display['taxa_inadimplencia_media'] = df_display['taxa_inadimplencia_media'].map('{:.2%}'.format)
            df_display['volume_carteira_total'] = df_display['volume_carteira_total'].map('R$ {:,.2f}'.format)
            st.dataframe(df_display, use_container_width=True, hide_index=True)

    else:
        st.warning("Não foi possível gerar as visualizações. Verifique a disponibilidade dos dados.")

except Exception as e:
    st.error("Ocorreu um erro ao carregar esta página.")
    st.exception(e)