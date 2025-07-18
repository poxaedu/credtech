# app.py (Versão com botões na Sidebar)

import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium

# --- 1. CONFIGURAÇÃO DA PÁGINA E CARREGAMENTO DO CSS ---
st.set_page_config(
    page_title="Dashboard de Análise Financeira",
    page_icon="💡",
    layout="wide"
)

def carregar_css(caminho_arquivo):
    with open(caminho_arquivo) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

carregar_css("style.css")


# --- 2. API SIMULADA (sem alterações) ---
class API_Financeira:
    @st.cache_data
    def carregar_dados_fraude(_self):
        loc_coords = {
            'São Paulo': (-23.55, -46.63), 'Rio de Janeiro': (-22.90, -43.17),
            'Belo Horizonte': (-19.92, -43.93), 'Salvador': (-12.97, -38.50),
            'Curitiba': (-25.42, -49.27), 'Porto Alegre': (-30.03, -51.20),
            'Brasília': (-15.78, -47.92), 'Recife': (-8.05, -34.90),
            'Fortaleza': (-3.73, -38.52), 'Manaus': (-3.11, -60.02)
        }
        cidades = list(loc_coords.keys())
        data = {
            'Transaction_ID': range(5000), 'Transaction_Type': np.random.choice(['Purchase', 'Withdrawal', 'Payment', 'Transfer'], 5000),
            'Fraud_Label': np.random.choice([0, 1], 5000, p=[0.95, 0.05]), 'Location': np.random.choice(cidades, 5000),
            'Transaction_Amount': np.random.uniform(10, 1000, 5000)
        }
        df = pd.DataFrame(data)
        df['Latitude'] = df['Location'].apply(lambda x: loc_coords[x][0] + np.random.normal(0, 0.2))
        df['Longitude'] = df['Location'].apply(lambda x: loc_coords[x][1] + np.random.normal(0, 0.2))
        return df

    def criar_mapa_folium_agregado(_self, df: pd.DataFrame):
        if df.empty: return None
        df_agregado = df.groupby('Location').agg(
            Latitude=('Latitude', 'mean'), Longitude=('Longitude', 'mean'),
            Total_Transacoes=('Transaction_ID', 'count'), Total_Fraudes=('Fraud_Label', 'sum')
        ).reset_index()
        if df_agregado.empty: return None
        df_agregado['Taxa_Fraude'] = (df_agregado['Total_Fraudes'] / df_agregado['Total_Transacoes']) * 100
        df_agregado['Cor'] = df_agregado['Taxa_Fraude'].apply(lambda x: '#d84315' if x > 10 else ('#f4511e' if x > 5 else ('#ffb300' if x > 0 else '#2e7d32')))
        mapa = folium.Map(location=[-14.2350, -51.9253], zoom_start=4, tiles="CartoDB positron")
        for _, row in df_agregado.iterrows():
            popup_text = f"<b>Local:</b> {row['Location']}<br><b>Transações:</b> {row['Total_Transacoes']:,}<br><b>Fraudes:</b> {row['Total_Fraudes']:,}<br><b>Taxa de Fraude:</b> {row['Taxa_Fraude']:.2f}%"
            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']], radius=np.log(row['Total_Transacoes'] + 1) * 2.5,
                popup=folium.Popup(popup_text, max_width=300), color=row['Cor'], fill=True, fill_color=row['Cor'], fill_opacity=0.7
            ).add_to(mapa)
        return mapa

api = API_Financeira()


# --- 3. NAVEGAÇÃO COM BOTÕES E CONTROLE DE ESTADO (MODIFICADO) ---

# Título principal na página
st.markdown("<div class='dashboard-title'><h1>Análise de Crédito e Risco Financeiro</h1></div>", unsafe_allow_html=True)
st.markdown("---")

# PASSO CHAVE 1: Inicializa o estado da sessão (sem alterações)
if 'pagina_ativa' not in st.session_state:
    st.session_state.pagina_ativa = 'Visão Geral'

# PASSO CHAVE 2: Cria os botões de navegação DENTRO DA SIDEBAR
st.sidebar.title("Menu de Navegação")
st.sidebar.write("Selecione a análise desejada:")

# O botão é do tipo "primary" (destacado) SE a página ativa for "Visão Geral"
if st.sidebar.button('💡 Visão Geral', use_container_width=True, 
                      type="primary" if st.session_state.pagina_ativa == 'Visão Geral' else "secondary"):
    st.session_state.pagina_ativa = 'Visão Geral'
    st.rerun() 

# O botão é do tipo "primary" SE a página ativa for "Análise Geográfica"
if st.sidebar.button('🗺️ Análise Geográfica', use_container_width=True,
                      type="primary" if st.session_state.pagina_ativa == 'Análise Geográfica' else "secondary"):
    st.session_state.pagina_ativa = 'Análise Geográfica'
    st.rerun()


# --- 4. RENDERIZAÇÃO DAS PÁGINAS (sem alterações) ---
# O conteúdo exibido depende do que está salvo na "memória" (session_state)

if st.session_state.pagina_ativa == "Visão Geral":
    st.markdown("<div class='section-header'><h1>💡 Visão Geral da Análise de Fraude</h1></div>", unsafe_allow_html=True)
    df_geral = api.carregar_dados_fraude()
    
    with st.container(border=True):
        c1, c2 = st.columns([0.35, 0.65])
        with c1:
            total_transacoes = df_geral['Transaction_ID'].count()
            total_fraudes = df_geral['Fraud_Label'].sum()
            valor_total = df_geral['Transaction_Amount'].sum()
            taxa_fraude_geral = (total_fraudes / total_transacoes) * 100 if total_transacoes > 0 else 0
            
            st.markdown(f"""
                <div style="padding: 10px;">
                    <div class="financial-metric-item"><p class="financial-metric-title">Volume Total de Transações</p><h3 class="financial-metric-value">{total_transacoes:,}</h3></div>
                    <div class="financial-metric-item"><p class="financial-metric-title">Valor Total Transacionado</p><h3 class="financial-metric-value">R$ {valor_total:,.2f}</h3></div>
                    <div class="financial-metric-item"><p class="financial-metric-title">Total de Fraudes Identificadas</p><h3 class="financial-metric-value" style="color: #ef4444;">{total_fraudes:,}</h3></div>
                    <div class="financial-metric-item"><p class="financial-metric-title">Taxa de Fraude Geral</p><h3 class="financial-metric-value" style="color: #ef4444;">{taxa_fraude_geral:.2f}%</h3></div>
                </div>
            """, unsafe_allow_html=True)
        with c2:
            mapa_folium_geral = api.criar_mapa_folium_agregado(df_geral)
            st_folium(mapa_folium_geral, use_container_width=True, height=500)

elif st.session_state.pagina_ativa == "Análise Geográfica":
    st.markdown("<div class='section-header'><h1>🗺️ Análise Geográfica com Filtros</h1></div>", unsafe_allow_html=True)
    
    with st.container(border=True):
        st.markdown("<div class='title-card'><h3>🔍 Filtre os dados para explorar o mapa</h3></div>", unsafe_allow_html=True)
        
        df_principal = api.carregar_dados_fraude()
        f1, f2 = st.columns(2)
        with f1:
            tipos_transacao = ['Todos'] + sorted(df_principal['Transaction_Type'].unique())
            tipo_selecionado = st.selectbox("Filtrar por Tipo de Transação:", tipos_transacao)
        with f2:
            status_fraude = {'Todos': None, 'Apenas Fraudes': 1, 'Apenas Legítimas': 0}
            status_selecionado_key = st.selectbox("Filtrar por Status:", options=list(status_fraude.keys()))
            status_selecionado_value = status_fraude[status_selecionado_key]

        df_filtrado = df_principal.copy()
        if tipo_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Transaction_Type'] == tipo_selecionado]
        if status_selecionado_value is not None:
            df_filtrado = df_filtrado[df_filtrado['Fraud_Label'] == status_selecionado_value]

        mapa_agregado = api.criar_mapa_folium_agregado(df_filtrado)
        st_folium(mapa_agregado, use_container_width=True, height=450)
