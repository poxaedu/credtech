# Home.py

import logging
import pandas as pd
import streamlit as st

# Importe apenas as funções necessárias para os KPIs desta página
from components.data_loader import get_bigquery_client, get_kpi_data

# --- Configuração de Logging e Página ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
st.set_page_config(page_title="Dashboard de Risco", layout="wide")


# --- Carregamento do Estilo CSS Customizado ---
def carregar_css(caminho_arquivo):
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css") # Carrega os estilos do seu arquivo style.css

import streamlit as st
import pandas as pd
import numpy as np



# --- Funções Auxiliares (mantidas aqui para os KPIs) ---
def format_big_number(num):
    """Formata um número e retorna o valor e o sufixo separadamente."""
    if num is None or pd.isna(num): return "N/A", ""
    num = float(num)
    if abs(num) >= 1e12: return f"{num / 1e12:.2f}", "Tri"
    if abs(num) >= 1e9: return f"{num / 1e9:.2f}", "Bi"
    if abs(num) >= 1e6: return f"{num / 1e6:.2f}", "Mi"
    if abs(num) >= 1e3: return f"{num / 1e3:,.0f}".replace(",", "."), "Mil"
    return f"{num:,.0f}".replace(",", "."), ""

# --- PÁGINA PRINCIPAL ---
st.markdown("<div class='dashboard-title'><h1>Dashboard de Análise de Risco de Crédito</h1></div>", unsafe_allow_html=True)
st.markdown("""<div class='dashboard-subtitle' style='text-align: center;'>
    <h3>Uma visão abrangente dos riscos de crédito no Brasil, potencializada pelo Google BigQuery.</h3>
</div>
""", unsafe_allow_html=True)

st.info("Para começar, selecione uma das páginas de análise na barra lateral à esquerda.")

# --- Resumo Executivo com Cards HTML Customizados ---
try:
    client = get_bigquery_client()
    kpi_data = get_kpi_data(client)


    if not kpi_data.empty:
        # Extrai e formata os valores
        volume_val, volume_sufixo = format_big_number(kpi_data['volume_total'].iloc[0])
        taxa_inadimplencia = kpi_data['inadimplencia_geral_ponderada'].iloc[0]
        inadimplente_val, inadimplente_sufixo = format_big_number(kpi_data['valor_total_inadimplente'].iloc[0])
        operacoes_val, operacoes_sufixo = format_big_number(kpi_data['total_operacoes'].iloc[0])

        # Cria as 4 colunas
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        # O ESTILO DOS CARDS HTML FOI MANTIDO AQUI
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

except Exception as e:
    st.error("Ocorreu um erro ao carregar os dados principais.")
    st.exception(e)

st.markdown("---")
