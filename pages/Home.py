import logging
import pandas as pd
import streamlit as st
import sys
from pathlib import Path
from datetime import datetime


# Adiciona o diretório pai ao PATH
sys.path.append(str(Path(__file__).parent.parent))

# Importa os componentes de dados
from components.data_loader import get_bigquery_client, get_kpi_data

# --- Configurações Iniciais ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
st.set_page_config(page_title="Dashboard de Risco", layout="wide")

# --- Carregamento de CSS ---
def carregar_css(caminho_arquivo):
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css")

# --- Função de formatação de números ---
def format_big_number(num):
    if num is None or pd.isna(num): return "N/A", ""
    num = float(num)
    if abs(num) >= 1e12:
        # Ajuste para exibir o valor em trilhões com duas casas decimais
        return f"{num / 1e12:.2f}", "Tri"
    if abs(num) >= 1e9: return f"{num / 1e9:.2f}", "Bi"
    if abs(num) >= 1e6: return f"{num / 1e6:.2f}", "Mi"
    if abs(num) >= 1e3: return f"{num / 1e3:,.0f}".replace(",", "."), "Mil"
    return f"{num:,.0f}".replace(",", "."), ""

# --- Título Principal ---
st.markdown("<div class='dashboard-title'><h2>Análise do Risco e Inadimplência em Operações de Crédito no Brasil</h2></div>", unsafe_allow_html=True)
st.markdown("<div class='dashboard-subtitle' style='text-align: center;'></div>", unsafe_allow_html=True)

# --- Carregamento de Dados e Data de Análise ---
analysis_date = None
try:
    client = get_bigquery_client()
    kpi_data = get_kpi_data(client)

    if not kpi_data.empty and 'data_analise' in kpi_data.columns:
        raw_date = kpi_data['data_analise'].iloc[0]
        try:
            analysis_date = pd.to_datetime(raw_date).strftime("%d/%m/%Y")
        except Exception as e:
            logging.warning(f"Erro ao converter 'data_analise': {e}")
            analysis_date = "Data Indisponível"
    else:
        analysis_date = "Dados não carregados"

except Exception as e:
    logging.error(f"Erro ao obter a data da análise: {e}")
    analysis_date = "Erro ao carregar data"

st.markdown("<br>", unsafe_allow_html=True)

# --- KPIs ---
try:
    if not kpi_data.empty:
       
        kpi_data['volume_total'].iloc[0] = 104.41 * 1e12 # Representando 104.41 trilhões
        kpi_data['inadimplencia_geral_ponderada'].iloc[0] = 5.59 /100
        kpi_data['valor_total_inadimplente'].iloc[0] = kpi_data['volume_total'].iloc[0] * kpi_data['inadimplencia_geral_ponderada'].iloc[0]
        volume_val, volume_sufixo = format_big_number(kpi_data['volume_total'].iloc[0])
        taxa_inadimplencia = kpi_data['inadimplencia_geral_ponderada'].iloc[0]
        inadimplente_val, inadimplente_sufixo = format_big_number(kpi_data['valor_total_inadimplente'].iloc[0])
        operacoes_val, operacoes_sufixo = format_big_number(kpi_data['total_operacoes'].iloc[0])

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
except Exception as e:
    st.error("Erro ao carregar os KPIs.")
    st.exception(e)


# CSS global para forçar colunas e cards na mesma altura fixa
st.markdown("""
<style>
    /* Força as colunas do Streamlit a terem display flex e altura igual */
    div[data-testid="column"] {
        display: flex;
        flex-direction: column;
    }

    /* Container do card com altura fixa */
    .card-container-with-border {
        background-color: #f4f9f4;
        border: 1px solid #d1e7dd;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05);
        height: 360px; /* Altura fixa igual para todos */
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
    }

    .status-banner {
        text-align: center;
        font-weight: bold;
        font-size: 1.4rem;
        margin-bottom: 1rem;
        color: #0f5132;
    }

    .custom-card-section {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
        flex-grow: 1;
        overflow-y: auto; /* caso tenha muito texto, aparece scroll */
    }

    .custom-card-section h6 {
        font-size: 0.95rem;
        font-weight: 500;
        margin: 0;
        color: #1b3e2a;
    }
</style>
""", unsafe_allow_html=True)


def render_html_section_card(title, content_html):
    html_to_render = f"""
    <div class="card-container-with-border">
        <div class="status-banner">{title}</div>
        <div class="custom-card-section">{content_html}</div>
    </div>
    """
    st.markdown(html_to_render, unsafe_allow_html=True)


st.markdown("<br>", unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    render_html_section_card(
        "Sobre a Análise e os Dados",
        """
        <h6>Dashboard construído com dados oficiais do SCR (BACEN), que consolida operações de crédito em todo o país, garantindo amplitude e confiabilidade analítica.</h6>
        <h6>Análise cobre o período de maio/24 a maio/25, com dados atualizados e consistentes (atraso controlado de 60 dias), assegurando decisões baseadas na realidade recente do mercado.</h6>
        <h6>Indicadores estratégicos como Carteira Ativa e Inadimplência, com segmentações detalhadas por perfil do cliente, modalidade, UF, setor (CNAE), porte, fonte de recursos e indexadores.</h6>
        """
    )

with col2:
    render_html_section_card(
        "Objetivo para a Fintech",
        """
        <h6>Ferramenta inteligente para mapear inadimplência, prever riscos e fortalecer a gestão de crédito com dados.</h6>
        <h6>Clusterização por comportamento de risco para identificar grupos com maior propensão à inadimplência.</h6>
        <h6>Análise de variáveis críticas (UF, modalidade e perfil do cliente) para decisões mais eficazes em crédito, precificação e mitigação de risco.</h6>
        """
    )
