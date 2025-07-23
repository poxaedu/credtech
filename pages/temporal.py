import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt # Adicionado novamente para datetime
import plotly.graph_objects as go
import plotly.express as px
from scipy.stats import pearsonr, spearmanr

# IMPORTANTE: Garanta que essas funções existam em seus respectivos arquivos
# Assegurando que calculate_metrics_for_period esteja disponível
from components.data_loader import get_bigquery_client, get_dados_tendencia_temporal, \
                                   get_dados_por_segmento, calcular_correlacoes, \
                                   interpretar_correlacao, calculate_metrics_for_period # Adicionada calculate_metrics_for_period

# Assegurando que plot_single_temporal_series esteja disponível e substituindo plot_tendencia_temporal
from components.plot_utils import plot_single_temporal_series, plot_matriz_correlacao, plot_scatter_correlacao 

import logging
from datetime import datetime, timedelta

# Configurar o logger (se já não estiver configurado globalmente)
logger = logging.getLogger(__name__)

# --- 1. SETUP INICIAL DA PÁGINA E CSS EMBUTIDO ---
st.set_page_config(page_title="Tendência Temporal", layout="wide")

# CSS Injetado diretamente para garantir a renderização correta e evitar conflitos
def carregar_css(caminho_arquivo):
    """Lê um arquivo CSS e o aplica ao app Streamlit."""
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css")


# --- 3. CARREGAMENTO DOS DADOS ---
client = get_bigquery_client()
@st.cache_data(ttl=3600)
def load_temporal_data(_client):
    return get_dados_tendencia_temporal(_client)

try:
    with st.spinner("Carregando dados temporais..."):
        df_temporal = load_temporal_data(client)
except Exception as e:
    st.error("Ocorreu um erro ao carregar os dados temporais."); 
    st.exception(e); 
    st.stop()

# Mapeamento de métricas (nomes de coluna) para nomes de exibição e cores (usado na nova seção temporal)
metric_options = {
    'taxa_inadimplencia_media': {'name': 'Inadimplência Média', 'color': '#0F5D00'},
    'taxa_desemprego': {'name': 'Desemprego', 'color': '#66c2a5'},
    'valor_ipca': {'name': 'IPCA', 'color': '#2ca25f'},
    'taxa_selic_meta': {'name': 'Selic', 'color': '#447908'}
}
# Nomes de exibição para os indicadores, usado nas seções de correlação
indicadores_nomes = {
    'taxa_desemprego': 'Taxa de Desemprego', 
    'valor_ipca': 'IPCA (Inflação)', 
    'taxa_selic_meta': 'Taxa Selic'
}


# --- 4. RENDERIZAÇÃO DA PÁGINA ---
st.markdown("<div class='dashboard-title'><h1>📈 Análise Temporal e Correlações</h1></div>", unsafe_allow_html=True)
st.markdown("<div class='dashboard-subtitle' style='text-align: center;'><h4>Correlação entre Inadimplência e Indicadores Macroeconômicos</h4></div>", unsafe_allow_html=True)
st.divider()

if not df_temporal.empty:
    correlacoes = calcular_correlacoes(df_temporal)
    
    # --- SEÇÃO 1: CARDS COM CORRELAÇÕES (MANTIDA DO CÓDIGO ANTIGO) ---
    if correlacoes:
        cards_html_list = []
        
        for indicador, dados in correlacoes.items():
            corr = dados['pearson']['corr']; p_val = dados['pearson']['p_value']
            interpretacao = interpretar_correlacao(corr)
            if abs(corr) >= 0.7: cor_classe = "high-correlation"
            elif abs(corr) >= 0.5: cor_classe = "medium-correlation"
            else: cor_classe = "low-correlation"
            significancia = "Significativa" if p_val < 0.05 else "Não Significativa"
            
            card_html = f"""
            <div class="custom-card-section {cor_classe}" style="flex: 1;">
                <div class="card-title">{indicadores_nomes[indicador]}</div>
                <div class="correlation-categorical-pill">{corr:.3f}</div>
                <div class="card-correlation-subtitle">Correlação de Pearson</div>
                <hr style="margin: 10px 0; border-color: #eee;">
                <div class="interpretation-text">
                    <strong>Intensidade:</strong> {interpretacao}<br>
                    <strong>P-valor:</strong> {p_val:.4f}<br>
                    <strong>Significância:</strong> {significancia}
                </div>
            </div>"""
            cards_html_list.append(card_html)
        
        all_cards_html = "".join(cards_html_list)
        banner_html = f"""
        <div class="correlation-banner">
            <h3 style="text-align: center; margin-top: 0; margin-bottom: 1.5rem; color: #333;">🔢 Análise Quantitativa de Correlações</h3>
            <div class="correlation-cards-container">{all_cards_html}</div>
        </div>"""
        st.markdown(banner_html, unsafe_allow_html=True)

    # --- NOVA SEÇÃO: EVOLUÇÃO TEMPORAL (COMBINANDO NOVAS FUNCIONALIDADES COM LAYOUT DE COLUNAS) ---
    st.markdown("<div class='section-header'><h3>📊 Evolução Temporal</h3></div>", unsafe_allow_html=True)
    with st.container(border=True):
        # Definindo as duas colunas principais para este container
        col_left_panel, col_right_panel = st.columns([1, 1])

        # Variáveis para armazenar os valores selecionados pelos widgets
        selected_metric_name_display = list(metric_options.values())[0]['name'] # Valor padrão
        selected_dates = (df_temporal['mes'].min(), df_temporal['mes'].max()) # Valor padrão

        with col_left_panel:
            # Selectbox para selecionar a métrica
            selected_metric_name_display = st.selectbox(
                "Métrica:", # Label para a coluna
                options=[details['name'] for details in metric_options.values()],
                key='metric_selector_selectbox_main_temporal', # Chave única
            )

            # Encontra a chave (nome da coluna) e cor correspondentes à seleção do selectbox
            selected_chart_metric_key = None
            selected_chart_metric_color = None
            for key, details in metric_options.items():
                if details['name'] == selected_metric_name_display:
                    selected_chart_metric_key = key
                    selected_chart_metric_color = details['color']
                    break
            
            # Placeholder para o display de métrica.
            # O conteúdo será preenchido após 'selected_dates' ser definido.
            avg_value_placeholder = st.empty()


        with col_right_panel:
            # Seletor de data
            default_start_date = df_temporal['mes'].min()
            default_end_date = df_temporal['mes'].max()

            selected_dates_raw = st.date_input(
                "Período:", # Label para a coluna
                value=(default_start_date, default_end_date),
                min_value=df_temporal['mes'].min(),
                max_value=df_temporal['mes'].max(),
                key='chart_date_range_picker_main_temporal', # Chave única
            )
            
            # Processa selected_dates_raw para garantir que seja sempre uma tupla de 2
            if len(selected_dates_raw) == 2:
                selected_dates = selected_dates_raw
            elif len(selected_dates_raw) == 1: # Se apenas uma data for selecionada, defina como um período de um dia
                 selected_dates = (selected_dates_raw[0], selected_dates_raw[0])
            else: # Caso contrário, volta para o padrão de todo o período
                selected_dates = (default_start_date, default_end_date)


            # Exibe a data formatada
            st.markdown(
                f"<div class='chart-date-selector-display'>"
                f"🗓️ {selected_dates[0].strftime('%d %b, %Y')} - {selected_dates[1].strftime('%d %b, %Y')}"
                f"</div>", 
                unsafe_allow_html=True
            )
        
        st.markdown("<br>", unsafe_allow_html=True)  # Espaço entre os widgets e o gráfico
        # --- Lógica para calcular as métricas com base nas seleções ---
        # Estes cálculos precisam ser feitos DEPOIS que 'selected_metric_name_display' e 'selected_dates'
        # foram definidos pelos seus respectivos widgets nas colunas.
        # Como o Streamlit executa o script do topo para baixo em cada interação,
        # os valores dos widgets já estarão atualizados quando esta parte do código for executada.
        
        start_date_for_metrics = selected_dates[0]
        end_date_for_metrics = selected_dates[1]

        avg_value, percent_change = calculate_metrics_for_period(
            df_temporal, # Usamos df_temporal completo, a função filtra pelo range
            start_date_for_metrics, 
            end_date_for_metrics, 
            selected_chart_metric_key 
        )
        # --- Preencher o placeholder do display de métrica na coluna esquerda ---
        with col_left_panel: # Reentramos no contexto da coluna esquerda para atualizar o placeholder
                avg_value_placeholder.markdown(
                    f"<div class='chart-metric-display'>"
                    f"<span>{selected_metric_name_display}:</span>"
                    f"<span class='chart-metric-pill'>{avg_value:,.2f}</span>"
                    f"<span class='chart-metric-percent-pill'>{'▲' if percent_change >= 0 else '▼'} {abs(percent_change):,.2f}%</span>"
                    f"</div>", 
                    unsafe_allow_html=True
                )
    with st.container(border=True):    # Gráfico temporal principal
        st.markdown(f"<h5 style='text-align: center'>{selected_metric_name_display} ao Longo do Tempo</h5>", unsafe_allow_html=True)
        
            # Filtrar o DataFrame para o plot com base no `selected_dates`
        df_filtered_for_plot = df_temporal[
                (df_temporal['mes'] >= selected_dates[0]) & 
                (df_temporal['mes'] <= selected_dates[1])
            ].copy()

        st.plotly_chart(
                plot_single_temporal_series(
                    df_plot=df_filtered_for_plot, 
                    selected_y_col=selected_chart_metric_key, 
                    y_axis_name=selected_metric_name_display, 
                    line_color=selected_chart_metric_color 
                ),
                use_container_width=True
            )
        
    # --- SEÇÕES DE GRÁFICOS RESTANTES (MANTIDAS DO CÓDIGO ANTIGO) ---
    
    st.markdown("<div class='section-header'><h3>🔥 Matriz de Correlação</h3></div>", unsafe_allow_html=True)
    with st.container(border=True):
        st.markdown("<h5 style='text-align: center'>Matriz de Correlação - Inadimplência vs Indicadores</h5>", unsafe_allow_html=True)
        st.plotly_chart(plot_matriz_correlacao(df_temporal), use_container_width=True) # Usa df_temporal completo para matriz
    
    st.markdown("<div class='section-header'><h3>🎯 Análises de Dispersão</h3></div>", unsafe_allow_html=True)
    with st.container(border=True):
        indicador_selecionado_scatter = st.selectbox( # Nova chave para diferenciar do selectbox principal
            "Selecione um indicador para análise detalhada:",
            options=['taxa_desemprego', 'valor_ipca', 'taxa_selic_meta'],
            format_func=lambda x: indicadores_nomes.get(x, x),
            key='scatter_indicator_selector' # Chave única
        )
        nome_indicador_scatter = f"{indicadores_nomes.get(indicador_selecionado_scatter, '')}" # Removi '%' aqui para flexibilidade no plot
    with st.container(border=True):
        st.markdown(f"<h5 style='text-align: center'>Correlação: Inadimplência vs {nome_indicador_scatter} (%)</h5>", unsafe_allow_html=True)
        st.plotly_chart(plot_scatter_correlacao(df_temporal, indicador_selecionado_scatter, nome_indicador_scatter), use_container_width=True)
    
    st.markdown("<div class='section-header'><h2>💡 Insights e Conclusões</h2></div>", unsafe_allow_html=True)
        
    if correlacoes:
        # Usamos um contêiner nativo do Streamlit para o fundo e a borda, que é mais estável
        with st.container(border=True):
            # Renderizamos a parte de interpretação primeiro
            st.markdown("""
            <div class="profile-section-title">Como interpretar as correlações</div>
            <div class="interpretation-text">
                <b>Correlação Positiva (+)</b>: Quando o indicador aumenta, a inadimplência tende a aumentar.<br>
                <b>Correlação Negativa (-)</b>: Quando o indicador aumenta, a inadimplência tende a diminuir.<br>
                <b>Força da Correlação:</b><br>
                <ul>
                    <li><b>0.7 a 1.0</b>: Forte</li>
                    <li><b>0.5 a 0.7</b>: Moderada</li>
                    <li><b>0.3 a 0.5</b>: Fraca</li>
                    <li><b>0.0 a 0.3</b>: Muito Fraca</li>
                </ul>
                <b>Significância (p-valor &lt; 0.05)</b>: A relação observada provavelmente não é aleatória.
            </div>
            """, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("<div class='profile-section-title'>Análise Automática dos Dados</div>", unsafe_allow_html=True)
            for indicador, dados in correlacoes.items():
                nome = indicadores_nomes[indicador]
                corr = dados['pearson']['corr']
                p_val = dados['pearson']['p_value']
                
                # Lógica para definir a cor e o texto do insight
                if p_val < 0.05:
                    if corr > 0.5:
                        insight = "Positiva Forte/Moderada. Risco Elevado."
                        pill_class = "pill-red"
                    elif corr < -0.5:
                        insight = "Negativa Forte/Moderada. Efeito Protetor."
                        pill_class = "pill-green"
                    else:
                        insight = "Fraca, mas Significativa."
                        pill_class = "pill-yellow"
                else:
                    insight = "Não Significativa. Relação Inconclusiva."
                    pill_class = "pill-yellow"
                
                st.markdown(f"""
                    <div class="feature-row">
                        <span class="feature-label"><strong>{nome}</strong></span>
                        <span class="categorical-pill {pill_class}">{insight}</span>
                    </div>
                """, unsafe_allow_html=True)
    else:
        with st.container(border=True):
            st.warning("Dados insuficientes para análise de correlação.")
else:
    st.warning("Não há dados temporais disponíveis para exibir o dashboard.")