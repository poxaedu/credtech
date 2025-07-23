import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path

# Adicione o diretório pai ao PATH para resolver o import
sys.path.append(str(Path(__file__).parent.parent))

from components.data_loader import get_bigquery_client, get_dados_comparativo_riscos, get_top_combinacoes_risco, get_dados_por_segmento
from components.plot_utils import plot_comparativo_riscos, plot_top_combinacoes_risco
from pages.Home import carregar_css, format_big_number

# Configuração da página
st.set_page_config(page_title="Comparativo de Riscos", layout="wide")

# Carregamento do CSS
carregar_css("style.css")

# Inicialização do cliente BigQuery
client = get_bigquery_client()

st.markdown("<div class='dashboard-title'><h1>⚠️ Comparativo de Riscos</h1></div>", unsafe_allow_html=True)

# --- Seção 1: Top Combinações de Risco ---
st.markdown("<div class='section-header'><h3>🔥 Top Combinações de Maior Risco</h3></div>", unsafe_allow_html=True)

try:
    with st.spinner("Carregando as combinações de maior risco..."):
        df_top_combinacoes = get_top_combinacoes_risco(client, top_n=15)

    if not df_top_combinacoes.empty:
        # Exibir métricas principais
        col1, col2, col3 = st.columns(3)

        with col1:
            maior_risco = df_top_combinacoes.iloc[0]
            st.markdown(f"""
            <div class="financial-metric-item">
                <div class="financial-metric-title">Maior Risco Identificado</div>
                <div class="financial-metric-value-container">
                    <div class="financial-metric-value">{maior_risco['taxa_inadimplencia_media']:.2%}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            taxa_media = df_top_combinacoes['taxa_inadimplencia_media'].mean()
            st.markdown(f"""
            <div class="financial-metric-item">
                <div class="financial-metric-title">Taxa Média (Top 15)</div>
                <div class="financial-metric-value-container">
                    <div class="financial-metric-value">{taxa_media:.2%}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            diferenca = df_top_combinacoes.iloc[0]['taxa_inadimplencia_media'] - df_top_combinacoes.iloc[-1]['taxa_inadimplencia_media']
            st.markdown(f"""
            <div class="financial-metric-item">
                <div class="financial-metric-title">Diferença Maior vs Menor</div>
                <div class="financial-metric-value-container">
                    <div class="financial-metric-value">{diferenca:.2%}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("<h5 style='text-align: center'>Combinações de Maior Risco</h5>", unsafe_allow_html=True)
            # Gráfico das top combinações
            st.plotly_chart(
                plot_top_combinacoes_risco(df_top_combinacoes, title=""), use_container_width=True
            )
            # Tabela detalhada
            with st.expander("📊 Ver Dados Detalhados das Top Combinações"):
                df_display = df_top_combinacoes.copy()
                df_display['Taxa de Inadimplência'] = df_display['taxa_inadimplencia_media'].apply(lambda x: f"{x:.2%}")
                df_display = df_display[['combinacao_risco', 'Taxa de Inadimplência']].rename(columns={
                    'combinacao_risco': 'Combinação de Risco'
                })
                st.dataframe(df_display, use_container_width=True)

    else:
        st.warning("Não foi possível carregar os dados de combinações de risco.")

except Exception as e:
    st.error("Erro ao carregar as combinações de risco.")
    st.exception(e)

st.markdown("<br>", unsafe_allow_html=True)

# --- Seção 2: Análise Comparativa Personalizada ---
st.markdown("<div class='section-header'><h3>🔍 Analise Comparativa Personalizada</h3></div>", unsafe_allow_html=True)

st.markdown("""
        <h4>Configure sua Análise</h4>
        <p>Selecione as dimensões que deseja comparar para identificar grupos de risco específicos</p>
""", unsafe_allow_html=True)

    # Opções de dimensões disponíveis
dimensoes_disponiveis = {
        'UF': 'uf',
        'Tipo de Cliente': 'cliente',
        'Modalidade': 'modalidade',
        'Porte da Empresa': 'porte',
        'Ocupação (PF)': 'ocupacao',
        'Seção CNAE (PJ)': 'cnae_secao'
    }
with st.container(border=True):
        st.markdown("***Dimensões Disponíveis:***")
        dimensoes_selecionadas = st.multiselect(
            "Escolha até 3 dimensões para comparar:",
            options=list(dimensoes_disponiveis.keys()),
            default=['Tipo de Cliente', 'Modalidade'],
            max_selections=3,
            help="Selecione as dimensões que deseja analisar em conjunto"
        )
st.markdown("<br>", unsafe_allow_html=True)
# Executar análise comparativa
if dimensoes_selecionadas:
    try:
        # Converter nomes para códigos das dimensões
        dimensoes_codigo = [dimensoes_disponiveis[dim] for dim in dimensoes_selecionadas]

        with st.spinner(f"Analisando comparativo por {', '.join(dimensoes_selecionadas)}..."):
            df_comparativo = get_dados_comparativo_riscos(client, dimensoes_codigo)

        if not df_comparativo.empty:

            if not df_comparativo.empty:
                # Criar coluna de identificação combinada
                if len(dimensoes_codigo) == 1:
                    df_comparativo['identificacao'] = df_comparativo[dimensoes_codigo[0]].astype(str)
                else:
                    df_comparativo['identificacao'] = df_comparativo[dimensoes_codigo].apply(
                        lambda row: ' - '.join(row.astype(str)), axis=1
                    )

                # Métricas resumo
                col_resumo1, col_resumo2, col_resumo3, col_resumo4 = st.columns(4)

                with col_resumo1:
                    max_risco = df_comparativo['taxa_inadimplencia_media'].max()
                    st.markdown(f"""
                    <div class="financial-metric-item">
                        <div class="financial-metric-title">Maior Risco</div>
                        <div class="financial-metric-value-container">
                            <div class="financial-metric-value">{max_risco:.2%}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_resumo2:
                    min_risco = df_comparativo['taxa_inadimplencia_media'].min()
                    st.markdown(f"""
                    <div class="financial-metric-item">
                        <div class="financial-metric-title">Menor Risco</div>
                        <div class="financial-metric-value-container">
                            <div class="financial-metric-value">{min_risco:.2%}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_resumo3:
                    media_risco = df_comparativo['taxa_inadimplencia_media'].mean()
                    st.markdown(f"""
                    <div class="financial-metric-item">
                        <div class="financial-metric-title">Risco Médio</div>
                        <div class="financial-metric-value-container">
                            <div class="financial-metric-value">{media_risco:.2%}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_resumo4:
                    variacao = max_risco - min_risco
                    st.markdown(f"""
                    <div class="financial-metric-item">
                        <div class="financial-metric-title">Variação</div>
                        <div class="financial-metric-value-container">
                            <div class="financial-metric-value">{variacao:.2%}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with st.container(border=True):
                    
                # Gráfico comparativo
                    st.plotly_chart(
                        plot_comparativo_riscos(
                            df_comparativo,
                            dimensoes_codigo,
                            f"Comparativo de Risco por {', '.join(dimensoes_selecionadas)}"
                        ),
                        use_container_width=True
                    )

                    # Tabela detalhada
                    with st.expander("📋 Dados Detalhados da Análise Comparativa"):
                        df_display_comp = df_comparativo.copy()
                        df_display_comp['Taxa de Inadimplência'] = df_display_comp['taxa_inadimplencia_media'].apply(lambda x: f"{x:.2%}")

                        # Selecionar colunas para exibição
                        colunas_exibir = ['identificacao', 'Taxa de Inadimplência']
                        df_display_comp = df_display_comp[colunas_exibir].rename(columns={
                            'identificacao': 'Identificação'
                        })

                        st.dataframe(df_display_comp, use_container_width=True)

            else:
                st.warning("Nenhum resultado encontrado com os filtros aplicados.")
        else:
            st.warning("Não foi possível carregar os dados para a análise comparativa.")

    except Exception as e:
        st.error("Erro ao executar a análise comparativa.")
        st.exception(e)

else:
    st.info("👆 Selecione pelo menos uma dimensão para iniciar a análise comparativa.")