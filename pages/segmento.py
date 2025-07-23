import streamlit as st
from components.data_loader import get_dados_por_segmento, get_dados_top_n_segmento
from components.plot_utils import plot_top_segmento_horizontal, plot_segmento_volume, plot_segmento_inadimplencia, plot_matriz_correlacao, plot_scatter_correlacao
from pages.Home import format_big_number, client

# --- 1. FUNÇÃO PARA CARREGAR O CSS ---
def carregar_css(caminho_arquivo):
    """Lê um arquivo CSS e o aplica ao app Streamlit."""
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado. Verifique o caminho.")

# Carrega os estilos
carregar_css("style.css")


# --- CABEÇALHO DA PÁGINA ---
st.markdown("<div class='dashboard-title'><h1>📊 Análise por Segmento</h1></div>", unsafe_allow_html=True)
st.markdown("""<div class='dashboard-subtitle' style='text-align: center;'>
    <h4>Uma visão abrangente dos segmentos de crédito no Brasil</h4><br>
</div>
""", unsafe_allow_html=True)

# --- 2. SEÇÃO FIXA: CARDS DE KPI PARA PF E PJ (COM NOVO ESTILO CORRIGIDO) ---
st.markdown("<div class='section-header'><h3>Tipo de Cliente</h3></div>", unsafe_allow_html=True)
with st.spinner("Buscando dados de PF e PJ..."):
    try:
        df_cliente = get_dados_por_segmento(client, 'cliente')
        pf_data_df = df_cliente[df_cliente['cliente'] == 'PF']
        pj_data_df = df_cliente[df_cliente['cliente'] == 'PJ']

        col_pf, col_pj = st.columns(2)
        
        # --- Coluna da Esquerda: Pessoa Física (PF) ---
        with col_pf:
            if not pf_data_df.empty:
                pf_data = pf_data_df.iloc[0]
                
                # Extrai e formata os dados
                vol_val, vol_suf = format_big_number(pf_data['volume_carteira_total'])
                inad_val = f"{pf_data['taxa_inadimplencia_media']:.2%}"

                # Monta o HTML do card diretamente aqui
                pf_card_html = f"""
                <div class="status-banner">
                    Pessoa Física (PF)
                    <div class="custom-card-section" style="margin-top: 8px; padding: 1.5rem;">
                        <div style="display: flex; justify-content: space-around; align-items: center;">
                            <div style="text-align: center;">
                                <div class="financial-metric-title">Volume Total da Carteira</div>
                                <div class="financial-metric-value-container" style="justify-content: center;">
                                    <div class="financial-metric-value">R$ {vol_val}</div>
                                    <div class="unit-pill">{vol_suf}</div>
                                </div>
                            </div>
                            <div style="text-align: center;">
                                <div class="financial-metric-title">Taxa de Inadimplência</div>
                                <div class="financial-metric-value-container" style="justify-content: center;">
                                    <div class="financial-metric-value">{inad_val}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                """
                # Renderiza o card, garantindo o uso de unsafe_allow_html=True
                st.markdown(pf_card_html, unsafe_allow_html=True)
            else:
                st.info("Não há dados para Pessoa Física.")
        
        st.markdown("<br>", unsafe_allow_html=True)

        # --- Coluna da Direita: Pessoa Jurídica (PJ) ---
        with col_pj:
            if not pj_data_df.empty:
                pj_data = pj_data_df.iloc[0]

                # Extrai e formata os dados
                vol_val_pj, vol_suf_pj = format_big_number(pj_data['volume_carteira_total'])
                inad_val_pj = f"{pj_data['taxa_inadimplencia_media']:.2%}"

                # Monta o HTML do card diretamente aqui
                pj_card_html = f"""
                <div class="status-banner">
                    Pessoa Jurídica (PJ)
                    <div class="custom-card-section" style="margin-top: 8px; padding: 1.5rem;">
                        <div style="display: flex; justify-content: space-around; align-items: center;">
                            <div style="text-align: center;">
                                <div class="financial-metric-title">Volume Total da Carteira</div>
                                <div class="financial-metric-value-container" style="justify-content: center;">
                                    <div class="financial-metric-value">R$ {vol_val_pj}</div>
                                    <div class="unit-pill">{vol_suf_pj}</div>
                                </div>
                            </div>
                            <div style="text-align: center;">
                                <div class="financial-metric-title">Taxa de Inadimplência</div>
                                <div class="financial-metric-value-container" style="justify-content: center;">
                                    <div class="financial-metric-value">{inad_val_pj}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                """
                # Renderiza o card, garantindo o uso de unsafe_allow_html=True
                st.markdown(pj_card_html, unsafe_allow_html=True)
            else:
                st.info("Não há dados para Pessoa Jurídica.")

    except Exception as e:
        st.error(f"Não foi possível carregar os dados por tipo de cliente. Erro: {e}")


# --- 3. SEÇÃO DINÂMICA COM ABAS ---
st.markdown("<div class='section-header'><h3>Análise Detalhada por Outros Segmentos</h3></div>", unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)
# Define os nomes e cria as abas
tab_names = ["Porte do Cliente", "Modalidade", "Ocupação", "CNAE Seção", "CNAE Subclasse"]
tabs = st.tabs(tab_names)

# --- Funções auxiliares para popular as abas ---

def render_full_charts(segmento_dim, display_name):
    """Renderiza os dois gráficos completos para uma dimensão."""
    with st.spinner(f"Analisando dados por {display_name}..."):
        df_segmento = get_dados_por_segmento(client, segmento_dim)
    with st.container(border=True):
        st.subheader(f"Volume da Carteira por {display_name}")
        st.plotly_chart(plot_segmento_volume(df_segmento, segmento_dim, f""), use_container_width=True)
    with st.container(border=True):
        st.subheader(f"Inadimplência Média por {display_name}") 
        st.plotly_chart(plot_segmento_inadimplencia(df_segmento, segmento_dim, f""), use_container_width=True)

def render_top_n_analysis(segmento_dim, display_name):
    """Renderiza a análise de Top N para uma dimensão."""
    analise_tipo = st.radio(
        "Escolha o tipo de análise Top 20:",
        ('Maiores Riscos (Inadimplência)', 'Maiores Volumes (Carteira)'),
        horizontal=True, key=f"radio_top_n_{segmento_dim}"
    )
    if 'Riscos' in analise_tipo:
        with st.spinner(f"Buscando Top 20 {display_name} por Risco..."):
            df_top = get_dados_top_n_segmento(client, segmento_dim, top_n=20, order_by='taxa_inadimplencia_media')
        st.plotly_chart(plot_top_segmento_horizontal(df_top, segmento_dim, 'taxa_inadimplencia_media', f"Top 20 {display_name} por Taxa de Inadimplência Média"), use_container_width=True)
    else:
        with st.spinner(f"Buscando Top 20 {display_name} por Volume..."):
            df_top = get_dados_top_n_segmento(client, segmento_dim, top_n=20, order_by='volume_carteira_total')
        st.plotly_chart(plot_top_segmento_horizontal(df_top, segmento_dim, 'volume_carteira_total', f"Top 20 {display_name} por Volume da Carteira"), use_container_width=True)

# --- Estrutura correta para popular as abas ---

with tabs[0]: # Aba "Porte do Cliente"
    render_full_charts('porte', 'Porte do Cliente')

with tabs[1]: # Aba "Modalidade"
    render_top_n_analysis('modalidade', 'Modalidade')

with tabs[2]: # Aba "Ocupação"
    render_top_n_analysis('ocupacao', 'Ocupação')

with tabs[3]: # Aba "CNAE Seção"
    render_top_n_analysis('cnae_secao', 'CNAE Seção')

with tabs[4]: # Aba "CNAE Subclasse"
    render_top_n_analysis('cnae_subclasse', 'CNAE Subclasse')
