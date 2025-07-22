import streamlit as st
import json
from components.data_loader import get_bigquery_client, get_dados_por_segmento, get_dados_top_n_segmento
from components.plot_utils import plot_segmento_volume, plot_segmento_inadimplencia, plot_top_segmento_horizontal
from Home import carregar_css, client, format_big_number # Importando format_big_number e client do Home.py

carregar_css("style.css")

st.markdown("<div class='section-header' style='content=center'><h2>📊 Análise por Segmento</h2></div>", unsafe_allow_html=True)

# --- Cartões de Volume PJ e PF ---
st.markdown("<h3>Volume da Carteira por Tipo de Cliente</h3>", unsafe_allow_html=True)
try:
    # Puxa os dados para a dimensão 'porte'
    df_porte = get_dados_por_segmento(client, 'porte')

    if not df_porte.empty:
        # Filtra para PJ e PF
        pj_data = df_porte[df_porte['porte'] == 'PJ']
        pf_data = df_porte[df_porte['porte'] == 'PF']

        col_pj, col_pf = st.columns(2)

        with col_pj:
            if not pj_data.empty:
                pj_volume_val, pj_volume_sufixo = format_big_number(pj_data['volume_carteira_total'].iloc[0])
                st.markdown(f"""
                <div class="financial-metric-item">
                    <div class="financial-metric-title">Volume Carteira PJ</div>
                    <div class="financial-metric-value-container">
                        <div class="financial-metric-value">R$ {pj_volume_val}</div>
                        <div class="unit-pill">{pj_volume_sufixo}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("Dados para PJ não disponíveis.")

        with col_pf:
            if not pf_data.empty:
                pf_volume_val, pf_volume_sufixo = format_big_number(pf_data['volume_carteira_total'].iloc[0])
                st.markdown(f"""
                <div class="financial-metric-item">
                    <div class="financial-metric-title">Volume Carteira PF</div>
                    <div class="financial-metric-value-container">
                        <div class="financial-metric-value">R$ {pf_volume_val}</div>
                        <div class="unit-pill">{pf_volume_sufixo}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("Dados para PF não disponíveis.")
    else:
        st.warning("Não foi possível carregar os dados de volume por tipo de cliente (PJ/PF).")

except Exception as e:
    st.error("Ocorreu um erro ao carregar os dados de PJ/PF.")
    st.exception(e)

st.markdown("---") # Separador visual

# --- Lógica de Análise por Segmento Existente ---
segmento_dim_options = ['modalidade', 'ocupacao', 'porte', 'cnae_secao', 'cnae_subclasse']
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
