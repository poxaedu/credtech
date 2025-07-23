import streamlit as st
import pandas as pd
from components.ml_utils import credit_risk_predictor, get_unique_values_for_features
from pages.Home import client
import plotly.graph_objects as go
import os
from datetime import datetime

# Configuração da página
st.set_page_config(page_title="Predição de Risco PJ", layout="wide")

# CSS
def carregar_css(caminho_arquivo):
    try:
        with open(caminho_arquivo) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning(f"Arquivo CSS '{caminho_arquivo}' não encontrado.")

carregar_css("style.css")

# Título
st.markdown("<div class='dashboard-title'><h2>🔮 Predição Inteligente de Risco para Empresas</h2></div>", unsafe_allow_html=True)


# Verificar status do modelo
model_info = credit_risk_predictor.get_model_info()

if not model_info['is_available']:
    st.error("⚠️ **Modelo não disponível.**")
    
    if not model_info['model_exists']:
        st.info("""💡 **Como treinar o modelo:**
        
        1. Execute o script de treinamento:
        ```bash
        python train_model_clean.py
        ```
        
        2. Aguarde a conclusão do treinamento
        3. Recarregue esta página
        """)
    else:
        st.warning("Modelo encontrado mas não pôde ser carregado. Verifique os logs.")
    
    st.stop()

# Informações do modelo
with st.expander("ℹ️ Informações do Modelo"):
    # Construa todo o conteúdo HTML como uma única string
    html_content = ""
    html_content += "<div class='model-info-table-container'>" # Abertura do container

    if 'last_modified' in model_info:
        last_modified_value = model_info['last_modified'].strftime('%d/%m/%Y %H:%M')
        html_content += (
            f"<div class='model-info-table-row'>"
            f"<span class='model-info-label'>Última atualização:</span>"
            f"<span class='model-info-pill'>{last_modified_value}</span>"
            f"</div>"
        )
    
    status_value = "Modelo carregado e pronto para uso ✅"
    html_content += (
        f"<div class='model-info-table-row'>"
        f"<span class='model-info-label'>Status:</span>"
        f"<span class='model-info-status-pill'>{status_value}</span>"
        f"</div>"
    )
    
    foco_value = "Exclusivamente Pessoa Jurídica (PJ)"
    html_content += (
        f"<div class='model-info-table-row'>"
        f"<span class='model-info-label'>Foco:</span>"
        f"<span class='model-info-pill'>{foco_value}</span>"
        f"</div>"
    )
    
    features_value = f"{len(credit_risk_predictor.feature_columns)} variáveis"
    html_content += (
        f"<div class='model-info-table-row'>"
        f"<span class='model-info-label'>Features:</span>"
        f"<span class='model-info-pill'>{features_value}</span>"
        f"</div>"
    )

    html_content += "</div>" # Fechamento do container

    # Renderize todo o HTML de uma vez dentro do expander
    st.markdown(html_content, unsafe_allow_html=True)

# Seção principal de predição
st.markdown("<div class='section-header'><h3>🔮 Calcular Risco do Cliente Jurídico</h3></div>", unsafe_allow_html=True)

unique_values = get_unique_values_for_features(client)

if not unique_values:
    st.error("Não foi possível carregar as opções. Verifique a conexão com o banco de dados.")
    st.stop()



with st.expander("📋 Dados da Empresa"):
    
    # Inputs do usuário - apenas PJ
    uf = st.selectbox("Estado (UF)", unique_values.get('uf', []))
    modalidade = st.selectbox("Modalidade de Crédito", unique_values.get('modalidade', []))
    porte = st.selectbox("Porte da Empresa", unique_values.get('porte', []))
    
    # CNAE para PJ
    cnae_secao = st.selectbox("CNAE Seção", [''] + unique_values.get('cnae_secao', []))
    cnae_subclasse = st.selectbox("CNAE Subclasse", [''] + unique_values.get('cnae_subclasse', []))
    
    # Informações sobre valores padrão
    st.info("""
    💡 **Informações Automáticas:**
    - Valores históricos de inadimplência são aplicados automaticamente
    - Baseado no porte da empresa selecionado
    - Dados de segmento são calculados internamente
    - Modelo treinado exclusivamente com dados de PJ
    """)
    
    # Botão de predição
    if st.button("🎯 Calcular Risco", type="primary", use_container_width=True):
        input_data = {
            'uf': uf,
            'modalidade': modalidade,
            'porte': porte,
            'cnae_secao': cnae_secao if cnae_secao else None,
            'cnae_subclasse': cnae_subclasse if cnae_subclasse else None
        }
        
        try:
            with st.spinner("Calculando risco..."):
                prediction = credit_risk_predictor.predict_risk(input_data)
            
            # Armazena resultado na sessão
            st.session_state['prediction_result'] = prediction
            st.success("✅ Risco calculado com sucesso!")
            
        except Exception as e:
            st.error(f"❌ Erro no cálculo: {e}")
            st.error("Verifique se o modelo foi treinado corretamente com o script train_model_clean.py")

with st.container(border=True):
    st.subheader("📊 Resultado da Análise")
    
    # Exibe resultado se disponível
    if 'prediction_result' in st.session_state and st.session_state['prediction_result'] is not None:
        result = st.session_state['prediction_result']
        
        # Card principal com o resultado (Gauge chart) - Mantido como está, pois o pedido era sobre as métricas e a tabela
        risk_pct = result['risk_percentage']
        risk_category = result['risk_category']
        risk_color = result['risk_color']
        
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = risk_pct,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Risco de Inadimplência (%)"},
            delta = {'reference': 5.0},  # Referência de 5%
            gauge = {
                'axis': {'range': [None, 15]},
                'bar': {'color': risk_color},
                'steps': [
                    {'range': [0, 2], 'color': "lightgreen"},
                    {'range': [2, 5], 'color': "yellow"},
                    {'range': [5, 15], 'color': "lightcoral"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 10
                }
            }
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
        
        # Métricas detalhadas transformadas em cards
        col_r1, col_r2, col_r3 = st.columns(3)
        with col_r1:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Risco Calculado</div>
                    <div class="metric-value" style="color: {risk_color};">{risk_pct:.2f}%</div>
                    <div class="metric-delta"></div>
                </div>
            """, unsafe_allow_html=True)
        with col_r2:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Categoria</div>
                    <div class="metric-value">{risk_category}</div>
                    <div class="metric-delta"></div>
                </div>
            """, unsafe_allow_html=True)
        with col_r3:
            ci = result['confidence_interval']
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Intervalo de Confiança</div>
                    <div class="metric-value">{ci['lower']:.1f}% - {ci['upper']:.1f}%</div>
                    <div class="metric-delta"></div>
                </div>
            """, unsafe_allow_html=True)
        
        # Recomendação - Mantido como está
        st.markdown("### 💡 Recomendação")
        if risk_category == "BAIXO":
            st.success("✅ **APROVADO**: Risco baixo. Empresa elegível para concessão de crédito.")
        elif risk_category == "MÉDIO":
            st.warning("⚠️ **ANÁLISE ADICIONAL**: Risco moderado. Recomenda-se avaliar garantias adicionais ou reduzir limite.")
        else:
            st.error("❌ **NEGADO**: Risco alto. Não recomendado para concessão de crédito.")
        
        # Detalhes técnicos em formato de tabela
        with st.expander("🔍 Detalhes da Análise"):
            st.markdown("<h5>Dados informados:</h5>", unsafe_allow_html=True)
            
            # Preparar dados informados para DataFrame
            input_data_list = []
            for key, value in result['input_data'].items():
                if value is not None:
                    input_data_list.append({"Característica": key.replace('_', ' ').title(), "Valor": value})
            
            input_df = pd.DataFrame(input_data_list)
            st.dataframe(input_df, use_container_width=True, hide_index=True)
            
            st.markdown("<h5>Valores aplicados automaticamente:</h5>", unsafe_allow_html=True)
            
            # Preparar valores aplicados automaticamente para DataFrame
            processed = result.get('processed_input', {})
            auto_fields_display = {
                'total_vencido_15d_segmento': 'Total Vencido 15 Dias Segmento',
                'total_inadimplida_arrastada_segmento': 'Total Inadimplida Arrastada Segmento',
                'media_taxa_inadimplencia_original': 'Média Taxa Inadimplência Original',
                'contagem_clientes_unicos_segmento': 'Contagem Clientes Únicos Segmento'
            }
            
            auto_data_list = []
            for field, display_name in auto_fields_display.items():
                if field in processed:
                    value = processed[field]
                    if isinstance(value, float):
                        auto_data_list.append({"Característica": display_name, "Valor": f"{value:.6f}"})
                    else:
                        auto_data_list.append({"Característica": display_name, "Valor": f"{value:,}"})
            
            auto_df = pd.DataFrame(auto_data_list)
            st.dataframe(auto_df, use_container_width=True, hide_index=True)
    
    else:
        st.info("☝🏻 Preencha os dados da empresa e clique em 'Calcular Risco' para ver a análise.")

# Seção de informações
st.divider()
st.markdown("<div class='section-header'><h3>ℹ️ Como Funciona</h3></div>", unsafe_allow_html=True)
col_info1, col_info2 = st.columns(2)

with col_info1:
    st.markdown("""
    <div class="profile-card">
        <div class="profile-section">
            <div class="profile-section-title">O que é analisado</div>
            <div class="feature-row">
                <span class="feature-label">📍 Localização</span>
                <span class="feature-value">Estado (UF) da empresa</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">💳 Produto</span>
                <span class="feature-value">Modalidade de crédito solicitada</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">📏 Porte</span>
                <span class="feature-value">Classificação da empresa (Micro, Pequeno, Médio, Grande)</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">🏢 Atividade</span>
                <span class="feature-value">CNAE Seção e Subclasse</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">📊 Histórico</span>
                <span class="feature-value">Dados de inadimplência do segmento</span>
            </div>
        </div>
        <div class="profile-section">
            <div class="profile-section-title">Resultado Fornecido</div>
            <div class="feature-row">
                <span class="feature-label">Taxa de Risco</span>
                <span class="feature-value categorical-pill">Percentual</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">Classificação</span>
                <span class="feature-value categorical-pill">Baixo / Médio / Alto</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">Intervalo de Confiança</span>
                <span class="feature-value categorical-pill">Estimativa</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">Recomendação</span>
                <span class="feature-value categorical-pill">Sim / Não / Analisar</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_info2:
    st.markdown("""
    <div class="profile-card">
        <div class="profile-section">
            <div class="profile-section-title">Tecnologia Utilizada</div>
            <div class="feature-row">
                <span class="feature-label">🤖 Algoritmo</span>
                <span class="feature-value">Random Forest (Machine Learning)</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">📈 Base de dados</span>
                <span class="feature-value">Histórico de inadimplência PJ</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">✅ Validação</span>
                <span class="feature-value">Cross-validation 5-fold</span>
            </div>
            <div class="feature-row">
                <span class="feature-label">🎯 Precisão</span>
                <span class="feature-value">Modelo treinado com dados reais</span>
            </div>
        </div>
        <div class="profile-section">
            <div class="profile-section-title">⚠️ Informações Importantes</div>
            <p class="interpretation-text">Análise focada <b>exclusivamente</b> em Pessoa Jurídica.</p>
            <p class="interpretation-text">Baseada em padrões históricos de segmento, não individuais.</p>
            <p class="interpretation-text">Resultado é uma estimativa probabilística.</p>
            <p class="interpretation-text">Decisão final deve considerar múltiplos fatores.</p>
            <p class="interpretation-text">Modelo é atualizado periodicamente para garantir a acurácia.</p>
            <br>
        </div>
    </div>
    """, unsafe_allow_html=True)