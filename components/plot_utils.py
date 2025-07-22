# components/plot_utils.py

import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

logger = logging.getLogger(__name__)


def plot_choropleth_brasil(df_uf: pd.DataFrame, geojson: dict, title: str) -> go.Figure:
    """
    Cria um mapa coroplético do Brasil usando go.Choropleth com hover corrigido.
    """
    if df_uf.empty:
        return go.Figure().update_layout(title_text=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    fig = go.Figure(data=go.Choropleth(
        geojson=geojson,
        locations=df_uf['uf'],
        featureidkey="properties.UF_05", 
        z=df_uf['taxa_inadimplencia_media'],
        colorscale='Aggrnyl_r',
        colorbar_title='Inadimplência Média (%)',
        marker_line_color='white',
        marker_line_width=0.5,
        
        # customdata agora só precisa do volume, pois usamos %{location} para a UF
        customdata=df_uf[['volume_carteira_total']],
        
        # --- HOVERTEMPLATE CORRIGIDO E OTIMIZADO ---
        hovertemplate=(
            '<b>Estado: %{location}</b><br><br>' + # '%{location}' pega a UF direto, é mais robusto
            'Inadimplência Média: %{z:.2%}<br>' +
            'Volume da Carteira: R$ %{customdata[0]:,.2f}' +
            '<extra></extra>' # Remove informações extras do tooltip
        )
    ))

    fig.update_layout(
        title_text=title,
        geo=dict(
            scope='south america',
            fitbounds="locations",
            visible=False,
        ),
        margin={"r":0,"t":40,"l":0,"b":0}
    )
    return fig

def plot_carteira_uf(df_agregado_uf: pd.DataFrame, title: str = "") -> go.Figure:
    if df_agregado_uf.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    fig = px.bar(df_agregado_uf, x='uf', y='volume_carteira_total', title=title,
                 labels={'uf': 'UF', 'volume_carteira_total': 'Volume da Carteira Ativa (R$)'},
                 color='volume_carteira_total', color_continuous_scale=px.colors.sequential.algae)
    fig.update_layout(xaxis_title="Unidade Federativa", yaxis_title="Volume da Carteira Ativa (R$)")
    return fig

def plot_segmento_volume(df_agregado: pd.DataFrame, dimension_col: str, title: str) -> go.Figure:
    if df_agregado.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    df_plot = df_agregado.sort_values(by='volume_carteira_total', ascending=False)
    fig = px.bar(df_plot, x=dimension_col, y='volume_carteira_total', title=title,
                 labels={dimension_col: dimension_col.replace('_', ' ').title(), 'volume_carteira_total': 'Volume (R$)'},
                 color='volume_carteira_total', color_continuous_scale=px.colors.sequential.Plasma)
    fig.update_layout(xaxis_title=dimension_col.replace('_', ' ').title(), yaxis_title="Volume da Carteira Ativa (R$)")
    return fig

def plot_segmento_inadimplencia(df_agregado: pd.DataFrame, dimension_col: str, title: str) -> go.Figure:
    if df_agregado.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    df_plot = df_agregado.sort_values(by='taxa_inadimplencia_media', ascending=False)
    fig = px.bar(df_plot, x=dimension_col, y='taxa_inadimplencia_media', title=title,
                 labels={dimension_col: dimension_col.replace('_', ' ').title(), 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 color='taxa_inadimplencia_media', color_continuous_scale=px.colors.sequential.Redor)
    fig.update_layout(xaxis_title=dimension_col.replace('_', ' ').title(), yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_tendencia_temporal(df_agregado_temporal: pd.DataFrame, title: str = "Inadimplência vs. Indicadores Macroeconômicos") -> go.Figure:
    """
    Gera um gráfico de linhas mostrando a tendência da taxa de inadimplência
    e indicadores macroeconômicos ao longo do tempo.
    Recebe um DataFrame JÁ AGREGADO E JUNTADO do BigQuery.
    """
    if df_agregado_temporal.empty:
        logger.warning(f"DataFrame vazio para plot_tendencia_temporal. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    
    df_plot = df_agregado_temporal.copy()
    df_plot['mes_str'] = pd.to_datetime(df_plot['mes']).dt.to_period('M').astype(str)
    
    fig = go.Figure()

    # Adiciona a linha de inadimplência (eixo Y1)
    fig.add_trace(go.Scatter(x=df_plot['mes_str'], y=df_plot['taxa_inadimplencia_media'],
                             mode='lines+markers', name='Inadimplência Média', yaxis='y1', line=dict(color='red')))

    # Adiciona os indicadores (eixo Y2)
    fig.add_trace(go.Scatter(x=df_plot['mes_str'], y=df_plot['taxa_desemprego'],
                             mode='lines+markers', name='Desemprego', yaxis='y2', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=df_plot['mes_str'], y=df_plot['valor_ipca'],
                             mode='lines+markers', name='IPCA', yaxis='y2', line=dict(color='green')))
    fig.add_trace(go.Scatter(x=df_plot['mes_str'], y=df_plot['taxa_selic_meta'],
                             mode='lines+markers', name='Selic', yaxis='y2', line=dict(color='purple')))

    # AQUI ESTÁ A CORREÇÃO APLICADA:
    fig.update_layout(
        title=title,
        xaxis_title="Mês",
        yaxis=dict(
            title=dict(
                text="Inadimplência (%)",
                font=dict(color="red") # Forma correta de definir a cor
            )
        ),
        yaxis2=dict(
            title="Indicadores (%)",
            overlaying='y',
            side='right'
        ),
        legend=dict(x=0.01, y=0.99, yanchor="top", xanchor="left")
    )
    return fig

def plot_inadimplencia_por_cluster(df_agregado_cluster: pd.DataFrame, title: str = "Taxa Média de Inadimplência por Cluster") -> go.Figure:
    if df_agregado_cluster.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    df_plot = df_agregado_cluster.copy()
    df_plot['cluster_id'] = df_plot['cluster_id'].astype(str)
    df_plot = df_plot.sort_values(by='taxa_inadimplencia_media', ascending=False)
    fig = px.bar(df_plot, x='cluster_id', y='taxa_inadimplencia_media', title=title,
                 labels={'cluster_id': 'Cluster', 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 color='taxa_inadimplencia_media', color_continuous_scale=px.colors.sequential.Viridis)
    fig.update_layout(xaxis_title="Cluster", yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_perfil_cluster(df_full_clusters: pd.DataFrame, cluster_id: int, features_perfil: list, title: str = "Perfil do Cluster") -> go.Figure:
    """Esta função mantém a lógica em Pandas para normalizar os eixos do gráfico de radar."""
    if df_full_clusters.empty or cluster_id not in df_full_clusters['cluster_id'].unique():
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    
    df_cluster_profile = df_full_clusters[df_full_clusters['cluster_id'] == cluster_id][features_perfil].mean().reset_index()
    df_cluster_profile.columns = ['Feature', 'Value']

    for feature in features_perfil:
        min_val, max_val = df_full_clusters[feature].min(), df_full_clusters[feature].max()
        if max_val > min_val:
            df_cluster_profile.loc[df_cluster_profile['Feature'] == feature, 'Value_Normalized'] = (df_cluster_profile.loc[df_cluster_profile['Feature'] == feature, 'Value'] - min_val) / (max_val - min_val)
        else:
            df_cluster_profile.loc[df_cluster_profile['Feature'] == feature, 'Value_Normalized'] = 0.5

    fig = go.Figure(data=go.Scatterpolar(
        r=df_cluster_profile['Value_Normalized'],
        theta=[feat.replace('_', ' ').title() for feat in df_cluster_profile['Feature']],
        fill='toself', name=f'Cluster {cluster_id}'
    ))
    fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), showlegend=False, title=f"{title} {cluster_id}")
    return fig

def plot_top_combinacoes_risco(df_agregado_top_combinacoes: pd.DataFrame, title: str = "Top Combinações de Risco por Taxa de Inadimplência") -> go.Figure:
    if df_agregado_top_combinacoes.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    fig = px.bar(df_agregado_top_combinacoes, y='combinacao_risco', x='taxa_inadimplencia_media', orientation='h', title=title,
                 labels={'combinacao_risco': 'Combinação de Risco', 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 color='taxa_inadimplencia_media', color_continuous_scale=px.colors.sequential.Hot)
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Taxa de Inadimplência Média (%)", yaxis_title="Combinação de Risco")
    return fig

def plot_comparativo_riscos(df_agregado: pd.DataFrame, comparison_dims: list, title: str) -> go.Figure:
    if df_agregado.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    df_plot = df_agregado.copy()
    if len(comparison_dims) > 1:
        df_plot['comparacao'] = df_plot[comparison_dims].apply(lambda row: ' - '.join(row.values.astype(str)), axis=1)
        x_axis_col = 'comparacao'
        x_axis_title = 'Combinação de Comparação'
    else:
        x_axis_col = comparison_dims[0]
        x_axis_title = comparison_dims[0].replace('_', ' ').title()

    df_plot = df_plot.sort_values(by='taxa_inadimplencia_media', ascending=False).head(25) # Limita a 25 para melhor visualização

    fig = px.bar(df_plot, x=x_axis_col, y='taxa_inadimplencia_media', title=title,
                 labels={x_axis_col: x_axis_title, 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 color='taxa_inadimplencia_media', color_continuous_scale=px.colors.sequential.Plasma)
    fig.update_layout(xaxis_title=x_axis_title, yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

# Adicione esta função ao seu components/plot_utils.py

def plot_top_segmento_horizontal(df_top_n: pd.DataFrame, dimension_col: str, metric_col: str, title: str) -> go.Figure:
    """
    Gera um gráfico de barras HORIZONTAL para exibir os Top N segmentos.
    Ideal para categorias com nomes longos como CNAE.
    """
    if df_top_n.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    # Garante a ordenação para o gráfico
    df_plot = df_top_n.sort_values(by=metric_col, ascending=True)

    fig = px.bar(
        df_plot,
        y=dimension_col, # Dimensão no eixo Y para leitura
        x=metric_col,    # Métrica no eixo X
        orientation='h',
        title=title,
        labels={
            dimension_col: dimension_col.replace('_', ' ').title(),
            metric_col: 'Valor'
        },
        color=metric_col,
        color_continuous_scale=px.colors.sequential.Reds if 'inadimplencia' in metric_col else px.colors.sequential.Blues
    )
    fig.update_layout(
        xaxis_title=metric_col.replace('_', ' ').title(),
        yaxis_title=dimension_col.replace('_', ' ').title()
    )
    return fig