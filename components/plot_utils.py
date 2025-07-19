import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# --- Configuração de Logging para este módulo ---
logger = logging.getLogger(__name__)
# O logger principal já é configurado no Home.py, este apenas usa o mesmo.

# --- Funções de Plotagem ---

def plot_inadimplencia_uf(df: pd.DataFrame, title: str = "Taxa Média de Inadimplência por UF") -> go.Figure:
    """
    Gera um gráfico de barras da taxa média de inadimplência por UF.
    Recebe um DataFrame já filtrado.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para plot_inadimplencia_uf. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Agrega os dados por UF
    df_plot = df.groupby('uf')['taxa_inadimplencia_final_segmento'].mean().reset_index()

    fig = px.bar(
        df_plot,
        x='uf',
        y='taxa_inadimplencia_final_segmento',
        title=title,
        labels={'uf': 'UF', 'taxa_inadimplencia_final_segmento': 'Taxa de Inadimplência Média (%)'},
        color='taxa_inadimplencia_final_segmento', # Colorir barras pela taxa
        color_continuous_scale=px.colors.sequential.YlOrRd # Escala de cores para risco (vermelho para maior risco)
    )
    fig.update_layout(xaxis_title="Unidade Federativa", yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_carteira_uf(df: pd.DataFrame, title: str = "Volume Total da Carteira Ativa por UF") -> go.Figure:
    """
    Gera um gráfico de barras do volume total da carteira ativa por UF.
    Recebe um DataFrame já filtrado.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para plot_carteira_uf. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Agrega os dados por UF
    df_plot = df.groupby('uf')['total_carteira_ativa_segmento'].sum().reset_index()

    fig = px.bar(
        df_plot,
        x='uf',
        y='total_carteira_ativa_segmento',
        title=title,
        labels={'uf': 'UF', 'total_carteira_ativa_segmento': 'Volume da Carteira Ativa (R$)'},
        color='total_carteira_ativa_segmento', # Colorir barras pelo volume
        color_continuous_scale=px.colors.sequential.Blues # Escala de cores para volume (azul para maior volume)
    )
    fig.update_layout(xaxis_title="Unidade Federativa", yaxis_title="Volume da Carteira Ativa (R$)")
    return fig

def plot_segmento_volume(df: pd.DataFrame, dimension_col: str, title: str) -> go.Figure:
    """
    Gera um gráfico de barras do volume total da carteira ativa por uma dimensão de segmento.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para plot_segmento_volume. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Agrega os dados pela dimensão selecionada e soma o volume
    df_plot = df.groupby(dimension_col)['total_carteira_ativa_segmento'].sum().reset_index()
    df_plot = df_plot.sort_values(by='total_carteira_ativa_segmento', ascending=False) # Ordena por volume

    fig = px.bar(
        df_plot,
        x=dimension_col,
        y='total_carteira_ativa_segmento',
        title=title,
        labels={dimension_col: dimension_col.replace('_', ' ').title(), 'total_carteira_ativa_segmento': 'Volume (R$)'},
        color='total_carteira_ativa_segmento',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    fig.update_layout(xaxis_title=dimension_col.replace('_', ' ').title(), yaxis_title="Volume da Carteira Ativa (R$)")
    return fig

def plot_segmento_inadimplencia(df: pd.DataFrame, dimension_col: str, title: str) -> go.Figure:
    """
    Gera um gráfico de barras da taxa média de inadimplência por uma dimensão de segmento.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para plot_segmento_inadimplencia. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Agrega os dados pela dimensão selecionada e calcula a média da taxa de inadimplência
    df_plot = df.groupby(dimension_col)['taxa_inadimplencia_final_segmento'].mean().reset_index()
    df_plot = df_plot.sort_values(by='taxa_inadimplencia_final_segmento', ascending=False) # Ordena por taxa

    fig = px.bar(
        df_plot,
        x=dimension_col,
        y='taxa_inadimplencia_final_segmento',
        title=title,
        labels={dimension_col: dimension_col.replace('_', ' ').title(), 'taxa_inadimplencia_final_segmento': 'Taxa de Inadimplência Média (%)'},
        color='taxa_inadimplencia_final_segmento',
        color_continuous_scale=px.colors.sequential.Redor
    )
    fig.update_layout(xaxis_title=dimension_col.replace('_', ' ').title(), yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_tendencia_temporal(df_scr: pd.DataFrame, df_indicadores: pd.DataFrame, title: str = "Inadimplência vs. Indicadores Macroeconômicos") -> go.Figure:
    """
    Gera um gráfico de linhas mostrando a tendência da taxa de inadimplência
    e indicadores macroeconômicos ao longo do tempo.
    """
    if df_scr.empty or df_indicadores.empty:
        logger.warning(f"DataFrame(s) vazio(s) para plot_tendencia_temporal. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Agrega a inadimplência por mês do SCR
    # Usamos pd.Grouper para agrupar por mês, e depois convertemos para string para o eixo X
    df_scr_mensal = df_scr.groupby(pd.Grouper(key='data_base', freq='M'))['taxa_inadimplencia_final_segmento'].mean().reset_index()
    df_scr_mensal['data_base_str'] = df_scr_mensal['data_base'].dt.to_period('M').astype(str)

    # Agrega indicadores por mês
    df_indicadores_mensal = df_indicadores.groupby(pd.Grouper(key='data_base', freq='M'))[['taxa_desemprego', 'valor_ipca', 'taxa_selic_meta']].mean().reset_index()
    df_indicadores_mensal['data_base_str'] = df_indicadores_mensal['data_base'].dt.to_period('M').astype(str)

    # Junta os dois DataFrames pela coluna de data_base string
    df_plot = pd.merge(df_scr_mensal, df_indicadores_mensal, on='data_base_str', how='left')

    fig = go.Figure()

    # Adiciona a linha de inadimplência (eixo Y1)
    fig.add_trace(go.Scatter(x=df_plot['data_base_str'], y=df_plot['taxa_inadimplencia_final_segmento'],
                             mode='lines+markers', name='Taxa de Inadimplência Média', yaxis='y1', line=dict(color='red')))

    # Adiciona os indicadores (eixo Y2)
    fig.add_trace(go.Scatter(x=df_plot['data_base_str'], y=df_plot['taxa_desemprego'],
                             mode='lines+markers', name='Taxa de Desemprego', yaxis='y2', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=df_plot['data_base_str'], y=df_plot['valor_ipca'],
                             mode='lines+markers', name='Valor IPCA', yaxis='y2', line=dict(color='green')))
    fig.add_trace(go.Scatter(x=df_plot['data_base_str'], y=df_plot['taxa_selic_meta'],
                             mode='lines+markers', name='Taxa Selic Meta', yaxis='y2', line=dict(color='purple')))

    fig.update_layout(
        title=title,
        xaxis_title="Mês",
        yaxis=dict(title="Taxa de Inadimplência (%)", showgrid=False),
        yaxis2=dict(title="Valores Indicadores (%)", overlaying='y', side='right', showgrid=False),
        legend=dict(x=0, y=1.1, orientation="h")
    )
    return fig

def plot_inadimplencia_por_cluster(df_clusters: pd.DataFrame, title: str = "Taxa Média de Inadimplência por Cluster") -> go.Figure:
    """
    Gera um gráfico de barras da taxa média de inadimplência por cluster.
    Assume que df_clusters tem colunas 'cluster_id' e 'taxa_inadimplencia_final_segmento'.
    """
    if df_clusters.empty:
        logger.warning(f"DataFrame vazio para plot_inadimplencia_por_cluster. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados de Clusterização não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # CORRIGIDO AQUI: Usar 'cluster_id' em vez de 'cluster'
    df_plot = df_clusters.groupby('cluster_id')['taxa_inadimplencia_final_segmento'].mean().reset_index()
    df_plot['cluster_id'] = df_plot['cluster_id'].astype(str) # Garante que o cluster seja tratado como categoria para o eixo X
    df_plot = df_plot.sort_values(by='taxa_inadimplencia_final_segmento', ascending=False)

    fig = px.bar(
        df_plot,
        x='cluster_id', # CORRIGIDO AQUI
        y='taxa_inadimplencia_final_segmento',
        title=title,
        labels={'cluster_id': 'Cluster', 'taxa_inadimplencia_final_segmento': 'Taxa de Inadimplência Média (%)'}, # CORRIGIDO AQUI no label
        color='taxa_inadimplencia_final_segmento',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig.update_layout(xaxis_title="Cluster", yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_perfil_cluster(df_clusters: pd.DataFrame, cluster_id: int, features_perfil: list, title: str = "Perfil do Cluster") -> go.Figure:
    """
    Gera um gráfico de radar (spider chart) para mostrar o perfil de um cluster específico.
    Assume que df_clusters tem uma coluna 'cluster_id' e as 'features_perfil' numéricas.
    """
    # CORRIGIDO AQUI: Usar 'cluster_id'
    if df_clusters.empty or cluster_id not in df_clusters['cluster_id'].unique():
        logger.warning(f"Dados de cluster ou cluster_id '{cluster_id}' não encontrado para plot_perfil_cluster. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados de Perfil de Cluster não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # CORRIGIDO AQUI: Usar 'cluster_id'
    df_cluster_profile = df_clusters[df_clusters['cluster_id'] == cluster_id][features_perfil].mean().reset_index()
    df_cluster_profile.columns = ['Feature', 'Value']

    # Normaliza os valores para o gráfico de radar (opcional, mas bom para comparar escalas diferentes)
    for feature in features_perfil:
        min_val = df_clusters[feature].min()
        max_val = df_clusters[feature].max()
        if max_val - min_val > 0:
            df_cluster_profile.loc[df_cluster_profile['Feature'] == feature, 'Value_Normalized'] = \
                (df_cluster_profile.loc[df_cluster_profile['Feature'] == feature, 'Value'] - min_val) / (max_val - min_val)
        else:
            df_cluster_profile.loc[df_cluster_profile['Feature'] == feature, 'Value_Normalized'] = 0.5

    fig = go.Figure(data=go.Scatterpolar(
        r=df_cluster_profile['Value_Normalized'],
        theta=df_cluster_profile['Feature'],
        fill='toself',
        name=f'Cluster {cluster_id}'
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )),
        showlegend=False,
        title=f"{title} {cluster_id}"
    )
    return fig

def plot_top_combinacoes_risco(df: pd.DataFrame, top_n: int = 20, title: str = "Top Combinações de Risco por Taxa de Inadimplência") -> go.Figure:
    """
    Gera um gráfico de barras das top N combinações de risco (ex: Cliente x Modalidade x Porte)
    por taxa de inadimplência.
    Assume que o DataFrame já tem as colunas 'cliente', 'modalidade', 'porte' e 'taxa_inadimplencia_final_segmento'.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para plot_top_combinacoes_risco. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados de Top Combinações de Risco não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Cria a combinação de strings para o eixo X
    df['combinacao_risco'] = df['cliente'].astype(str) + ' - ' + \
                             df['modalidade'].astype(str) + ' - ' + \
                             df['porte'].astype(str)

    # Agrega por combinação e calcula a média da taxa de inadimplência
    df_plot = df.groupby('combinacao_risco')['taxa_inadimplencia_final_segmento'].mean().reset_index()
    df_plot = df_plot.sort_values(by='taxa_inadimplencia_final_segmento', ascending=False).head(top_n)

    fig = px.bar(
        df_plot,
        x='combinacao_risco',
        y='taxa_inadimplencia_final_segmento',
        title=title,
        labels={'combinacao_risco': 'Combinação de Risco', 'taxa_inadimplencia_final_segmento': 'Taxa de Inadimplência Média (%)'},
        color='taxa_inadimplencia_final_segmento',
        color_continuous_scale=px.colors.sequential.Hot
    )
    fig.update_layout(xaxis_title="Combinação de Risco", yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_comparativo_riscos(df: pd.DataFrame, comparison_dims: list, title: str = "Comparativo de Riscos") -> go.Figure:
    """
    Gera um gráfico de barras agrupadas para comparar a inadimplência entre diferentes dimensões.
    Recebe uma lista de dimensões para comparação (ex: ['uf', 'modalidade']).
    """
    if df.empty or not comparison_dims:
        logger.warning(f"DataFrame vazio ou dimensões de comparação não fornecidas para plot_comparativo_riscos. Retornando figura vazia para: {title}")
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados de Comparativo de Riscos não disponíveis", xref="paper", yref="paper", showarrow=False)])

    # Agrega pela(s) dimensão(ões) de comparação
    df_plot = df.groupby(comparison_dims)['taxa_inadimplencia_final_segmento'].mean().reset_index()

    # Cria a coluna de combinação para o eixo X se houver múltiplas dimensões
    if len(comparison_dims) > 1:
        df_plot['comparacao'] = df_plot[comparison_dims].apply(lambda row: ' - '.join(row.values.astype(str)), axis=1)
        x_axis_col = 'comparacao'
        x_axis_title = 'Combinação de Comparação'
    else:
        x_axis_col = comparison_dims[0]
        x_axis_title = comparison_dims[0].replace('_', ' ').title()

    fig = px.bar(
        df_plot,
        x=x_axis_col,
        y='taxa_inadimplencia_final_segmento',
        title=title,
        labels={x_axis_col: x_axis_title, 'taxa_inadimplencia_final_segmento': 'Taxa de Inadimplência Média (%)'},
        color='taxa_inadimplencia_final_segmento',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    fig.update_layout(xaxis_title=x_axis_title, yaxis_title="Taxa de Inadimplência Média (%)")
    return fig
