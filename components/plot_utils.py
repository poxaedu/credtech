# components/plot_utils.py

import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import datetime
from datetime import timedelta

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
        # COR ALTERADA: Usando 'Aggrnyl' (verde agressivo) invertido, ideal para risco.
        colorscale='Aggrnyl_r',
        colorbar_title='Inadimplência Média (%)',
        marker_line_color='white',
        marker_line_width=0.5,
        customdata=df_uf[['volume_carteira_total']],
        hovertemplate=(
            '<b>Estado: %{location}</b><br><br>' +
            'Inadimplência Média: %{z:.2%}<br>' +
            'Volume da Carteira: R$ %{customdata[0]:,.2f}' +
            '<extra></extra>'
        )
    ))

    fig.update_layout(
        title_text=None,
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
                 # COR ALTERADA: Usando 'algae' que já estava na lista.
                 color='volume_carteira_total', color_continuous_scale=px.colors.sequential.algae)
    fig.update_layout(xaxis_title="Unidade Federativa", yaxis_title="Volume da Carteira Ativa (R$)")
    return fig

def plot_segmento_volume(df_agregado: pd.DataFrame, dimension_col: str, title: str) -> go.Figure:
    if df_agregado.empty:
        return go.Figure().update_layout(title=None, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    df_plot = df_agregado.sort_values(by='volume_carteira_total', ascending=False)
    fig = px.bar(df_plot, x=dimension_col, y='volume_carteira_total', title=title,
                 labels={dimension_col: dimension_col.replace('_', ' ').title(), 'volume_carteira_total': 'Volume (R$)'},
                 # COR ALTERADA: Usando 'tealgrn' (verde azulado) para volume.
                 color='volume_carteira_total', color_continuous_scale=px.colors.sequential.Blugrn)
    fig.update_layout(xaxis_title=dimension_col.replace('_', ' ').title(), yaxis_title="Volume da Carteira Ativa (R$)")
    return fig

def plot_segmento_inadimplencia(df_agregado: pd.DataFrame, dimension_col: str, title: str) -> go.Figure:
    if df_agregado.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    df_plot = df_agregado.sort_values(by='taxa_inadimplencia_media', ascending=False)
    fig = px.bar(df_plot, x=dimension_col, y='taxa_inadimplencia_media', title=title,
                 labels={dimension_col: dimension_col.replace('_', ' ').title(), 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 # COR ALTERADA: Usando 'emrld' (esmeralda) para inadimplência/risco.
                 color='taxa_inadimplencia_media', color_continuous_scale=px.colors.sequential.algae)
    fig.update_layout(xaxis_title=dimension_col.replace('_', ' ').title(), yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def calculate_metrics_for_period(df: pd.DataFrame, start_date: datetime, end_date: datetime, main_metric_col: str):
    """
    Calcula o valor médio e a mudança percentual para a métrica principal em um dado período.
    Args:
        df: DataFrame contendo os dados temporais.
        start_date: Data de início do período.
        end_date: Data de fim do período.
        main_metric_col: Nome da coluna que contém a métrica principal a ser analisada.
    Returns:
        Tupla (valor_medio, mudanca_percentual).
    """
    df_filtered = df[(df['mes'] >= start_date) & (df['mes'] <= end_date)].copy()
    if df_filtered.empty:
        return 0, 0

    avg_value = df_filtered[main_metric_col].mean()
    
    # Para mudança percentual, precisamos de pelo menos dois pontos no tempo
    if len(df_filtered) >= 2:
        # Ordena por data para garantir que o primeiro e último são corretos
        df_filtered = df_filtered.sort_values(by='mes')
        first_value = df_filtered[main_metric_col].iloc[0]
        last_value = df_filtered[main_metric_col].iloc[-1]
        
        if first_value != 0:
            percent_change = ((last_value - first_value) / first_value) * 100
        else:
            percent_change = 0
    else:
        percent_change = 0
    
    return avg_value, percent_change


def plot_single_temporal_series(df_plot: pd.DataFrame, selected_y_col: str, y_axis_name: str, line_color: str) -> go.Figure:
    """
    Cria um gráfico de linha com área preenchida para uma única série selecionada,
    seguindo o modelo da imagem.
    Args:
        df_plot: DataFrame com os dados a serem plotados.
        selected_y_col: Nome da coluna do DataFrame para o eixo Y (a série selecionada).
        y_axis_name: Nome de exibição da série para a legenda e métricas.
        line_color: Cor da linha e da área preenchida do gráfico.
    Returns:
        Um objeto go.Figure.
    """
    if df_plot.empty or selected_y_col not in df_plot.columns:
        logger.warning(f"DataFrame vazio ou coluna '{selected_y_col}' ausente. Retornando figura vazia.")
        return go.Figure().update_layout(title="", annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    fig = go.Figure()

    # Trace principal com área preenchida
    fig.add_trace(go.Scatter(
        x=df_plot['mes'], # Use 'mes' (tipo datetime) para Plotly lidar com o eixo X corretamente
        y=df_plot[selected_y_col],
        mode='lines',
        fill='tozeroy', # Preenche a área abaixo da linha
        line=dict(color=line_color, width=2),
        # Calcula a cor da área preenchida com transparência usando a cor da linha
        fillcolor=f'rgba({",".join(map(str, go.Scatter().line.color_to_RGB(line_color)))}, 0.2)',
        name=y_axis_name # Nome da legenda
    ))

    # Ponto de destaque (similar ao da imagem, pegando o último ponto como exemplo)
    if not df_plot.empty:
        df_sorted = df_plot.sort_values(by='mes')
        last_date = df_sorted['mes'].iloc[-1]
        last_value = df_sorted[selected_y_col].iloc[-1]

        fig.add_trace(go.Scatter(
            x=[last_date],
            y=[last_value],
            mode='markers+text',
            marker=dict(size=10, color='black', symbol='circle'),
            text=[f"{last_value:,.2f}"], # Formato do texto no ponto
            textposition="top center",
            textfont=dict(color="white", size=12),
            hoverinfo='none', # Não mostrar tooltip extra para este marcador
            showlegend=False
        ))
        # Adicionar a linha pontilhada vertical
        fig.add_shape(type="line",
                      x0=last_date, y0=0, x1=last_date, y1=last_value,
                      line=dict(color="black", width=1, dash="dot"),
                      yref="y", xref="x")

    fig.update_layout(
        title='', # O título principal já está no markdown
        xaxis_title="", # A imagem não tem título no eixo X
        yaxis_title="", # A imagem não tem título no eixo Y
        xaxis=dict(
            showgrid=False, # Remover linhas de grade verticais
            tickformat="%d %b", # Formato da data no eixo X (ex: 17 Apr)
            # Garante que o range do eixo X seja da data mínima à máxima
            range=[df_plot['mes'].min(), df_plot['mes'].max() + timedelta(days=1)] # Adiciona um dia para garantir que a última data apareça
        ),
        yaxis=dict(
            showgrid=True, # Manter linhas de grade horizontais
            gridcolor='#e0e0e0', # Cor das linhas de grade
            rangemode='tozero', # Garante que o eixo Y comece em zero
            tickformat=".0f" # Formato para números inteiros, ajuste se precisar de decimais
        ),
        showlegend=False, # A legenda será substituída pelas pills
        margin=dict(l=20, r=20, t=20, b=20), # Margens menores para o gráfico
        height=300, # Altura do gráfico
        plot_bgcolor='rgba(0,0,0,0)', # Fundo transparente do plot
        paper_bgcolor='rgba(0,0,0,0)', # Fundo transparente do papel do gráfico
        hovermode="x unified" # Exibe informações de hover para todos os traces em uma determinada data
    )
    return fig

def plot_inadimplencia_por_cluster(df_agregado_cluster: pd.DataFrame, title: str = "") -> go.Figure:
    """
    Cria um gráfico de rosca (donut chart) para mostrar a distribuição da inadimplência por cluster.
    O título é opcional e por padrão não é exibido.
    """
    if df_agregado_cluster.empty:
        return go.Figure().update_layout(title_text=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])
    
    df_plot = df_agregado_cluster.copy()
    df_plot['cluster_id'] = 'Cluster ' + df_plot['cluster_id'].astype(str)
    
    fig = px.pie(
        df_plot, 
        names='cluster_id', 
        values='taxa_inadimplencia_media', 
        title=title, # Passa o título (que por padrão é vazio)
        hole=0.2,
        color_discrete_sequence=px.colors.sequential.Greens_r
    )
    
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Inadimplência Média: %{value:.2%}<extra></extra>',
        insidetextfont=dict(size=16, color='white')
    )
    # Ajustes finais de layout para remover o espaço do título e posicionar a legenda
    fig.update_layout(
        height=500,
        legend_title_text='Clusters',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="left",
            x=0
        ),
        # Remove completamente o título e seu espaço
        title_text=None, 
        margin=dict(t=20, b=60, l=20, r=20) # Reduz a margem superior
    )
    
    return fig

def plot_perfil_cluster(df_full_clusters: pd.DataFrame, cluster_id: int, features_perfil: list, title: str = "Perfil do Cluster") -> go.Figure:
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
        fill='toself', name=f'Cluster {cluster_id}',
        # COR ALTERADA: Definindo cor da linha e do preenchimento para verde.
        line=dict(color='#006d2c'),
        fillcolor='rgba(44, 162, 95, 0.6)'
    ))
    fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), showlegend=False, title=f"{title} {cluster_id}")
    return fig

def plot_top_combinacoes_risco(df_agregado_top_combinacoes: pd.DataFrame, title: str = "") -> go.Figure:
    """
    Gera um gráfico de barras VERTICAL com as combinações de risco e suas taxas de inadimplência.
    """
    if df_agregado_top_combinacoes.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    # Invertemos os eixos: x se torna a categoria e y o valor numérico.
    fig = px.bar(df_agregado_top_combinacoes,
                 x='combinacao_risco',
                 y='taxa_inadimplencia_media',
                 title=title,
                 labels={'combinacao_risco': 'Combinação de Risco', 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 color='taxa_inadimplencia_media',
                 color_continuous_scale='Greens')

    # Ajustamos o layout para o novo formato vertical:
    # - A ordenação agora é no eixo x. Usei 'total descending' para mostrar a maior barra primeiro.
    # - Trocamos os títulos dos eixos (xaxis_title e yaxis_title).
    # - A orientação 'v' (vertical) é o padrão, então não é necessário definir 'orientation'.
    fig.update_layout(xaxis={'categoryorder':'total descending'},
                      xaxis_title="Combinação de Risco",
                      yaxis_title="Taxa de Inadimplência Média (%)",
                      )
    return fig

def plot_comparativo_riscos(df_agregado: pd.DataFrame, comparison_dims: list, title: str) -> go.Figure:
    if df_agregado.empty:
        return go.Figure().update_layout(title=None, annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    df_plot = df_agregado.copy()
    if len(comparison_dims) > 1:
        df_plot['comparacao'] = df_plot[comparison_dims].apply(lambda row: ' - '.join(row.values.astype(str)), axis=1)
        x_axis_col = 'comparacao'
        x_axis_title = 'Combinação de Comparação'
    else:
        x_axis_col = comparison_dims[0]
        x_axis_title = comparison_dims[0].replace('_', ' ').title()

    df_plot = df_plot.sort_values(by='taxa_inadimplencia_media', ascending=False).head(25)

    fig = px.bar(df_plot, x=x_axis_col, y='taxa_inadimplencia_media', title=title,
                 labels={x_axis_col: x_axis_title, 'taxa_inadimplencia_media': 'Taxa de Inadimplência Média (%)'},
                 # COR ALTERADA: Usando 'Greens' para um gradiente de verde.
                 color='taxa_inadimplencia_media', color_continuous_scale='Greens')
    fig.update_layout(title="", xaxis_title=x_axis_title, yaxis_title="Taxa de Inadimplência Média (%)")
    return fig

def plot_top_segmento_horizontal(df_top_n: pd.DataFrame, dimension_col: str, metric_col: str, title: str) -> go.Figure:
    """
    Gera um gráfico de barras HORIZONTAL para exibir os Top N segmentos.
    """
    if df_top_n.empty:
        return go.Figure().update_layout(title=title, annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    df_plot = df_top_n.sort_values(by=metric_col, ascending=True)

    fig = px.bar(
        df_plot,
        y=dimension_col,
        x=metric_col,
        orientation='h',
        title=None,
        labels={
            dimension_col: dimension_col.replace('_', ' ').title(),
            metric_col: metric_col.replace('_', ' ').title()
        },
        color=metric_col,
        # COR ALTERADA: Lógica para usar 'emrld' para risco e 'tealgrn' para volume.
        color_continuous_scale='emrld' if 'inadimplencia' in metric_col else 'tealgrn'
    )
    fig.update_layout(
        xaxis_title=metric_col.replace('_', ' ').title(),
        yaxis_title=dimension_col.replace('_', ' ').title()
    )
    return fig

def plot_matriz_correlacao(df_temporal):
    if df_temporal.empty: return go.Figure().update_layout(title="Dados não disponíveis")
    colunas_numericas = ['taxa_inadimplencia_media', 'taxa_desemprego', 'valor_ipca', 'taxa_selic_meta']
    df_corr = df_temporal[colunas_numericas].dropna()
    if len(df_corr) < 3: return go.Figure().update_layout(title="Dados insuficientes")
    matriz_corr = df_corr.corr()
    fig = go.Figure(data=go.Heatmap(
        z=matriz_corr.values, x=['Inadimplência', 'Desemprego', 'IPCA', 'Selic'],
        y=['Inadimplência', 'Desemprego', 'IPCA', 'Selic'], colorscale='greens', zmid=0,
        text=np.round(matriz_corr.values, 3), texttemplate="%{text}", textfont={"size": 12}
    ))
    fig.update_layout(title='', height=400)
    return fig

def plot_scatter_correlacao(df_temporal, indicador, nome_indicador):
    if df_temporal.empty or indicador not in df_temporal.columns:
        return go.Figure().update_layout(title="Dados não disponíveis")
    df_clean = df_temporal.dropna(subset=['taxa_inadimplencia_media', indicador])
    if len(df_clean) < 3: return go.Figure().update_layout(title="Dados insuficientes")
    z = np.polyfit(df_clean[indicador], df_clean['taxa_inadimplencia_media'], 1)
    p = np.poly1d(z)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_clean[indicador], y=df_clean['taxa_inadimplencia_media'], mode='markers',
        name='Dados Observados', marker=dict(size=8, color='#2ca25f'),
        hovertemplate=f'<b>{nome_indicador}</b>: %{{x}}<br><b>Inadimplência</b>: %{{y:.2%}}<extra></extra>'
    ))
    fig.add_trace(go.Scatter(
        x=df_clean[indicador], y=p(df_clean[indicador]), mode='lines',
        name='Linha de Tendência', line=dict(color='#006d2c', width=2)
    ))
    fig.update_layout(title='',
                      xaxis_title=nome_indicador, yaxis_title="Taxa de Inadimplência (%)", height=400)
    return fig

# --- Nova função auxiliar para converter cor hex para RGB ---
def hex_to_rgb(hex_color):
    """Converts a hex color string (e.g., '#RRGGBB') to an RGB tuple."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

def plot_single_temporal_series(df_plot: pd.DataFrame, selected_y_col: str, y_axis_name: str, line_color: str) -> go.Figure:
    """
    Cria um gráfico de linha com área preenchida para uma única série selecionada,
    seguindo o modelo da imagem.
    Args:
        df_plot: DataFrame com os dados a serem plotados.
        selected_y_col: Nome da coluna do DataFrame para o eixo Y (a série selecionada).
        y_axis_name: Nome de exibição da série para a legenda e métricas.
        line_color: Cor da linha e da área preenchida do gráfico.
    Returns:
        Um objeto go.Figure.
    """
    if df_plot.empty or selected_y_col not in df_plot.columns:
        logger.warning(f"DataFrame vazio ou coluna '{selected_y_col}' ausente. Retornando figura vazia.")
        return go.Figure().update_layout(title="", annotations=[dict(text="Dados não disponíveis", showarrow=False)])

    fig = go.Figure()

    # --- CORREÇÃO AQUI: Convertendo line_color para RGBA para fillcolor ---
    try:
        r, g, b = hex_to_rgb(line_color)
        fill_rgba_color = f'rgba({r},{g},{b}, 0.2)' # 0.2 é a transparência
    except Exception as e:
        logger.warning(f"Não foi possível converter a cor '{line_color}' para RGBA. Usando cor padrão. Erro: {e}")
        fill_rgba_color = 'rgba(0,128,0,0.2)' # Cor verde padrão com transparência em caso de erro


    # Trace principal com área preenchida
    fig.add_trace(go.Scatter(
        x=df_plot['mes'],
        y=df_plot[selected_y_col],
        mode='lines',
        fill='tozeroy', # Preenche a área abaixo da linha
        line=dict(color=line_color, width=2),
        fillcolor=fill_rgba_color, # Use a cor RGBA derivada
        name=y_axis_name # Nome da legenda
    ))

    # Ponto de destaque (similar ao da imagem, pegando o último ponto como exemplo)
    if not df_plot.empty:
        df_sorted = df_plot.sort_values(by='mes')
        last_date = df_sorted['mes'].iloc[-1]
        last_value = df_sorted[selected_y_col].iloc[-1]

        fig.add_trace(go.Scatter(
            x=[last_date],
            y=[last_value],
            mode='markers+text',
            marker=dict(size=10, color='black', symbol='circle'),
            text=[f"{last_value:,.2f}"], # Formato do texto no ponto
            textposition="top center",
            textfont=dict(color="white", size=12),
            hoverinfo='none', # Não mostrar tooltip extra para este marcador
            showlegend=False
        ))
        # Adicionar a linha pontilhada vertical
        fig.add_shape(type="line",
                      x0=last_date, y0=0, x1=last_date, y1=last_value,
                      line=dict(color="black", width=1, dash="dot"),
                      yref="y", xref="x")

    fig.update_layout(
        title='', # O título principal já está no markdown
        xaxis_title="", # A imagem não tem título no eixo X
        yaxis_title="", # A imagem não tem título no eixo Y
        xaxis=dict(
            showgrid=False, # Remover linhas de grade verticais
            tickformat="%d %b", # Formato da data no eixo X (ex: 17 Apr)
            # Garante que o range do eixo X seja da data mínima à máxima
            range=[df_plot['mes'].min(), df_plot['mes'].max() + timedelta(days=1)] # Adiciona um dia para garantir que a última data apareça
        ),
        yaxis=dict(
            showgrid=True, # Manter linhas de grade horizontais
            gridcolor='#e0e0e0', # Cor das linhas de grade
            rangemode='tozero', # Garante que o eixo Y comece em zero
            tickformat=".0f" # Formato para números inteiros, ajuste se precisar de decimais
        ),
        showlegend=False, # A legenda será substituída pelas pills
        margin=dict(l=20, r=20, t=20, b=20), # Margens menores para o gráfico
        height=300, # Altura do gráfico
        plot_bgcolor='rgba(0,0,0,0)', # Fundo transparente do plot
        paper_bgcolor='rgba(0,0,0,0)', # Fundo transparente do papel do gráfico
        hovermode="x unified" # Exibe informações de hover para todos os traces em uma determinada data
    )
    return fig