import logging
import os
from datetime import date, timedelta

import numpy as np  # Para np.log1p se for usado em novas métricas logaritmizadas
import pandas as pd

# from dateutil.relativedelta import relativedelta # Descomente se preferir usar relativedelta para datas


# --- Configuração de Logging ---
# Define o caminho para o arquivo de log, colocando-o na raiz do projeto
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etl_gold_pipeline.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path), # Salva logs em um arquivo
        logging.StreamHandler() # Exibe logs no console
    ]
)
logging.info("Script ETL: Silver to Gold (SCR.data) - Iniciado em ambiente LOCAL.")


# --- Configurações Locais de Pastas (Corrigido para a estrutura do seu projeto) ---
# Define o diretório base como o diretório PAI do diretório do script.
# Se o script está em /MeuProjeto/scripts/, BASE_DIR será /MeuProjeto/.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminhos das camadas no sistema de arquivos local
# SILVER_SCR_INPUT_PATH será: seu_projeto_raiz/data/silver/scr
SILVER_SCR_INPUT_PATH = os.path.join(BASE_DIR, 'data', 'silver', 'scr')

# GOLD_SCR_OUTPUT_PATH será: seu_projeto_raiz/data/gold/scr-tratado
GOLD_SCR_OUTPUT_PATH = os.path.join(BASE_DIR, 'data', 'gold', 'scr-tratado') # Use 'scr-tratado' conforme sua imagem

# Criar os diretórios Gold se não existirem
os.makedirs(GOLD_SCR_OUTPUT_PATH, exist_ok=True)
logging.info(f"Pasta Gold (destino) verificada/criada em: {GOLD_SCR_OUTPUT_PATH}")

# --- Adicionado para Depuração (Remova após confirmar que está funcionando) ---
logging.info(f"DEBUG: Caminho absoluto do script: {os.path.abspath(__file__)}")
logging.info(f"DEBUG: Diretório do script: {os.path.dirname(os.path.abspath(__file__))}")
logging.info(f"DEBUG: BASE_DIR (raiz do projeto): {BASE_DIR}")
logging.info(f"DEBUG: Caminho completo para SILVER (Origem): {SILVER_SCR_INPUT_PATH}")
logging.info(f"DEBUG: Caminho completo para GOLD (Destino): {GOLD_SCR_OUTPUT_PATH}")
logging.info("-" * 50)


# --- Definições de Colunas para Agregação ---
# As colunas que definem seus segmentos, devem ser as colunas categóricas da Silver
colunas_para_agregacao = [
    'data_base',
    'uf',
    # 'tcb', # Descomente se 'tcb' também é um segmento para você
    'cliente',
    'modalidade',
    'ocupacao',
    'cnae_secao',
    'cnae_subclasse',
    'porte',
    # 'sr', 'origem', 'indexador' # Descomente outras colunas se quiser agregá-las também
]

# --- Funções de Processamento ---

def processar_silver_to_gold(caminho_arquivo_silver_local: str, caminho_arquivo_gold_local: str) -> pd.DataFrame:
    """
    Processa dados da camada SILVER (limpos) para GOLD (agregados/final) para SCR.data.
    Lê de arquivo local (Silver), agrega e salva em arquivo local (Gold).
    """
    filename_silver = os.path.basename(caminho_arquivo_silver_local)
    logging.info(f"Iniciando Silver -> Gold para: {filename_silver}")
    try:
        # Verifica se o arquivo Silver de origem existe antes de tentar carregar
        if not os.path.exists(caminho_arquivo_silver_local):
            logging.warning(f"AVISO: Arquivo Silver não encontrado em '{caminho_arquivo_silver_local}'. Pulando este arquivo/mês.")
            return None # Retorna None e não tenta processar

        df = pd.read_parquet(caminho_arquivo_silver_local)
        logging.info(f"Arquivo '{filename_silver}' carregado. Linhas: {len(df)}")


        # Garante que data_base é apenas a data para agrupamento (para consistência após leitura de Parquet)
        df['data_base'] = pd.to_datetime(df['data_base']).dt.normalize()

        # --- Lógica de Agregação ---
        df_segmentos_agregados = df.groupby(colunas_para_agregacao, observed=True).agg(
            total_carteira_ativa_segmento=('carteira_ativa', 'sum'),
            total_vencido_15d_segmento=('vencido_acima_de_15_dias', 'sum'),
            total_inadimplida_arrastada_segmento=('carteira_inadimplida_arrastada', 'sum'),
            total_ativo_problematico_segmento=('ativo_problematico', 'sum'),
            # Usamos a média das taxas pré-calculadas ou podemos recalcular no nível agregado
            media_taxa_inadimplencia_original=('taxa_inadimplencia_segmento', 'mean'),
            media_perc_ativo_problematico_original=('perc_ativo_problematico', 'mean'),
            contagem_clientes_unicos_segmento=('cliente', 'nunique'), # Nova métrica
            contagem_subsegmentos=('data_base', 'count') # Contagem de linhas/registros no agrupamento
        ).reset_index()
        logging.info(f"Agregação concluída. Linhas agregadas: {len(df_segmentos_agregados)}")

        # --- Recálculo de KPIs Finais no nível Agregado (mais robusto) ---
        # Taxa de Inadimplência
        df_segmentos_agregados['taxa_inadimplencia_final_segmento'] = df_segmentos_agregados.apply(
            lambda row: (row['total_vencido_15d_segmento'] + row['total_inadimplida_arrastada_segmento']) / row['total_carteira_ativa_segmento']
            if row['total_carteira_ativa_segmento'] > 0 else 0, axis=1
        ).fillna(0)
        # Limita a taxa de inadimplência a um máximo de 1.0 (100%) e a um mínimo de 0
        df_segmentos_agregados['taxa_inadimplencia_final_segmento'] = df_segmentos_agregados['taxa_inadimplencia_final_segmento'].clip(lower=0.0, upper=1.0)

        # Percentual de Ativo Problemático
        df_segmentos_agregados['perc_ativo_problematico_final_segmento'] = df_segmentos_agregados.apply(
            lambda row: row['total_ativo_problematico_segmento'] / row['total_carteira_ativa_segmento']
            if row['total_carteira_ativa_segmento'] > 0 else 0, axis=1
        ).fillna(0)
        # Limita o percentual a um máximo de 1.0 (100%) e a um mínimo de 0
        df_segmentos_agregados['perc_ativo_problematico_final_segmento'] = df_segmentos_agregados['perc_ativo_problematico_final_segmento'].clip(lower=0.0, upper=1.0)

        # Salva o DataFrame agregado
        df_segmentos_agregados.to_parquet(caminho_arquivo_gold_local, index=False) # Salva em Gold local
        logging.info(f"Arquivo agregado salvo em Gold: {os.path.basename(caminho_arquivo_gold_local)}")
        return df_segmentos_agregados
    except FileNotFoundError:
        # Este bloco agora é mais para fallback, a verificação está acima
        logging.warning(f"Arquivo Silver não encontrado (no bloco try/except): {filename_silver}. Pulando este mês.")
        return None
    except Exception as e:
        logging.error(f"Erro ao processar Silver -> Gold para {filename_silver}: {e}", exc_info=True)
        return None

# --- Lógica Principal de Automação ---
if __name__ == '__main__':
    logging.info("--- INICIANDO PROCESSAMENTO LOCAL: Camada SCR.data (Silver -> Gold) ---")

    # Definindo o período de processamento (DEVE SER O MESMO DO BRONZE->SILVER)
    # Ajuste estas datas para corresponder aos seus arquivos reais
    start_date_process =  date(2024, 1, 1) # Primeiro mês de dados
    end_date_process = date(2025, 5, 1) # Ajuste a data final conforme seus arquivos (considerando 18 de julho de 2025, esta data final é plausível)

    current_date_loop = start_date_process

    # Loop para iterar sobre cada mês
    while current_date_loop <= end_date_process:
        year = current_date_loop.year
        month = current_date_loop.month
        month_str = f"{month:02d}"

        # Nome do arquivo de entrada na Silver (gerado pelo script anterior)
        silver_file_name_expected = f"treated_planilha_{year}{month_str}.parquet"
        silver_file_path = os.path.join(SILVER_SCR_INPUT_PATH, silver_file_name_expected)

        # Nome do arquivo de saída na Gold
        gold_file_name_output = f"aggr_segmentos_{year}{month_str}.parquet"

        # O caminho de destino na Gold deve incluir o ano para organização
        gold_output_year_dir = os.path.join(GOLD_SCR_OUTPUT_PATH, str(year))
        os.makedirs(gold_output_year_dir, exist_ok=True) # Garante que a pasta Gold/ano exista

        gold_file_path = os.path.join(gold_output_year_dir, gold_file_name_output)

        logging.info(f"Tentando processar dados para {year}-{month_str}: {silver_file_path}")

        # Processa o arquivo
        processar_silver_to_gold(silver_file_path, gold_file_path)

        # Avança para o próximo mês
        if month == 12:
            current_date_loop = date(year + 1, 1, 1)
        else:
            current_date_loop = date(year, month + 1, 1)

    logging.info("--- Processamento SCR.data (Silver -> Gold) CONCLUÍDO ---")
    logging.info("--- Script ETL: Silver to Gold (SCR.data) - FINALIZADO. ---")