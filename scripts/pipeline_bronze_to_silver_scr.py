import pandas as pd
import os
import logging
from datetime import date, timedelta
import numpy as np

# --- Configuração de Logging ---
# Configura o logging para exibir no console e salvar em um arquivo
log_file_path = 'etl_pipeline.log' # Nome do arquivo de log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path), # Salva logs em um arquivo
        logging.StreamHandler() # Exibe logs no console
    ]
)
logging.info("Script ETL: Bronze to Silver (SCR.data) - Iniciado.")

# --- Configurações de Caminho Locais ---
# Obtém o diretório base do script para construir caminhos relativos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Caminhos para as pastas de dados locais
BRONZE_SCR_LOCAL_PATH = os.path.join(BASE_DIR, 'raw_data', 'scr')
SILVER_SCR_LOCAL_PATH = os.path.join(BASE_DIR, 'silver', 'scr')

# Garante que a pasta Silver exista
os.makedirs(SILVER_SCR_LOCAL_PATH, exist_ok=True)
logging.info(f"Pasta Silver (destino) verificada/criada em: {SILVER_SCR_LOCAL_PATH}")

# --- Definições de Colunas e Tipos para SCR.data (Mantenha as suas) ---
COLUNAS_SCR_ESSENCIAIS = [
    'data_base', 'uf', 'tcb', 'sr', 'cliente', 'ocupacao', 'cnae_secao',
    'cnae_subclasse', 'porte', 'modalidade', 'origem', 'indexador', 'numero_de_operacoes',
    'a_vencer_ate_90_dias', 'a_vencer_de_91_ate_360_dias', 'a_vencer_de_361_ate_1080_dias',
    'a_vencer_de_1081_ate_1800_dias', 'a_vencer_de_1801_ate_5400_dias', 'a_vencer_acima_de_5400_dias',
    'vencido_acima_de_15_dias', 'carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico'
]

dtypes_scr_iniciais = {
    'data_base': str, 'uf': 'category', 'tcb': 'category', 'sr': 'category', 'cliente': 'category',
    'ocupacao': 'category', 'cnae_secao': 'category', 'cnae_subclasse': 'category', 'porte': 'category',
    'modalidade': 'category', 'origem': 'category', 'indexador': 'category',
    'numero_de_operacoes': str,
    'a_vencer_ate_90_dias': str, 'a_vencer_de_91_ate_360_dias': str,
    'a_vencer_de_361_ate_1080_dias': str, 'a_vencer_de_1081_ate_1800_dias': str,
    'a_vencer_de_1801_ate_5400_dias': str, 'a_vencer_acima_de_5400_dias': str,
    'vencido_acima_de_15_dias': str, 'carteira_ativa': str,
    'carteira_inadimplida_arrastada': str, 'ativo_problematico': str,
}

colunas_scr_valor_para_limpar = [
    'a_vencer_ate_90_dias', 'a_vencer_de_91_ate_360_dias', 'a_vencer_de_361_ate_1080_dias',
    'a_vencer_de_1081_ate_1800_dias', 'a_vencer_de_1801_ate_5400_dias', 'a_vencer_acima_de_5400_dias',
    'vencido_acima_de_15_dias', 'carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico'
]

colunas_scr_categoricas_para_strip = [
    'uf', 'tcb', 'sr', 'cliente', 'ocupacao', 'cnae_secao',
    'cnae_subclasse', 'porte', 'modalidade', 'origem', 'indexador'
]

# --- Funções de Processamento ---
def processar_bronze_to_silver_local(caminho_arquivo_bruto_local: str, caminho_arquivo_silver_local: str, output_format: str = 'parquet') -> pd.DataFrame:
    """
    Processa um arquivo SCR.data da camada BRONZE (raw) para SILVER (limpo) localmente.
    Lê do diretório local, limpa e salva no diretório local (Silver).
    """
    filename = os.path.basename(caminho_arquivo_bruto_local)
    logging.info(f"Iniciando Bronze -> Silver para: {filename}")
    try:
        df = pd.read_csv(
            caminho_arquivo_bruto_local,
            usecols=COLUNAS_SCR_ESSENCIAIS,
            dtype=dtypes_scr_iniciais,
            sep=';',
            decimal=','
        )
        logging.info(f"Arquivo '{filename}' carregado. Linhas: {len(df)}")

        # --- Etapa de Limpeza e Transformação (Mantenha as suas lógicas) ---
        df['data_base'] = pd.to_datetime(df['data_base'], errors='coerce')
        for col in colunas_scr_valor_para_limpar:
            temp_series = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(temp_series, errors='coerce')
        df['numero_de_operacoes'] = pd.to_numeric(df['numero_de_operacoes'].astype(str).str.replace('<= 15', '15', regex=False), errors='coerce').fillna(0).astype('int32')

        for col in colunas_scr_categoricas_para_strip:
            df[col] = df[col].astype(str).str.strip().astype('category')

        # Criar Métricas de Risco no DataFrame
        df['taxa_inadimplencia_segmento'] = df.apply(
            lambda row: (row['vencido_acima_de_15_dias'] + row['carteira_inadimplida_arrastada']) / row['carteira_ativa']
            if row['carteira_ativa'] > 0 else 0, axis=1
        ).fillna(0)

        df['perc_ativo_problematico'] = df.apply(
            lambda row: row['ativo_problematico'] / row['carteira_ativa']
            if row['carteira_ativa'] > 0 else 0, axis=1
        ).fillna(0)
        # --- Fim das transformações ---

        # Salvar o DataFrame tratado
        if output_format == 'parquet':
            df.to_parquet(caminho_arquivo_silver_local, index=False)
            logging.info(f"Arquivo tratado salvo em Silver (Parquet): {os.path.basename(caminho_arquivo_silver_local)}")
        elif output_format == 'csv':
            # Se for CSV, garantir que o nome do arquivo termine em .csv
            if not caminho_arquivo_silver_local.endswith('.csv'):
                caminho_arquivo_silver_local = caminho_arquivo_silver_local.replace('.parquet', '.csv')
            df.to_csv(caminho_arquivo_silver_local, index=False, sep=';', decimal=',') # Use ; e , para CSV de volta
            logging.info(f"Arquivo tratado salvo em Silver (CSV): {os.path.basename(caminho_arquivo_silver_local)}")
        else:
            logging.error(f"Formato de saída '{output_format}' não suportado. Salve como 'parquet' ou 'csv'.")
            return None

        return df # Retorna o DataFrame (opcional)
    except Exception as e:
        logging.error(f"Erro ao processar Bronze -> Silver para {filename}: {e}", exc_info=True) # exc_info=True para detalhes do erro
        return None

# --- Lógica Principal de Automação ---
if __name__ == '__main__':
    logging.info("--- INICIANDO PROCESSAMENTO LOCAL: Camada SCR.data (Bronze -> Silver) ---")

    # Opção para definir o formato de saída ('parquet' ou 'csv')
    OUTPUT_FORMAT = 'parquet' # <<< Defina 'parquet' ou 'csv' aqui >>>

    # Listar todos os arquivos na pasta raw_data/scr
    csv_files = [f for f in os.listdir(BRONZE_SCR_LOCAL_PATH) if f.startswith('planilha_') and f.endswith('.csv')]
    
    if not csv_files:
        logging.warning(f"Nenhum arquivo CSV encontrado na pasta: {BRONZE_SCR_LOCAL_PATH}")
    else:
        logging.info(f"Encontrados {len(csv_files)} arquivos CSV para processar na pasta Bronze.")

        for filename_base in sorted(csv_files): # Ordena para processar em ordem cronológica se nomes forem padrão
            logging.info(f"Processando arquivo: {filename_base}")

            bronze_file_path = os.path.join(BRONZE_SCR_LOCAL_PATH, filename_base)
            
            # Define o nome do arquivo de saída na Silver com base no formato escolhido
            if OUTPUT_FORMAT == 'parquet':
                silver_file_name_treated = f"treated_{filename_base.replace('.csv', '.parquet')}"
            else: # CSV
                silver_file_name_treated = f"treated_{filename_base}" # Já é .csv
            
            silver_file_path = os.path.join(SILVER_SCR_LOCAL_PATH, silver_file_name_treated)

            processar_bronze_to_silver_local(bronze_file_path, silver_file_path, output_format=OUTPUT_FORMAT)

    logging.info("--- Processamento SCR.data (Bronze -> Silver) CONCLUÍDO ---")
    logging.info("--- Script ETL: Bronze to Silver (SCR.data) - FINALIZADO. ---")