import glob
import logging  # Importar logging para uma abordagem mais robusta
import os

import pandas as pd

# --- Configuração de Logging ---
# Define o caminho para o arquivo de log, colocando-o na raiz do projeto
# etl_gold_outros_consolidado.log para diferenciar dos outros pipelines Gold
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etl_gold_outros_consolidado.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path), # Salva logs em um arquivo
        logging.StreamHandler() # Exibe logs no console
    ]
)
logging.info("Script ETL: Silver to Gold (Consolidar Outros Dados) - Iniciado.")

# --- 1. CONFIGURAÇÃO DE CAMINHOS (Corrigido para a estrutura do seu projeto) ---
# Define o diretório base como o diretório PAI do diretório do script.
# Se o script está em /MeuProjeto/scripts/, BASE_DIR será /MeuProjeto/.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminhos de origem e destino, agora relativos ao BASE_DIR (raiz do projeto)
SILVER_DIR_INPUT = os.path.join(BASE_DIR, 'data', 'silver', 'outros') # Onde estão os CSVs tratados individualmente
GOLD_DIR_OUTPUT = os.path.join(BASE_DIR, 'data', 'gold', 'outros-tratado') # Onde o CSV consolidado será salvo

# Nome do arquivo de saída na camada Gold
OUTPUT_GOLD_FILE = 'indicadores_consolidados.csv' # Nome mais descritivo

# Garante que a pasta de destino (Gold) exista
os.makedirs(GOLD_DIR_OUTPUT, exist_ok=True)

logging.info("--- INICIANDO PIPELINE SILVER -> GOLD (Consolidar Outros Dados) ---")
logging.info(f"Pasta de origem (Silver): {SILVER_DIR_INPUT}")
logging.info(f"Pasta de destino (Gold): {GOLD_DIR_OUTPUT}")

# --- Adicionado para Depuração (Remova após confirmar que está funcionando) ---
logging.info(f"DEBUG: Caminho absoluto do script: {os.path.abspath(__file__)}")
logging.info(f"DEBUG: Diretório do script: {os.path.dirname(os.path.abspath(__file__))}")
logging.info(f"DEBUG: BASE_DIR (raiz do projeto): {BASE_DIR}")
logging.info(f"DEBUG: Caminho completo para SILVER (Origem): {SILVER_DIR_INPUT}")
logging.info(f"DEBUG: Caminho completo para GOLD (Destino): {GOLD_DIR_OUTPUT}")
logging.info("-" * 50)


try:
    # --- 2. LER E UNIFICAR DADOS DA CAMADA SILVER ---

    # Encontra todos os ficheiros .csv na pasta silver
    caminhos_silver = glob.glob(os.path.join(SILVER_DIR_INPUT, '*.csv'))
    if not caminhos_silver:
        logging.error(f"ERRO: Nenhum ficheiro .csv encontrado na pasta '{SILVER_DIR_INPUT}'. Verifique a origem dos dados.")
        # Se não há arquivos, não há o que fazer, então podemos sair
        exit()

    lista_dfs = []
    logging.info("\nLendo e processando ficheiros da camada Silver:")
    for caminho in caminhos_silver:
        nome_ficheiro = os.path.basename(caminho)
        logging.info(f"- {nome_ficheiro}")

        # Lê cada ficheiro, garantindo que a data seja o tipo correto
        df = pd.read_csv(caminho, sep=';', decimal=',', parse_dates=['data_referencia'])

        # Define a data como índice para facilitar a junção
        df.set_index('data_referencia', inplace=True)
        lista_dfs.append(df)

    # Junta todos os DataFrames da lista numa única tabela
    # O 'outer join' garante que todas as datas de todos os ficheiros sejam mantidas
    df_gold = pd.concat(lista_dfs, axis=1, join='outer')

    logging.info(f"\nSUCESSO! {len(caminhos_silver)} ficheiros unificados. Total de linhas: {len(df_gold)}")

    # --- 3. FILTRAR POR PERÍODO ---
    data_inicio = '2024-05-01'
    data_fim = '2025-05-31' # Mantendo a data de fim original, que é válida

    logging.info(f"Filtrando dados entre {data_inicio} e {data_fim}...")

    # Ordena o índice de datas (boa prática) e aplica o filtro
    df_gold.sort_index(inplace=True)
    df_gold_filtrado = df_gold.loc[data_inicio:data_fim]

    logging.info(f"Foram encontrados {len(df_gold_filtrado)} registos no período.")

    # --- 4. TRATAMENTO DE DADOS FALTANTES ---
    # Ao juntar séries temporais, é comum ter valores ausentes (NaN).
    # O método 'forward fill' (ffill) preenche os valores ausentes com o último valor válido.
    # Isso é útil para indicadores que só mudam em certas datas (ex: Selic).
    df_gold_filtrado.ffill(inplace=True)

    # Remove qualquer linha que ainda possa ter todos os valores nulos após o ffill
    # (aconteceria se as primeiras linhas do período não tivessem dados)
    df_gold_filtrado.dropna(how='all', inplace=True)

    logging.info("Valores ausentes foram preenchidos com o último dado válido.")

    # --- 5. SALVAR O RESULTADO FINAL ---
    # Restaura a coluna de data a partir do índice
    df_gold_filtrado.reset_index(inplace=True)

    caminho_gold_final = os.path.join(GOLD_DIR_OUTPUT, OUTPUT_GOLD_FILE) # Usa GOLD_DIR_OUTPUT

    # Salva o DataFrame final na pasta gold
    df_gold_filtrado.to_csv(
        caminho_gold_final,
        index=False,
        sep=';',
        decimal=',',
        date_format='%Y-%m-%d' # Formato de data padrão
    )

    logging.info(f"\nSUCESSO! Ficheiro consolidado salvo em: '{caminho_gold_final}'")

except Exception as e:
    logging.error(f"\nERRO inesperado durante o pipeline: {e}", exc_info=True) # exc_info=True para detalhes do erro

logging.info("\n--- PIPELINE SILVER -> GOLD CONCLUÍDO ---")