# Script para processar e unificar ficheiros da camada Silver para a Gold,
# consolidando os dados e aplicando um filtro de data.

import pandas as pd
import os
import glob

# --- 1. CONFIGURAÇÃO DE CAMINHOS ---
SILVER_DIR = 'silver'
GOLD_DIR = 'gold'
OUTPUT_GOLD_FILE = 'indicadores.csv'

# Garante que a pasta de destino (Gold) exista
os.makedirs(GOLD_DIR, exist_ok=True)

print("--- INICIANDO PIPELINE SILVER -> GOLD ---")
print(f"Pasta de origem (Silver): {SILVER_DIR}")
print(f"Pasta de destino (Gold): {GOLD_DIR}")

try:
    # --- 2. LER E UNIFICAR DADOS DA CAMADA SILVER ---
    
    # Encontra todos os ficheiros .csv na pasta silver
    caminhos_silver = glob.glob(os.path.join(SILVER_DIR, '*.csv'))
    if not caminhos_silver:
        print(f"ERRO: Nenhum ficheiro .csv encontrado na pasta '{SILVER_DIR}'.")
        exit()

    lista_dfs = []
    print("\nLendo e processando ficheiros da camada Silver:")
    for caminho in caminhos_silver:
        nome_ficheiro = os.path.basename(caminho)
        print(f"- {nome_ficheiro}")
        
        # Lê cada ficheiro, garantindo que a data seja o tipo correto
        df = pd.read_csv(caminho, sep=';', decimal=',', parse_dates=['data_referencia'])
        
        # Define a data como índice para facilitar a junção
        df.set_index('data_referencia', inplace=True)
        lista_dfs.append(df)

    # Junta todos os DataFrames da lista numa única tabela
    # O 'outer join' garante que todas as datas de todos os ficheiros sejam mantidas
    df_gold = pd.concat(lista_dfs, axis=1, join='outer')
    
    print(f"\nSUCESSO! {len(caminhos_silver)} ficheiros unificados.")

    # --- 3. FILTRAR POR PERÍODO ---
    data_inicio = '2024-05-01'
    data_fim = '2025-05-31'
    
    print(f"Filtrando dados entre {data_inicio} e {data_fim}...")
    
    # Ordena o índice de datas (boa prática) e aplica o filtro
    df_gold.sort_index(inplace=True)
    df_gold_filtrado = df_gold.loc[data_inicio:data_fim]
    
    print(f"Foram encontrados {len(df_gold_filtrado)} registos no período.")

    # --- 4. TRATAMENTO DE DADOS FALTANTES ---
    # Ao juntar séries temporais, é comum ter valores ausentes (NaN).
    # O método 'forward fill' (ffill) preenche os valores ausentes com o último valor válido.
    # Isso é útil para indicadores que só mudam em certas datas (ex: Selic).
    df_gold_filtrado.ffill(inplace=True)
    
    # Remove qualquer linha que ainda possa ter todos os valores nulos após o ffill
    # (aconteceria se as primeiras linhas do período não tivessem dados)
    df_gold_filtrado.dropna(how='all', inplace=True)
    
    print("Valores ausentes foram preenchidos com o último dado válido.")

    # --- 5. SALVAR O RESULTADO FINAL ---
    # Restaura a coluna de data a partir do índice
    df_gold_filtrado.reset_index(inplace=True)
    
    caminho_gold = os.path.join(GOLD_DIR, OUTPUT_GOLD_FILE)
    
    # Salva o DataFrame final na pasta gold
    df_gold_filtrado.to_csv(
        caminho_gold,
        index=False,
        sep=';',
        decimal=',',
        date_format='%Y-%m-%d' # Formato de data padrão
    )
    
    print(f"\nSUCESSO! Ficheiro consolidado salvo em: '{caminho_gold}'")

except Exception as e:
    print(f"\nERRO inesperado durante o pipeline: {e}")

print("\n--- PIPELINE SILVER -> GOLD CONCLUÍDO ---")