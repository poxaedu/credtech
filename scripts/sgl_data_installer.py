import pandas as pd
from bcb import sgs
from datetime import datetime
import os
from tqdm import tqdm

# --- 1. CONFIGURAÇÃO ---
print("Iniciando a extração de séries individuais do BACEN SGS...")

# Dicionário com os códigos e nomes amigáveis para cada série
codigos_sgs = {
    'taxa_selic_meta': 432,
    'ipca_inflacao': 13522,
    'inadimplencia_pf': 21082,
    'endividamento_familias': 19882,
    'taxa_desemprego': 24369
}

# Pasta onde os arquivos individuais serão salvos
OUTPUT_DIR = "raw_data/outros"

# Período de extração (últimos 10 anos a partir da data atual)
data_final = datetime.now()
data_inicial = data_final - pd.DateOffset(years=2)

# --- 2. SETUP DO DIRETÓRIO ---
# Cria o diretório de saída se ele não existir
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Diretório '{OUTPUT_DIR}' criado com sucesso.")

# --- 3. LOOP DE EXTRAÇÃO INDIVIDUAL ---
print(f"Baixando séries de {data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}...")

# Itera sobre cada item do dicionário com uma barra de progresso
for nome_amigavel, codigo in tqdm(codigos_sgs.items(), desc="Baixando Séries"):
    try:
        # Puxa UMA única série por vez da API do BACEN
        df_serie = sgs.get(codigo, start=data_inicial, end=data_final)

        # Verifica se o DataFrame retornado não está vazio
        if not df_serie.empty:
            # Monta o nome do arquivo de forma descritiva
            nome_arquivo = f"sgs_{codigo}_{nome_amigavel}.csv"
            caminho_completo = os.path.join(OUTPUT_DIR, nome_arquivo)

            # Salva o arquivo CSV original, sem tratamento (sem .ffill)
            df_serie.to_csv(caminho_completo, sep=';', decimal=',', encoding='utf-8-sig')
        else:
            tqdm.write(f"Aviso: A série '{nome_amigavel}' (código {codigo}) não retornou dados.")

    except Exception as e:
        tqdm.write(f"Erro ao buscar a série '{nome_amigavel}' (código {codigo}): {e}")

print(f"\n--- SUCESSO! ---")
print(f"Processo concluído. Os arquivos CSV individuais foram salvos na pasta '{OUTPUT_DIR}'.")