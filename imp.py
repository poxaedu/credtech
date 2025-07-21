from google.cloud import bigquery

# 1. Defina suas variáveis
client = bigquery.Client()
project_id = "credtech-1"
# ATENÇÃO: Escolha o dataset correto. Veja a nota abaixo sobre a região.
dataset_id = "dataclean" # Ou o nome do seu dataset
table_name = "dim_cluster_profiles"
file_path = "C:/Users/mendes/Desktop/transfer/dim_cluster_profiles.csv"

# 2. Configure o job de carregamento
table_id = f"{project_id}.{dataset_id}.{table_name}"
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Pula a linha do cabeçalho
    autodetect=True,      # Deixa o BigQuery adivinhar o esquema
)

# 3. Execute o job a partir do arquivo
try:
    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    print("Iniciando o job de carregamento...")
    load_job.result()  # Espera o job terminar
    print("Job finalizado.")

    destination_table = client.get_table(table_id)
    print(f"Arquivo carregado com sucesso. A tabela {table_name} agora tem {destination_table.num_rows} linhas.")

except Exception as e:
    print(f"Ocorreu um erro: {e}")