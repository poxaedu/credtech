# CredTech Data Pipelines

## Visão Geral do Projeto

Este projeto implementa uma pipeline ETL (Extract, Transform, Load) para processar dados financeiros e econômicos, movendo-os por diferentes camadas (Bronze, Silver, Gold) e, finalmente, carregando-os em um banco de dados PostgreSQL. Ele foi desenhado para lidar com dados de Carteira de Crédito (`SCR.data`) e Indicadores Econômicos externos.

---

## Estrutura de Diretórios

A organização do projeto segue uma estrutura clara para gerenciar dados, scripts e ambientes:

```
.
├── components/                     # Módulos ou funções reutilizáveis (se houver)
├── data/                          # Armazena os dados processados em diferentes camadas
│   ├── gold/                      # Dados finalizados, agregados e prontos para consumo
│   │   ├── outros-tratado/        # Indicadores econômicos consolidados
│   │   └── scr-tratado/           # Dados SCR agregados por mês
│   ├── silver/                    # Dados limpos e padronizados
│   │   ├── outros/                # Indicadores econômicos individuais limpos
│   │   └── scr/                   # Dados SCR limpos
│   └── temp/                      # Arquivos intermediários (opcional)
├── raw_data/                      # Dados brutos extraídos da fonte
│   ├── outros/                    # Indicadores econômicos brutos
│   └── scr/                       # Dados brutos da Carteira de Crédito
├── scripts/                       # Scripts Python da pipeline ETL
│   ├── etl_pipeline.log
│   ├── etl_pipeline_outros.log
│   ├── etl_gold_pipeline.log
│   ├── etl_gold_outros_consolidado.log
│   ├── load_gold_aggr_to_db.log
│   ├── etl_indicadores_gold_to_db.log
│   ├── pipeline_bronze_to_silver_sgs.py
│   ├── pipeline_bronze_to_silver_outros.py
│   ├── pipeline_silver_to_gold_sgs.py
│   ├── pipeline_silver_to_gold_outros.py
│   ├── python_load_gold_aggr_to_db.py
│   ├── python_load_gold_outros_to_db.py
│   └── sgl_data_installer.py
├── .gitignore
├── app.py
├── Home.py
├── LICENSE
├── README.md
├── requirements.txt
└── venv/
```

---

## Pré-requisitos

Antes de iniciar a instalação, certifique-se de ter os seguintes softwares instalados em seu sistema:

- **Python 3.8+**: [https://www.python.org/downloads/](https://www.python.org/downloads/)
- **Git**: [https://git-scm.com/downloads](https://git-scm.com/downloads)
- **PostgreSQL**: [https://www.postgresql.org/download/](https://www.postgresql.org/download/)

> Durante a instalação do PostgreSQL:
>
> - Defina uma senha para o usuário `postgres`.
> - Crie o banco de dados `credtech` e o usuário `jjguilherme` com permissões adequadas.
> - Ou ajuste as credenciais diretamente nos scripts Python conforme necessário.

### Exemplo de comandos SQL

```sql
-- Conecte-se como usuário postgres: psql -U postgres
CREATE DATABASE credtech;
CREATE USER jjguilherme WITH PASSWORD 'admin';  -- ou outra senha
GRANT ALL PRIVILEGES ON DATABASE credtech TO jjguilherme;
```

---

## Instalação

### 1. Clonar o Repositório

```bash
git clone <URL_DO_SEU_REPOSITORIO>
cd <nome_da_pasta_do_seu_repositorio>  # Ex: cd CredTechDataPipelines
```

### 2. Configurar o Ambiente Virtual

```bash
python -m venv venv
```

### 3. Ativar o Ambiente Virtual

- Windows:

```bash
.env\Scriptsctivate
```

- macOS/Linux:

```bash
source venv/bin/activate
```

### 4. Instalar as Dependências

```bash
pip install -r requirements.txt
```

#### Exemplo de `requirements.txt`:

```text
pandas>=1.3.0
numpy>=1.20.0
sqlalchemy>=1.4.0
psycopg2-binary>=2.9.0
openpyxl
python-dateutil
```

---

## Configuração do Banco de Dados

Abra os scripts em `scripts/` e ajuste os parâmetros de conexão, se necessário:

```python
DB_USER = 'jjguilherme'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'
```

> **Dica:** Para produção, use variáveis de ambiente em vez de deixar senhas hardcoded.

---

## Como Executar a Pipeline ETL

Certifique-se de que o ambiente virtual esteja ativado.

### 1. Baixar Dados Brutos (Opcional)

```bash
python scripts/sgl_data_installer.py
```

### 2. Processar Dados: Bronze → Silver

#### Dados SCR:

```bash
python scripts/pipeline_bronze_to_silver_sgs.py
```

#### Indicadores Econômicos:

```bash
python scripts/pipeline_bronze_to_silver_outros.py
```

### 3. Processar Dados: Silver → Gold

#### Dados SCR:

```bash
python scripts/pipeline_silver_to_gold_sgs.py
```

#### Indicadores Econômicos:

```bash
python scripts/pipeline_silver_to_gold_outros.py
```

### 4. Carregar Dados: Gold → PostgreSQL

#### Dados SCR:

```bash
python scripts/python_load_gold_aggr_to_db.py
```

#### Indicadores Econômicos:

```bash
python scripts/python_load_gold_outros_to_db.py
```

---

## Logs

Todos os scripts geram arquivos `.log` para monitoramento e depuração. Verifique arquivos como:

- `etl_pipeline.log`
- `etl_gold_pipeline.log`
- `etl_pipeline_outros.log`
- `load_gold_aggr_to_db.log`
- etc.

---

## Contribuição

Sinta-se à vontade para contribuir! Faça um fork do repositório, crie uma branch com suas mudanças e envie um Pull Request.

---

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
