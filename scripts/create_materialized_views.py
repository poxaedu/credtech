import logging
import psycopg2
import urllib.parse
from datetime import datetime

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('create_materialized_views.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configurações de Conexão com PostgreSQL ---
DB_USER = 'rogerym'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'credtech'

def get_db_connection():
    """Cria conexão com o PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logger.info("Conexão com PostgreSQL estabelecida com sucesso.")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
        raise

def drop_materialized_views(cursor):
    """Remove views materializadas existentes"""
    views_to_drop = [
        'mv_scr_agregado_uf',
        'mv_scr_tendencia_mensal', 
        'mv_scr_agregado_segmentos',
        'mv_scr_top_combinacoes_risco',
        'mv_indicadores_economicos_resumo'
    ]
    
    for view in views_to_drop:
        try:
            cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE;")
            logger.info(f"View materializada '{view}' removida (se existia).")
        except Exception as e:
            logger.warning(f"Erro ao remover view '{view}': {e}")

def create_materialized_views(cursor):
    """Cria todas as views materializadas otimizadas"""
    
    # 1. View agregada por UF
    logger.info("Criando view materializada: mv_scr_agregado_uf")
    cursor.execute("""
        CREATE MATERIALIZED VIEW mv_scr_agregado_uf AS
        SELECT 
            uf,
            data_base,
            AVG(taxa_inadimplencia_final_segmento) as taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) as total_carteira_ativa,
            SUM(total_vencido_15d_segmento + total_inadimplida_arrastada_segmento) as total_inadimplente,
            COUNT(*) as total_segmentos,
            AVG(perc_ativo_problematico_final_segmento) as perc_ativo_problematico_media
        FROM ft_scr_agregado_mensal
        GROUP BY uf, data_base
        ORDER BY uf, data_base;
    """)
    
    # 2. View para tendência temporal mensal
    logger.info("Criando view materializada: mv_scr_tendencia_mensal")
    cursor.execute("""
        CREATE MATERIALIZED VIEW mv_scr_tendencia_mensal AS
        SELECT 
            DATE_TRUNC('month', data_base) as mes,
            AVG(taxa_inadimplencia_final_segmento) as taxa_inadimplencia_media,
            SUM(total_carteira_ativa_segmento) as total_carteira_ativa,
            SUM(total_vencido_15d_segmento + total_inadimplida_arrastada_segmento) as total_inadimplente,
            COUNT(*) as total_registros,
            AVG(perc_ativo_problematico_final_segmento) as perc_ativo_problematico_media
        FROM ft_scr_agregado_mensal
        GROUP BY DATE_TRUNC('month', data_base)
        ORDER BY mes;
    """)
    
    # 3. View agregada por segmentos (limitada aos top performers)
    logger.info("Criando view materializada: mv_scr_agregado_segmentos")
    cursor.execute("""
        CREATE MATERIALIZED VIEW mv_scr_agregado_segmentos AS
        WITH segmentos_ranked AS (
            SELECT 
                cliente, modalidade, ocupacao, porte, cnae_secao, uf,
                AVG(taxa_inadimplencia_final_segmento) as taxa_inadimplencia_media,
                SUM(total_carteira_ativa_segmento) as total_carteira_ativa,
                COUNT(*) as total_registros,
                AVG(perc_ativo_problematico_final_segmento) as perc_ativo_problematico_media,
                ROW_NUMBER() OVER (ORDER BY SUM(total_carteira_ativa_segmento) DESC) as rank_volume
            FROM ft_scr_agregado_mensal
            GROUP BY cliente, modalidade, ocupacao, porte, cnae_secao, uf
        )
        SELECT *
        FROM segmentos_ranked
        WHERE rank_volume <= 1000  -- Top 1000 segmentos por volume
        ORDER BY total_carteira_ativa DESC;
    """)
    
    # 4. View para top combinações de risco
    logger.info("Criando view materializada: mv_scr_top_combinacoes_risco")
    cursor.execute("""
        CREATE MATERIALIZED VIEW mv_scr_top_combinacoes_risco AS
        WITH combinacoes_risco AS (
            SELECT 
                CONCAT(cliente, ' - ', modalidade, ' - ', porte) as combinacao_risco,
                cliente,
                modalidade, 
                porte,
                AVG(taxa_inadimplencia_final_segmento) as taxa_inadimplencia_media,
                SUM(total_carteira_ativa_segmento) as total_carteira_ativa,
                COUNT(*) as total_registros
            FROM ft_scr_agregado_mensal
            GROUP BY cliente, modalidade, porte
            HAVING SUM(total_carteira_ativa_segmento) > 1000000  -- Filtro mínimo de volume
        )
        SELECT *
        FROM combinacoes_risco
        ORDER BY taxa_inadimplencia_media DESC
        LIMIT 50;  -- Top 50 combinações de maior risco
    """)
    
    # 5. View resumo dos indicadores econômicos
    logger.info("Criando view materializada: mv_indicadores_economicos_resumo")
    cursor.execute("""
        CREATE MATERIALIZED VIEW mv_indicadores_economicos_resumo AS
        SELECT 
            DATE_TRUNC('month', data_base) as mes,
            AVG(taxa_desemprego) as taxa_desemprego_media,
            AVG(taxa_inadimplencia_pf) as taxa_inadimplencia_pf_media,
            AVG(valor_ipca) as valor_ipca_medio,
            AVG(taxa_selic_meta) as taxa_selic_meta_media
        FROM ft_indicadores_economicos_mensal
        GROUP BY DATE_TRUNC('month', data_base)
        ORDER BY mes;
    """)
    
    logger.info("Todas as views materializadas foram criadas com sucesso.")

def create_indexes(cursor):
    """Cria índices para otimizar as consultas"""
    logger.info("Criando índices de performance...")
    
    indexes = [
        # Índices para tabelas principais
        "CREATE INDEX IF NOT EXISTS idx_ft_scr_data_base ON ft_scr_agregado_mensal(data_base);",
        "CREATE INDEX IF NOT EXISTS idx_ft_scr_uf ON ft_scr_agregado_mensal(uf);",
        "CREATE INDEX IF NOT EXISTS idx_ft_scr_cliente ON ft_scr_agregado_mensal(cliente);",
        "CREATE INDEX IF NOT EXISTS idx_ft_scr_modalidade ON ft_scr_agregado_mensal(modalidade);",
        "CREATE INDEX IF NOT EXISTS idx_ft_scr_composite ON ft_scr_agregado_mensal(data_base, uf, cliente);",
        
        # Índices para views materializadas
        "CREATE INDEX IF NOT EXISTS idx_mv_scr_uf_data ON mv_scr_agregado_uf(data_base);",
        "CREATE INDEX IF NOT EXISTS idx_mv_scr_uf_uf ON mv_scr_agregado_uf(uf);",
        "CREATE INDEX IF NOT EXISTS idx_mv_scr_tendencia_mes ON mv_scr_tendencia_mensal(mes);",
        "CREATE INDEX IF NOT EXISTS idx_mv_scr_segmentos_volume ON mv_scr_agregado_segmentos(total_carteira_ativa);",
        "CREATE INDEX IF NOT EXISTS idx_mv_indicadores_mes ON mv_indicadores_economicos_resumo(mes);",
        
        # Índices para clusters (se existirem)
        "CREATE INDEX IF NOT EXISTS idx_clusters_id ON ft_scr_segmentos_clusters(cluster_id);",
        "CREATE INDEX IF NOT EXISTS idx_clusters_data ON ft_scr_segmentos_clusters(data_base);"
    ]
    
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            logger.info(f"Índice criado: {index_sql.split('idx_')[1].split(' ')[0] if 'idx_' in index_sql else 'índice'}")
        except Exception as e:
            logger.warning(f"Erro ao criar índice: {e}")

def refresh_materialized_views(cursor):
    """Atualiza todas as views materializadas"""
    views_to_refresh = [
        'mv_scr_agregado_uf',
        'mv_scr_tendencia_mensal',
        'mv_scr_agregado_segmentos', 
        'mv_scr_top_combinacoes_risco',
        'mv_indicadores_economicos_resumo'
    ]
    
    for view in views_to_refresh:
        try:
            logger.info(f"Atualizando view materializada: {view}")
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view};")
            logger.info(f"View {view} atualizada com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao atualizar view {view}: {e}")

def get_views_info(cursor):
    """Obtém informações sobre as views criadas"""
    logger.info("Obtendo informações das views materializadas...")
    
    cursor.execute("""
        SELECT 
            schemaname,
            matviewname,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
        FROM pg_matviews 
        WHERE matviewname LIKE 'mv_%'
        ORDER BY matviewname;
    """)
    
    results = cursor.fetchall()
    
    if results:
        logger.info("Views materializadas criadas:")
        for schema, view_name, size in results:
            logger.info(f"  - {view_name}: {size}")
    else:
        logger.warning("Nenhuma view materializada encontrada.")

def main():
    """Função principal"""
    logger.info("=== Iniciando criação de Views Materializadas ===")
    logger.info(f"Timestamp: {datetime.now()}")
    
    conn = None
    try:
        # Conecta ao banco
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Remove views existentes
        logger.info("Removendo views materializadas existentes...")
        drop_materialized_views(cursor)
        conn.commit()
        
        # Cria novas views
        logger.info("Criando novas views materializadas...")
        create_materialized_views(cursor)
        conn.commit()
        
        # Cria índices
        logger.info("Criando índices de performance...")
        create_indexes(cursor)
        conn.commit()
        
        # Obtém informações das views
        get_views_info(cursor)
        
        logger.info("=== Views materializadas criadas com sucesso! ===")
        logger.info("Para atualizar as views, execute: python refresh_materialized_views.py")
        
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Conexão com PostgreSQL fechada.")

if __name__ == "__main__":
    main()