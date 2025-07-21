import logging
import psycopg2
import pandas as pd
from datetime import datetime

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('diagnose_views.log'),
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

def check_database_connection(cursor):
    """Verifica se a conexão com o banco está funcionando"""
    try:
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL conectado: {version}")
        return True
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")
        return False

def check_base_tables(cursor):
    """Verifica se as tabelas base existem"""
    logger.info("=== Verificando tabelas base ===")
    
    base_tables = [
        'ft_scr_agregado_mensal',
        'ft_indicadores_economicos_mensal',
        'ft_scr_segmentos_clusters',
        'dim_cluster_profiles'
    ]
    
    for table in base_tables:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as total_rows
                FROM {table}
            """)
            count = cursor.fetchone()[0]
            logger.info(f"✓ Tabela '{table}': {count:,} registros")
        except Exception as e:
            logger.error(f"✗ Tabela '{table}' não encontrada ou erro: {e}")

def check_materialized_views(cursor):
    """Verifica se as views materializadas existem"""
    logger.info("=== Verificando views materializadas ===")
    
    try:
        cursor.execute("""
            SELECT 
                matviewname,
                ispopulated,
                pg_size_pretty(pg_total_relation_size('public.'||matviewname)) as size
            FROM pg_matviews 
            WHERE matviewname LIKE 'mv_%'
            ORDER BY matviewname
        """)
        
        views = cursor.fetchall()
        
        if views:
            logger.info(f"Encontradas {len(views)} views materializadas:")
            for view_name, is_populated, size in views:
                status = "✓ Populada" if is_populated else "✗ Não populada"
                logger.info(f"  - {view_name}: {status} ({size})")
        else:
            logger.warning("✗ Nenhuma view materializada encontrada!")
            
        return views
        
    except Exception as e:
        logger.error(f"Erro ao verificar views materializadas: {e}")
        return []

def check_specific_view_content(cursor, view_name):
    """Verifica o conteúdo de uma view específica"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
        count = cursor.fetchone()[0]
        logger.info(f"View '{view_name}' contém {count:,} registros")
        
        # Mostra uma amostra dos dados
        cursor.execute(f"SELECT * FROM {view_name} LIMIT 3")
        sample = cursor.fetchall()
        if sample:
            logger.info(f"Amostra de dados da '{view_name}':")
            for row in sample:
                logger.info(f"  {row}")
                
    except Exception as e:
        logger.error(f"Erro ao verificar conteúdo da view '{view_name}': {e}")

def diagnose_views_issue(cursor):
    """Diagnóstica problemas específicos com as views"""
    logger.info("=== Diagnóstico de problemas ===")
    
    expected_views = [
        'mv_scr_agregado_uf',
        'mv_scr_tendencia_mensal',
        'mv_scr_agregado_segmentos',
        'mv_scr_top_combinacoes_risco',
        'mv_indicadores_economicos_resumo'
    ]
    
    # Verifica quais views existem
    cursor.execute("""
        SELECT matviewname 
        FROM pg_matviews 
        WHERE matviewname LIKE 'mv_%'
    """)
    existing_views = [row[0] for row in cursor.fetchall()]
    
    missing_views = [view for view in expected_views if view not in existing_views]
    
    if missing_views:
        logger.error(f"Views materializadas ausentes: {missing_views}")
        logger.info("SOLUÇÃO: Execute novamente o script create_materialized_views.py")
    else:
        logger.info("✓ Todas as views materializadas esperadas existem")
        
        # Verifica se estão populadas
        cursor.execute("""
            SELECT matviewname, ispopulated 
            FROM pg_matviews 
            WHERE matviewname LIKE 'mv_%' AND NOT ispopulated
        """)
        unpopulated_views = cursor.fetchall()
        
        if unpopulated_views:
            logger.error(f"Views não populadas: {[v[0] for v in unpopulated_views]}")
            logger.info("SOLUÇÃO: Execute o script refresh_materialized_views.py")
        else:
            logger.info("✓ Todas as views estão populadas")

def main():
    """Função principal de diagnóstico"""
    logger.info("=== DIAGNÓSTICO DE VIEWS MATERIALIZADAS ===")
    logger.info(f"Timestamp: {datetime.now()}")
    
    conn = None
    try:
        # Conecta ao banco
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verifica conexão
        if not check_database_connection(cursor):
            return
        
        # Verifica tabelas base
        check_base_tables(cursor)
        
        # Verifica views materializadas
        views = check_materialized_views(cursor)
        
        # Diagnóstica problemas
        diagnose_views_issue(cursor)
        
        # Se existem views, verifica conteúdo de uma delas
        if views:
            logger.info("=== Verificando conteúdo de uma view ===")
            check_specific_view_content(cursor, views[0][0])
        
        logger.info("=== Diagnóstico concluído ===")
        
    except Exception as e:
        logger.error(f"Erro durante o diagnóstico: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Conexão fechada.")

if __name__ == "__main__":
    main()