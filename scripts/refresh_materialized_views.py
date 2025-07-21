import logging
import psycopg2
from datetime import datetime
import sys
import os

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('refresh_materialized_views.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configurações de Conexão ---
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
        logger.info("Conexão estabelecida com sucesso.")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar: {e}")
        raise

def refresh_materialized_views(cursor, specific_view=None):
    """Atualiza views materializadas"""
    views_to_refresh = [
        'mv_scr_agregado_uf',
        'mv_scr_tendencia_mensal',
        'mv_scr_agregado_segmentos',
        'mv_scr_top_combinacoes_risco',
        'mv_indicadores_economicos_resumo'
    ]
    
    if specific_view:
        if specific_view in views_to_refresh:
            views_to_refresh = [specific_view]
        else:
            logger.error(f"View '{specific_view}' não encontrada.")
            return False
    
    success_count = 0
    for view in views_to_refresh:
        try:
            start_time = datetime.now()
            logger.info(f"Atualizando {view}...")
            
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view};")
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"{view} atualizada em {duration:.2f}s")
            success_count += 1
            
        except Exception as e:
            logger.error(f"Erro ao atualizar {view}: {e}")
    
    logger.info(f"Concluído: {success_count}/{len(views_to_refresh)} views atualizadas.")
    return success_count == len(views_to_refresh)

def main():
    """Função principal"""
    logger.info("=== Iniciando atualização das views ===")
    
    specific_view = sys.argv[1] if len(sys.argv) > 1 else None
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        success = refresh_materialized_views(cursor, specific_view)
        conn.commit()
        
        if success:
            logger.info("=== Atualização concluída com sucesso! ===")
        else:
            logger.warning("=== Atualização com erros ===")
            
    except Exception as e:
        logger.error(f"Erro: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()