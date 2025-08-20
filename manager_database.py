from typing import Any, Dict, List, Optional
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """
    Suporta múltiplos SGBDs através do SQLAlchemy:
    - PostgreSQL
    - MySQL/MariaDB
    - SQLite
    - SQL Server
    - Oracle
    """
    
    def __init__(self, connection_string: str, **engine_kwargs):
        """
        Inicializa o conector de banco de dados.
        
        Args:
            connection_string: String de conexão do banco (ex: postgresql://user:pass@host:port/db)
            **engine_kwargs: Argumentos adicionais para o engine do SQLAlchemy
        """
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None
        self.engine_kwargs = engine_kwargs
        
    def connect(self) -> None:
        """Estabelece conexão com o banco de dados."""
        try:
            self.engine = create_engine(self.connection_string, **self.engine_kwargs)
            # Testa a conexão
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Conexão estabelecida com sucesso")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao conectar: {e}")
            raise
            
    def disconnect(self) -> None:
        """Fecha a conexão com o banco de dados."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Conexão fechada")
            
    @contextmanager
    def get_connection(self):
        """Context manager para gerenciar conexões automaticamente."""
        if not self.engine:
            self.connect()
            
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()
            
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Executa uma query SELECT e retorna os resultados.
        
        Args:
            query: Query SQL a ser executada
            parameters: Parâmetros para a query (opcional)
            
        Returns:
            Lista de dicionários com os resultados
        """
        with self.get_connection() as conn:
            try:
                result = conn.execute(text(query), parameters or {})
                if result.returns_rows:
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in result.fetchall()]
                return []
            except SQLAlchemyError as e:
                logger.error(f"Erro ao executar query: {e}")
                raise
                
    def execute_non_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> int:
        """
        Executa uma query de modificação (INSERT, UPDATE, DELETE).
        
        Args:
            query: Query SQL a ser executada
            parameters: Parâmetros para a query (opcional)
            
        Returns:
            Número de linhas afetadas
        """
        with self.get_connection() as conn:
            trans = conn.begin()
            try:
                result = conn.execute(text(query), parameters or {})
                trans.commit()
                return result.rowcount
            except SQLAlchemyError as e:
                trans.rollback()
                logger.error(f"Erro ao executar query: {e}")
                raise
                
    def execute_many(self, query: str, parameters_list: List[Dict[str, Any]]) -> int:
        """
        Executa múltiplas queries com diferentes parâmetros em uma transação.
        
        Args:
            query: Query SQL a ser executada
            parameters_list: Lista de dicionários com parâmetros
            
        Returns:
            Número total de linhas afetadas
        """
        with self.get_connection() as conn:
            trans = conn.begin()
            try:
                total_affected = 0
                for parameters in parameters_list:
                    result = conn.execute(text(query), parameters)
                    total_affected += result.rowcount
                trans.commit()
                return total_affected
            except SQLAlchemyError as e:
                trans.rollback()
                logger.error(f"Erro ao executar queries em lote: {e}")
                raise
                
    def execute_transaction(self, queries: List[tuple]) -> bool:
        """
        Executa múltiplas queries em uma única transação.
        
        Args:
            queries: Lista de tuplas (query, parameters)
            
        Returns:
            True se todas as queries foram executadas com sucesso
        """
        with self.get_connection() as conn:
            trans = conn.begin()
            try:
                for query, parameters in queries:
                    conn.execute(text(query), parameters or {})
                trans.commit()
                return True
            except SQLAlchemyError as e:
                trans.rollback()
                logger.error(f"Erro na transação: {e}")
                raise
                
    def test_connection(self) -> bool:
        """
        Testa se a conexão está funcionando.
        
        Returns:
            True se a conexão está ativa
        """
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False
            
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtém informações sobre as colunas de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Lista com informações das colunas
        """
        # Query genérica que funciona na maioria dos SGBDs
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = :table_name
        ORDER BY ordinal_position
        """
        return self.execute_query(query, {"table_name": table_name})
        
    def __enter__(self):
        """Suporte para context manager."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup automático ao sair do context manager."""
        self.disconnect()