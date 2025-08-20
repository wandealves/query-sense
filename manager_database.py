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
        
    def _get_database_type(self) -> str:
        """
        Detecta o tipo de banco de dados baseado na connection string.
        
        Returns:
            String identificando o tipo do banco
        """
        connection_lower = self.connection_string.lower()
        
        if connection_lower.startswith('postgresql://') or connection_lower.startswith('postgres://'):
            return 'postgresql'
        elif connection_lower.startswith('mysql+') or connection_lower.startswith('mysql://'):
            return 'mysql'
        elif connection_lower.startswith('sqlite'):
            return 'sqlite'
        elif connection_lower.startswith('mssql+') or connection_lower.startswith('sqlserver'):
            return 'sqlserver'
        elif connection_lower.startswith('oracle+'):
            return 'oracle'
        else:
            return 'unknown'
            
    def get_schemas(self) -> List[str]:
        """
        Obtém a lista de schemas disponíveis no banco de dados.
        
        Returns:
            Lista com nomes dos schemas
        """
        db_type = self._get_database_type()
        
        try:
            if db_type == 'postgresql':
                # PostgreSQL - usar information_schema ou pg_namespace
                query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
                """
                results = self.execute_query(query)
                return [row['schema_name'] for row in results]
                
            elif db_type == 'mysql':
                # MySQL - usar information_schema
                query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
                ORDER BY schema_name
                """
                results = self.execute_query(query)
                return [row['schema_name'] for row in results]
                
            elif db_type == 'sqlite':
                # SQLite não tem schemas reais, retorna schema padrão
                return ['main']
                
            elif db_type == 'sqlserver':
                # SQL Server - usar sys.schemas
                query = """
                SELECT name as schema_name
                FROM sys.schemas 
                WHERE name NOT IN ('sys', 'information_schema', 'guest', 'INFORMATION_SCHEMA')
                ORDER BY name
                """
                results = self.execute_query(query)
                return [row['schema_name'] for row in results]
                
            elif db_type == 'oracle':
                # Oracle - usar all_users (equivalente a schemas)
                query = """
                SELECT username as schema_name
                FROM all_users 
                WHERE username NOT IN ('SYS', 'SYSTEM', 'DBSNMP', 'SYSMAN', 'OUTLN')
                ORDER BY username
                """
                results = self.execute_query(query)
                return [row['schema_name'] for row in results]
                
            else:
                # Fallback genérico usando information_schema
                query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                ORDER BY schema_name
                """
                results = self.execute_query(query)
                return [row['schema_name'] for row in results]
                
        except SQLAlchemyError as e:
            logger.warning(f"Erro ao obter schemas: {e}")
            # Retorna lista vazia se não conseguir obter schemas
            return []
            
    def get_tables(self, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Obtém a lista de tabelas de um schema específico ou do schema padrão.
        
        Args:
            schema_name: Nome do schema (opcional)
            
        Returns:
            Lista com informações das tabelas
        """
        db_type = self._get_database_type()
        
        try:
            if db_type == 'postgresql':
                if schema_name:
                    query = """
                    SELECT table_name, table_type
                    FROM information_schema.tables 
                    WHERE table_schema = :schema_name
                    ORDER BY table_name
                    """
                    parameters = {"schema_name": schema_name}
                else:
                    query = """
                    SELECT table_name, table_type, table_schema
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY table_schema, table_name
                    """
                    parameters = {}
                    
            elif db_type == 'mysql':
                if schema_name:
                    query = """
                    SELECT table_name, table_type
                    FROM information_schema.tables 
                    WHERE table_schema = :schema_name
                    ORDER BY table_name
                    """
                    parameters = {"schema_name": schema_name}
                else:
                    query = """
                    SELECT table_name, table_type, table_schema
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
                    ORDER BY table_schema, table_name
                    """
                    parameters = {}
                    
            elif db_type == 'sqlite':
                # SQLite - usar sqlite_master
                query = """
                SELECT name as table_name, type as table_type
                FROM sqlite_master 
                WHERE type IN ('table', 'view')
                ORDER BY name
                """
                parameters = {}
                
            elif db_type == 'sqlserver':
                if schema_name:
                    query = """
                    SELECT table_name, table_type
                    FROM information_schema.tables 
                    WHERE table_schema = :schema_name
                    ORDER BY table_name
                    """
                    parameters = {"schema_name": schema_name}
                else:
                    query = """
                    SELECT table_name, table_type, table_schema
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('sys', 'information_schema')
                    ORDER BY table_schema, table_name
                    """
                    parameters = {}
                    
            else:
                # Fallback genérico
                if schema_name:
                    query = """
                    SELECT table_name, table_type
                    FROM information_schema.tables 
                    WHERE table_schema = :schema_name
                    ORDER BY table_name
                    """
                    parameters = {"schema_name": schema_name}
                else:
                    query = """
                    SELECT table_name, table_type, table_schema
                    FROM information_schema.tables 
                    ORDER BY table_schema, table_name
                    """
                    parameters = {}
                    
            return self.execute_query(query, parameters)
            
        except SQLAlchemyError as e:
            logger.error(f"Erro ao obter tabelas: {e}")
            raise
        
    def __enter__(self):
        """Suporte para context manager."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup automático ao sair do context manager."""
        self.disconnect()