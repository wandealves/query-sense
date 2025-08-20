from typing import Any, Dict, List, Optional
import logging
import json
from contextlib import contextmanager
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class ManagerDatabase:
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
            
    def get_table_columns_detailed(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Obtém informações detalhadas sobre as colunas de uma tabela específica.
        
        Args:
            table_name: Nome da tabela
            schema_name: Nome do schema (opcional)
            
        Returns:
            Lista com informações detalhadas das colunas
        """
        db_type = self._get_database_type()
        
        try:
            if db_type == 'postgresql':
                query = """
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.ordinal_position,
                    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
                    CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
                    fk.referenced_table_name,
                    fk.referenced_column_name
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT ku.column_name, ku.table_name, ku.table_schema
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku 
                        ON tc.constraint_name = ku.constraint_name 
                        AND tc.table_schema = ku.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                ) pk ON c.column_name = pk.column_name 
                    AND c.table_name = pk.table_name 
                    AND c.table_schema = pk.table_schema
                LEFT JOIN (
                    SELECT 
                        ku.column_name, 
                        ku.table_name, 
                        ku.table_schema,
                        ccu.table_name as referenced_table_name,
                        ccu.column_name as referenced_column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku 
                        ON tc.constraint_name = ku.constraint_name 
                        AND tc.table_schema = ku.table_schema
                    JOIN information_schema.constraint_column_usage ccu 
                        ON tc.constraint_name = ccu.constraint_name 
                        AND tc.table_schema = ccu.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                ) fk ON c.column_name = fk.column_name 
                    AND c.table_name = fk.table_name 
                    AND c.table_schema = fk.table_schema
                WHERE c.table_name = :table_name
                """
                parameters = {"table_name": table_name}
                if schema_name:
                    query += " AND c.table_schema = :schema_name"
                    parameters["schema_name"] = schema_name
                query += " ORDER BY c.ordinal_position"
                
            elif db_type == 'mysql':
                query = """
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.ordinal_position,
                    CASE WHEN c.column_key = 'PRI' THEN true ELSE false END as is_primary_key,
                    CASE WHEN c.column_key = 'MUL' THEN true ELSE false END as is_foreign_key,
                    kcu.referenced_table_name,
                    kcu.referenced_column_name
                FROM information_schema.columns c
                LEFT JOIN information_schema.key_column_usage kcu 
                    ON c.table_name = kcu.table_name 
                    AND c.column_name = kcu.column_name 
                    AND c.table_schema = kcu.table_schema
                    AND kcu.referenced_table_name IS NOT NULL
                WHERE c.table_name = :table_name
                """
                parameters = {"table_name": table_name}
                if schema_name:
                    query += " AND c.table_schema = :schema_name"
                    parameters["schema_name"] = schema_name
                query += " ORDER BY c.ordinal_position"
                
            elif db_type == 'sqlite':
                # SQLite usa PRAGMA table_info()
                query = f"PRAGMA table_info({table_name})"
                results = self.execute_query(query)
                
                # Converter formato do SQLite para formato padrão
                formatted_results = []
                for row in results:
                    formatted_results.append({
                        'column_name': row['name'],
                        'data_type': row['type'],
                        'is_nullable': 'YES' if row['notnull'] == 0 else 'NO',
                        'column_default': row['dflt_value'],
                        'character_maximum_length': None,
                        'numeric_precision': None,
                        'numeric_scale': None,
                        'ordinal_position': row['cid'] + 1,
                        'is_primary_key': bool(row['pk']),
                        'is_foreign_key': False,
                        'referenced_table_name': None,
                        'referenced_column_name': None
                    })
                return formatted_results
                
            elif db_type == 'sqlserver':
                query = """
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.ordinal_position,
                    CASE WHEN pk.column_name IS NOT NULL THEN CAST(1 as bit) ELSE CAST(0 as bit) END as is_primary_key,
                    CASE WHEN fk.column_name IS NOT NULL THEN CAST(1 as bit) ELSE CAST(0 as bit) END as is_foreign_key,
                    fk.referenced_table_name,
                    fk.referenced_column_name
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT ku.column_name, ku.table_name, ku.table_schema
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku 
                        ON tc.constraint_name = ku.constraint_name 
                        AND tc.table_schema = ku.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                ) pk ON c.column_name = pk.column_name 
                    AND c.table_name = pk.table_name 
                    AND c.table_schema = pk.table_schema
                LEFT JOIN (
                    SELECT 
                        ku.column_name, 
                        ku.table_name, 
                        ku.table_schema,
                        ccu.table_name as referenced_table_name,
                        ccu.column_name as referenced_column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku 
                        ON tc.constraint_name = ku.constraint_name 
                        AND tc.table_schema = ku.table_schema
                    JOIN information_schema.constraint_column_usage ccu 
                        ON tc.constraint_name = ccu.constraint_name 
                        AND tc.table_schema = ccu.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                ) fk ON c.column_name = fk.column_name 
                    AND c.table_name = fk.table_name 
                    AND c.table_schema = fk.table_schema
                WHERE c.table_name = :table_name
                """
                parameters = {"table_name": table_name}
                if schema_name:
                    query += " AND c.table_schema = :schema_name"
                    parameters["schema_name"] = schema_name
                query += " ORDER BY c.ordinal_position"
                
            elif db_type == 'oracle':
                query = """
                SELECT 
                    atc.column_name,
                    atc.data_type,
                    CASE WHEN atc.nullable = 'Y' THEN 'YES' ELSE 'NO' END as is_nullable,
                    atc.data_default as column_default,
                    atc.char_length as character_maximum_length,
                    atc.data_precision as numeric_precision,
                    atc.data_scale as numeric_scale,
                    atc.column_id as ordinal_position,
                    CASE WHEN acc.column_name IS NOT NULL THEN 1 ELSE 0 END as is_primary_key,
                    CASE WHEN fk.column_name IS NOT NULL THEN 1 ELSE 0 END as is_foreign_key,
                    fk.referenced_table_name,
                    fk.referenced_column_name
                FROM all_tab_columns atc
                LEFT JOIN (
                    SELECT acc.column_name, acc.table_name, acc.owner
                    FROM all_cons_columns acc
                    JOIN all_constraints ac ON acc.constraint_name = ac.constraint_name
                    WHERE ac.constraint_type = 'P'
                ) acc ON atc.column_name = acc.column_name 
                    AND atc.table_name = acc.table_name 
                    AND atc.owner = acc.owner
                LEFT JOIN (
                    SELECT 
                        acc.column_name, 
                        acc.table_name, 
                        acc.owner,
                        r_acc.table_name as referenced_table_name,
                        r_acc.column_name as referenced_column_name
                    FROM all_cons_columns acc
                    JOIN all_constraints ac ON acc.constraint_name = ac.constraint_name
                    JOIN all_cons_columns r_acc ON ac.r_constraint_name = r_acc.constraint_name
                    WHERE ac.constraint_type = 'R'
                ) fk ON atc.column_name = fk.column_name 
                    AND atc.table_name = fk.table_name 
                    AND atc.owner = fk.owner
                WHERE atc.table_name = :table_name
                """
                parameters = {"table_name": table_name.upper()}
                if schema_name:
                    query += " AND atc.owner = :schema_name"
                    parameters["schema_name"] = schema_name.upper()
                query += " ORDER BY atc.column_id"
                
            else:
                # Fallback genérico usando information_schema
                query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position,
                    false as is_primary_key,
                    false as is_foreign_key,
                    null as referenced_table_name,
                    null as referenced_column_name
                FROM information_schema.columns 
                WHERE table_name = :table_name
                """
                parameters = {"table_name": table_name}
                if schema_name:
                    query += " AND table_schema = :schema_name"
                    parameters["schema_name"] = schema_name
                query += " ORDER BY ordinal_position"
                
            return self.execute_query(query, parameters)
            
        except SQLAlchemyError as e:
            logger.error(f"Erro ao obter colunas detalhadas da tabela {table_name}: {e}")
            raise
            
    def get_database_structure_json(self, schema_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtém a estrutura completa do banco de dados em formato JSON.
        
        Args:
            schema_filter: Nome do schema específico para filtrar (opcional)
            
        Returns:
            Dicionário com a estrutura completa do banco
        """
        db_type = self._get_database_type()
        
        try:
            # Estrutura base do JSON
            structure = {
                "database_type": db_type,
                "connection_string": self.connection_string.split('@')[0] + '@***',  # Ocultar credenciais
                "schemas": []
            }
            
            # Obter schemas
            if schema_filter:
                schemas = [schema_filter] if schema_filter in self.get_schemas() else []
            else:
                schemas = self.get_schemas()
                
            for schema_name in schemas:
                schema_info = {
                    "schema_name": schema_name,
                    "tables": []
                }
                
                # Obter tabelas do schema
                if db_type == 'sqlite':
                    tables = self.get_tables()
                else:
                    tables = self.get_tables(schema_name)
                    
                for table in tables:
                    table_info = {
                        "table_name": table['table_name'],
                        "table_type": table['table_type'],
                        "columns": []
                    }
                    
                    # Obter colunas detalhadas da tabela
                    columns = self.get_table_columns_detailed(
                        table['table_name'], 
                        schema_name if db_type != 'sqlite' else None
                    )
                    
                    for column in columns:
                        column_info = {
                            "column_name": column['column_name'],
                            "data_type": column['data_type'],
                            "is_nullable": column['is_nullable'],
                            "column_default": column['column_default'],
                            "character_maximum_length": column.get('character_maximum_length'),
                            "numeric_precision": column.get('numeric_precision'),
                            "numeric_scale": column.get('numeric_scale'),
                            "ordinal_position": column.get('ordinal_position'),
                            "is_primary_key": column.get('is_primary_key', False),
                            "is_foreign_key": column.get('is_foreign_key', False),
                            "referenced_table_name": column.get('referenced_table_name'),
                            "referenced_column_name": column.get('referenced_column_name')
                        }
                        table_info["columns"].append(column_info)
                        
                    schema_info["tables"].append(table_info)
                    
                structure["schemas"].append(schema_info)
                
            return structure
            
        except SQLAlchemyError as e:
            logger.error(f"Erro ao obter estrutura do banco: {e}")
            raise
            
    def export_structure_to_file(self, file_path: str, schema_filter: Optional[str] = None, indent: int = 2) -> bool:
        """
        Exporta a estrutura do banco de dados para um arquivo JSON.
        
        Args:
            file_path: Caminho do arquivo para salvar
            schema_filter: Nome do schema específico para filtrar (opcional)
            indent: Indentação do JSON (padrão: 2)
            
        Returns:
            True se exportado com sucesso
        """
        try:
            structure = self.get_database_structure_json(schema_filter)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(structure, f, indent=indent, ensure_ascii=False, default=str)
                
            logger.info(f"Estrutura exportada para: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao exportar estrutura: {e}")
            return False
        
    def __enter__(self):
        """Suporte para context manager."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup automático ao sair do context manager."""
        self.disconnect()