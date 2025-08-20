import os
import logging
from dotenv import load_dotenv
from langchain_litellm import ChatLiteLLM
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from agent_state import AgentState

logger = logging.getLogger(__name__)

load_dotenv()

class Agent:
    """
    Suporta múltiplos SGBDs através do SQLAlchemy:
    - PostgreSQL
    - MySQL/MariaDB
    - SQLite
    - SQL Server
    - Oracle
    """
    
    def __init__(self, model: str, table_schemas:str, database: str, environ: str,temperature=0.1):
        """
        Inicializa o conector de banco de dados.
        
        Args:
            connection_string: String de conexão do banco (ex: postgresql://user:pass@host:port/db)
            **engine_kwargs: Argumentos adicionais para o engine do SQLAlchemy
        """
        self.llm = ChatLiteLLM(model=model, temperature=temperature)
        self.table_schemas = table_schemas
        self.database = database
        os.environ[environ] = os.getenv(environ)


    def search_engineer_node(self,state: AgentState):
        # Fornecemos o esquema do banco de dados diretamente
        state['table_schemas'] = self.table_schemas
        state['database'] = self.database
        return {"table_schemas": state['table_schemas'], "database": state['database']}
    

    def senior_sql_writer_node(self,state: AgentState):
        role_prompt = """
    Você é um especialista em SQL. Sua tarefa é escrever **apenas** a consulta SQL que responda à pergunta do usuário. A consulta deve:

    - Usar a sintaxe SQL padrão em inglês.
    - Utilizar os nomes das tabelas e colunas conforme definidos no esquema do banco de dados.
    - Não incluir comentários, explicações ou qualquer texto adicional.
    - Não utilizar formatação de código ou markdown.
    - Retornar apenas a consulta SQL válida.
    """
        instruction = f"Esquema do banco de dados:\n{state['table_schemas']}\n"
        if len(state['reflect']) > 0:
            instruction += f"Considere os seguintes feedbacks:\n{chr(10).join(state['reflect'])}\n"
        instruction += f"Escreva a consulta SQL que responda à seguinte pergunta: {state['question']}\n"
        messages = [
            SystemMessage(content=role_prompt), 
            HumanMessage(content=instruction)
        ]
        response = self.llm.invoke(messages)
        return {"sql": response.content.strip(), "revision": state['revision'] + 1}
    
    #Função do QA Engineer
    def senior_qa_engineer_node(self,state: AgentState):
        role_prompt = """
    Você é um engenheiro de QA especializado em SQL. Sua tarefa é verificar se a consulta SQL fornecida responde corretamente à pergunta do usuário.
    """
        instruction = f"Com base no seguinte esquema de banco de dados:\n{state['table_schemas']}\n"
        instruction += f"E na seguinte consulta SQL:\n{state['sql']}\n"
        instruction += f"Verifique se a consulta SQL pode completar a tarefa: {state['question']}\n"
        instruction += "Responda 'ACEITO' se estiver correta ou 'REJEITADO' se não estiver.\n"
        messages = [
            SystemMessage(content=role_prompt), 
            HumanMessage(content=instruction)
        ]
        response = self.llm.invoke(messages)
        return {"accepted": 'ACEITO' in response.content.upper()}
    
    #Função do Chief DBA
    def chief_dba_node(self,state: AgentState):
        role_prompt = """
        Você é um DBA experiente. Sua tarefa é fornecer feedback detalhado para melhorar a consulta SQL fornecida.
        """
        instruction = f"Com base no seguinte esquema de banco de dados:\n{state['table_schemas']}\n"
        instruction += f"E na seguinte consulta SQL:\n{state['sql']}\n"
        instruction += f"Por favor, forneça recomendações úteis e detalhadas para ajudar a melhorar a consulta SQL para a tarefa: {state['question']}\n"
        messages = [
            SystemMessage(content=role_prompt), 
            HumanMessage(content=instruction)
        ]
        response = self.llm.invoke(messages)
        return {"reflect": [response.content]}
    
    def compile(self):
        """
        Executa o agente com o estado fornecido.
        
        Args:
            state: Estado do agente contendo informações sobre a pergunta, esquema do banco de dados, etc.
        
        Returns:
            Dicionário com os resultados da execução do agente.
        """
        builder = StateGraph(AgentState)

        # Adicionar nós
        builder.add_node("search_engineer", self.search_engineer_node)
        builder.add_node("sql_writer", self.senior_sql_writer_node)
        builder.add_node("qa_engineer", self.senior_qa_engineer_node)
        builder.add_node("chief_dba", self.chief_dba_node)

        # Adicionar arestas
        builder.add_edge("search_engineer", "sql_writer")
        builder.add_edge("sql_writer", "qa_engineer")
        builder.add_edge("chief_dba", "sql_writer")

        # Adicionar arestas condicionais
        builder.add_conditional_edges(
            "qa_engineer", 
            lambda state: END if state['accepted'] or state['revision'] >= state['max_revision'] else "reflect", 
            {END: END, "reflect": "chief_dba"}
        )

        # Definir ponto de entrada
        builder.set_entry_point("search_engineer")

        # Configurar o checkpointer usando MemorySaver
        memory = MemorySaver()

        # Compilar o grafo com o checkpointer
        self.graph = builder.compile(checkpointer=memory)

        def run(self, question: str, max_revision = 10):
            """
            Executa o agente com o estado fornecido.
            
            Args:
                state: Estado do agente contendo informações sobre a pergunta, esquema do banco de dados, etc.
            
            Returns:
                Dicionário com os resultados da execução do agente.
            """
            # Inicializar o estado
            initial_state = {
            'question': question,
            'table_schemas': '',  # Será preenchido pelo 'search_engineer_node'
            'database': '',       # Será preenchido pelo 'search_engineer_node'
            'sql': '',
            'reflect': [],
            'accepted': False,
            'revision': 0,
            'max_revision': max_revision
            }

            # Executar o grafo
            thread = {"configurable": {"thread_id": "1"}}
            for s in self.graph.stream(initial_state, thread):
                pass  # O processamento é feito internamente

            # Obter o estado final
            self.state = self.graph.get_state(thread)
