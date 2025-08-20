from typing import List, TypedDict


class AgentState(TypedDict):
    question: str
    table_schemas: str
    database: str
    sql: str
    reflect: List[str]
    accepted: bool
    revision: int
    max_revision: int