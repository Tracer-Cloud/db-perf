from dataclasses import dataclass


@dataclass
class Query:
    name: str
    query: str
