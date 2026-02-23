from enum import Enum
from typing import List, Union
from pydantic import BaseModel, Field


class Operator(str, Enum):
    EQ = "=="
    NEQ = "!="
    GT = ">"
    LT = "<"


class Condition(BaseModel):
    field: str
    operator: Operator
    value: Union[str, int, float]


class QueryAST(BaseModel):
    conditions: List[Condition]


def parse_query(query: str) -> QueryAST:
    """
    Parses a simple query string into a QueryAST.
    Supports: field operator value [AND field operator value ...]
    Operators: ==, !=, >, <
    """
    if not query or not query.strip():
        return QueryAST(conditions=[])

    conditions = []
    # Split by AND, being careful with whitespace
    parts = [p.strip() for p in query.split(" AND ")]

    for part in parts:
        if not part:
            continue

        # Find the operator
        op_found = None
        op_idx = -1
        # Order matters: check 2-char operators first
        for op in ["==", "!=", ">", "<"]:
            idx = part.find(op)
            if idx != -1:
                op_found = op
                op_idx = idx
                break

        if not op_found:
            raise ValueError(f"Invalid syntax: No valid operator found in '{part}'")

        field = part[:op_idx].strip()
        value_str = part[op_idx + len(op_found) :].strip()

        if not field:
            raise ValueError(f"Invalid syntax: Missing field in '{part}'")
        if not value_str:
            raise ValueError(f"Invalid syntax: Missing value in '{part}'")

        # Strip quotes if present
        if value_str.startswith('"') and value_str.endswith('"'):
            value_str = value_str[1:-1]
        elif value_str.startswith("'") and value_str.endswith("'"):
            value_str = value_str[1:-1]

        conditions.append(
            Condition(field=field, operator=Operator(op_found), value=value_str)
        )

    return QueryAST(conditions=conditions)
