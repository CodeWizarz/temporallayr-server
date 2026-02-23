import logging
from typing import List, Dict, Any
from sqlalchemy import select, asc, desc

from app.core.database import async_session_maker
from app.models.event import Event
from app.query.parser import QueryAST, Operator

logger = logging.getLogger("temporallayr.query.engine")


async def run_query(
    parsed_query: QueryAST, tenant_id: str, limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Executes a parsed query against the Event payload JSONB store safely and efficiently.
    Always filters by tenant_id. Enforces a maximum limit of 500.
    """
    print("[QUERY ENGINE] executed")

    # Enforce hard limits on DB extraction
    effective_limit = min(limit, 500)

    if not async_session_maker:
        logger.warning("Database offline. Returning mocked payload array.")
        return []

    stmt = select(Event.payload).where(Event.api_key == tenant_id)

    # Translate conditions into SQLAlchemy filters securely mapped onto JSONB payloads
    for cond in parsed_query.conditions:
        field = cond.field
        op = cond.operator
        val = cond.value

        # Cast numeric types if appropriate
        try:
            val_float = float(val)
            val_int = int(val)
            is_numeric = (
                str(val_float) == str(val)
                or str(val_int) == str(val)
                or "." in str(val)
            )
        except ValueError:
            is_numeric = False

        json_field = Event.payload[field]

        if op == Operator.EQ:
            stmt = stmt.where(json_field.as_string() == str(val))
        elif op == Operator.NEQ:
            stmt = stmt.where(json_field.as_string() != str(val))
        elif op == Operator.GT:
            if is_numeric:
                # It's cleaner to compare strings in jsonb for date strings vs casting to floats
                try:
                    stmt = stmt.where(json_field.as_float() > float(val))
                except Exception:
                    stmt = stmt.where(json_field.as_string() > str(val))
            else:
                stmt = stmt.where(json_field.as_string() > str(val))
        elif op == Operator.LT:
            if is_numeric:
                try:
                    stmt = stmt.where(json_field.as_float() < float(val))
                except Exception:
                    stmt = stmt.where(json_field.as_string() < str(val))
            else:
                stmt = stmt.where(json_field.as_string() < str(val))

    stmt = stmt.order_by(Event.timestamp.desc()).limit(effective_limit)

    executions = []
    try:
        async with async_session_maker() as session:
            result = await session.execute(stmt)
            for payload in result.scalars():
                if not payload:
                    continue
                # Extract specifically requested fields cleanly bounding allocations safely
                exec_id = payload.get("execution_id") or payload.get("id") or "unknown"
                timestamp = (
                    payload.get("_ingested_at") or payload.get("timestamp") or ""
                )

                # Best-effort node extraction defensively parsing topologies natively
                root_node = "unknown"
                nodes = payload.get("nodes", [])
                if nodes and isinstance(nodes, list):
                    # Try to find a node with no parent_id or an empty parent_id
                    for n in nodes:
                        if isinstance(n, dict) and not n.get("parent_id"):
                            root_node = n.get("name", "unknown")
                            break
                    if root_node == "unknown" and isinstance(nodes[0], dict):
                        root_node = nodes[0].get("name", "unknown")

                executions.append(
                    {
                        "execution_id": exec_id,
                        "timestamp": timestamp,
                        "root_node": root_node,
                        "failure_type": payload.get("failure_type", ""),
                        "duration": payload.get("duration", 0),
                    }
                )
    except Exception as e:
        logger.error(f"Failed extracting tenant query dynamically smoothly: {e}")

    return executions
