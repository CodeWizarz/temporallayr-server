from typing import Optional, Dict, Any


async def detect_execution_failure(
    execution: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Robustly scan execution payloads natively determining if structural failures exist.
    """
    print("[FAILURE DETECTOR] checked execution")

    if not isinstance(execution, dict):
        return None

    # Safely handle diverse graph structures gracefully
    graph = execution.get("graph", {})

    # Extract nodes array defensively tracking mapping variations
    if isinstance(graph, dict) and "nodes" in graph:
        nodes = graph.get("nodes")
    else:
        nodes = execution.get("nodes", [])

    if not isinstance(nodes, list):
        return None

    for node in nodes:
        if not isinstance(node, dict):
            continue

        metadata = node.get("metadata", {})
        if not isinstance(metadata, dict):
            continue

        # Inspect metadata for standard failure signatures natively converting mapping to lowercase string
        meta_str = str(metadata).lower()

        if "error" in meta_str or "exception" in meta_str or "traceback" in meta_str:
            # Reconstruct incident dictionary cleanly
            return {
                "tenant_id": execution.get("tenant_id", "unknown"),
                "execution_id": execution.get("execution_id")
                or execution.get("id", "unknown"),
                "timestamp": node.get("created_at")
                or execution.get("created_at", "unknown"),
                "failure_type": "runtime_error",
                "node_name": node.get("name", "unknown"),
                "summary": "Execution failure detected in node metadata.",
            }

    return None
