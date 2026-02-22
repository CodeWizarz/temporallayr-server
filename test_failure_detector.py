import asyncio
from app.services.failure_detector import detect_execution_failure


async def test_detectors():
    print("--- Test 1: Happy Path ---")
    happy_graph = {
        "tenant_id": "tenant-A",
        "execution_id": "exec-1",
        "created_at": "2026-02-22T10:00:00Z",
        "nodes": [{"name": "Step1", "metadata": {"status": "ok"}}],
    }
    res = await detect_execution_failure(happy_graph)
    print("Result:", res)
    assert res is None

    print("\n--- Test 2: Error in Metadata ---")
    error_graph = {
        "tenant_id": "tenant-B",
        "id": "exec-2",
        "graph": {
            "nodes": [
                {
                    "name": "FailingStep",
                    "created_at": "2026-02-22T10:05:00Z",
                    "metadata": {
                        "exception": "ZeroDivisionError",
                        "details": "divided by zero",
                    },
                }
            ]
        },
    }
    res = await detect_execution_failure(error_graph)
    print("Result:", res)
    assert res is not None
    assert res["tenant_id"] == "tenant-B"
    assert res["execution_id"] == "exec-2"
    assert res["failure_type"] == "runtime_error"
    assert res["node_name"] == "FailingStep"

    print("\n--- Test 3: Malformed Graphs ---")
    res1 = await detect_execution_failure(None)  # Not a dict
    res2 = await detect_execution_failure({"nodes": "not an array"})  # Malformed nodes
    res3 = await detect_execution_failure(
        {"nodes": [{"metadata": "not a dict"}]}
    )  # Malformed metadata
    print("Result Malformed 1:", res1)
    print("Result Malformed 2:", res2)
    print("Result Malformed 3:", res3)
    assert res1 is None
    assert res2 is None
    assert res3 is None

    print("\nAll tests passed successfully!")


if __name__ == "__main__":
    asyncio.run(test_detectors())
