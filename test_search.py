import asyncio
from app.services.search import search_executions


async def test_search():
    print("Testing search_executions with pagination and function_name filtering...")

    # 1. Base Test: No function name
    print("\n--- TEST: No function_name ---")
    results = await search_executions(
        tenant_id="demo-tenant",
        function_name=None,
        start_time=None,
        end_time=None,
        limit=10,
        offset=0,
    )
    print(f"Results (limit=10): {len(results)}")
    for r in results:
        print(r)

    # 2. Function Filter Test
    print("\n--- TEST: Filtering by fake_llm_call ---")
    results = await search_executions(
        tenant_id="demo-tenant",
        function_name="fake_llm_call",
        start_time=None,
        end_time=None,
        limit=10,
        offset=0,
    )
    print(f"Results (limit=10): {len(results)}")
    for r in results:
        print(r)


if __name__ == "__main__":
    asyncio.run(test_search())
