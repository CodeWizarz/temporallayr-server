import asyncio


async def run_test():
    try:
        from app.services.storage_service import StorageService

        svc = StorageService()

        results = await svc.search_executions_by_query(
            tenant_id="dev-test-key", query_str="crash", limit=50
        )
        print("Success!", results)
    except Exception as e:
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(run_test())
