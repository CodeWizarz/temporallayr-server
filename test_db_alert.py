from app.core.database import async_session_maker
from app.models.event import AlertRule
import asyncio


async def test_insert():
    if not async_session_maker:
        print("Engine offline.")
        return
    try:
        async with async_session_maker() as session:
            new_rule = AlertRule(
                tenant_id="dev-test-key",
                name="runtime failures",
                failure_type="runtime_error",
                node_name=None,
                webhook_url="https://example.com/webhook",
            )
            session.add(new_rule)
            await session.commit()
            print("Successfully inserted rule into Postgres!")
    except Exception as e:
        print(f"Exception Triggered: {e}")


asyncio.run(test_insert())
