import asyncio
import uuid
import random
from datetime import datetime, timedelta

from app.core.database import async_session_maker
from app.models.event import Event


async def generate_demo_traffic(tenant_id: str, count: int = 5000):
    print(f"Generating {count} demo execution graphs for {tenant_id}...")

    functions = [
        "fake_llm_call",
        "retrieve_docs",
        "format_prompt",
        "execute_sql",
        "classify_intent",
    ]

    async with async_session_maker() as session:
        for _ in range(count):
            # random past 24 hour drift natively dynamically
            drift_minutes = random.randint(0, 24 * 60)
            target_time = datetime.utcnow() - timedelta(minutes=drift_minutes)

            main_func = random.choice(functions)
            status = "FAILED" if random.random() < 0.05 else "SUCCESS"

            payload = {
                "function_name": main_func,
                "status": status,
                "nodes": [
                    {
                        "name": main_func,
                        "metadata": {
                            "output": {
                                "duration_ms": random.randint(10, 500),
                                "tokens_used": random.randint(50, 2000),
                            }
                        },
                    },
                    {
                        "name": random.choice(functions),
                        "metadata": {"output": {"duration_ms": random.randint(5, 100)}},
                    },
                ],
            }

            event = Event(
                tenant_id=tenant_id,
                event_type="execution_graph",
                timestamp=target_time,
                payload=payload,
            )
            session.add(event)

        await session.commit()
    print("Demo DB populated successfully!")


if __name__ == "__main__":
    asyncio.run(generate_demo_traffic("demo-tenant", 5000))
