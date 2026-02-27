import argparse
import random
import time
import httpx
import uuid
from datetime import datetime, timezone, timedelta

FUNCTIONS = [
    "fake_llm_call",
    "retrieve_docs",
    "format_prompt",
    "execute_sql",
    "classify_intent",
]


def generate_events(count=50):
    events = []

    for i in range(count):
        drift_minutes = random.randint(0, 24 * 60)
        target_time = datetime.now(timezone.utc) - timedelta(minutes=drift_minutes)
        main_func = random.choice(FUNCTIONS)
        is_error = random.random() < 0.10
        status = "FAILED" if is_error else "SUCCESS"
        exec_id = f"demo-exec-{uuid.uuid4().hex[:8]}"

        payload = {
            "execution_id": exec_id,
            "function_name": main_func,
            "status": status,
            "nodes": [
                {
                    "name": main_func,
                    "metadata": {
                        "inputs": {"query": "Sample user query"},
                        "output": {
                            "duration_ms": random.randint(50, 2000),
                            "tokens_used": random.randint(50, 3000),
                        },
                    },
                }
            ],
        }

        if is_error:
            payload["nodes"][0]["metadata"]["error"] = (
                "Simulated timeout or LLM failure"
            )

        events.append(
            {
                "event_type": "execution_graph",
                "timestamp": target_time.isoformat(),
                "payload": payload,
            }
        )

    return events


def main():
    parser = argparse.ArgumentParser(
        description="Seed TemporalLayr with demo execution data."
    )
    parser.add_argument(
        "--url", default="http://localhost:8000", help="Base URL of TemporalLayr Server"
    )
    parser.add_argument("--api-key", default="demo-key", help="API Key")
    parser.add_argument("--tenant-id", default="demo-tenant", help="Tenant ID")
    parser.add_argument(
        "--count", type=int, default=100, help="Number of events to send"
    )

    args = parser.parse_args()

    ingest_url = f"{args.url.rstrip('/')}/v1/ingest"

    print(f"Generating {args.count} events for tenant '{args.tenant_id}'...")
    events = generate_events(args.count)

    payload = {"api_key": args.api_key, "tenant_id": args.tenant_id, "events": events}

    print(f"Sending payload to {ingest_url}...")

    with httpx.Client(timeout=30.0) as client:
        response = client.post(
            ingest_url,
            json=payload,
            headers={
                "X-API-Key": args.api_key,
                "X-Tenant-ID": args.tenant_id,
                "Content-Type": "application/json",
            },
        )

        if response.status_code in (200, 201, 202):
            print("Successfully ingested demo data!")
            print(response.json())
        else:
            print(f"Failed to ingest: [{response.status_code}] {response.text}")


if __name__ == "__main__":
    main()
