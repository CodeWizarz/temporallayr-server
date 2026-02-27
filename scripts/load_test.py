import asyncio
import httpx
import time
import argparse

HEADERS = {
    "X-API-Key": "demo-key",
    "X-Tenant-ID": "demo-tenant",
    "Content-Type": "application/json",
}


async def hit_endpoint(client, url, method="GET", json=None):
    try:
        start = time.time()
        if method == "GET":
            res = await client.get(url, headers=HEADERS)
        else:
            res = await client.post(url, headers=HEADERS, json=json)
        return {"status": res.status_code, "duration": time.time() - start}
    except Exception as e:
        return {"status": "error", "duration": time.time() - start, "error": str(e)}


async def load_test(base_url, duration=300, concurrency=50):
    print(
        f"Starting load test against {base_url} for {duration} seconds with {concurrency} ops/sec..."
    )
    async with httpx.AsyncClient(timeout=10.0) as client:
        start_time = time.time()

        health_errors = 0
        ingest_errors = 0

        while time.time() - start_time < duration:
            tasks = []
            for _ in range(concurrency):
                tasks.append(
                    asyncio.create_task(hit_endpoint(client, f"{base_url}/health"))
                )
                payload = {
                    "api_key": "demo-key",
                    "events": [{"type": "load_test"}],
                    "tenant_id": "demo-tenant",
                }
                tasks.append(
                    asyncio.create_task(
                        hit_endpoint(client, f"{base_url}/v1/ingest", "POST", payload)
                    )
                )

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Simple stats
            errors = [
                r
                for r in results
                if isinstance(r, dict) and r["status"] not in (200, 202)
            ]
            if errors:
                print(f"[!] Burst saw {len(errors)} errors/timeouts.")
                for e in errors[:3]:
                    print(e)

            await asyncio.sleep(1)

        print("Load test complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--duration", type=int, default=300)
    args = parser.parse_args()
    asyncio.run(load_test(args.url, args.duration))
