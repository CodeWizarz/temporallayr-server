import os
import uvicorn

if __name__ == "__main__":
    print("--- TEMPORALLAYR RESILIENT STARTUP ---")

    # Dump environment (redacted)
    for k, v in os.environ.items():
        if any(
            secret in k.lower() for secret in ["key", "pass", "secret", "url", "token"]
        ):
            print(f"{k}: [REDACTED]")
        else:
            print(f"{k}: {v}")

    port = int(os.environ.get("PORT", "8000"))

    app = None
    startup_error = None
    try:
        print("Pre-loading app.main...")
        from app.main import app as real_app

        app = real_app
        print("Application loaded successfully.")
    except Exception as e:
        startup_error = str(e)
        print("!!! CRITICAL STARTUP ERROR !!!")
        print(f"Error: {startup_error}")
        import traceback

        traceback.print_exc()

        # Start dummy resilience app
        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/health")
        async def health():
            return {
                "status": "ok",
                "mode": "emergency_degraded",
                "error": startup_error,
            }

        @app.get("/{rest_of_path:path}")
        async def catch_all(rest_of_path: str):
            return {
                "ok": False,
                "error": "Server in emergency degraded mode due to startup crash",
                "details": startup_error,
            }

    print(f"Invoking uvicorn on 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info", access_log=True)
