import os
import uvicorn

if __name__ == "__main__":
    print("--- RAILWAY ENV DUMP ---")
    for k, v in os.environ.items():
        if "KEY" in k or "SECRET" in k or "PASSWORD" in k or "URL" in k:
            print(f"{k}: ***HIDDEN***")
        else:
            print(f"{k}: {v}")
    print("------------------------")

    port = int(os.environ.get("PORT", "8000"))
    print(f"Starting server on port {port}")
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, workers=1, log_level="info")
