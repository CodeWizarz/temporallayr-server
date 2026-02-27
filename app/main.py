from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins, but can be locked down to dashboard URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  # This allows X-API-Key, X-Tenant-ID, Content-Type, etc.
)

@app.get("/health")
def health_check():
    return {"status": "ok"}
