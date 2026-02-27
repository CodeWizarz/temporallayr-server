import os
import logging

logger = logging.getLogger("temporallayr.config")


# Ensure required structure but never crash if missing
def get_database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        url = os.environ.get("DATABASE_PUBLIC_URL")
    return url


DATABASE_URL = get_database_url()

if DATABASE_URL:
    logger.info(
        f"Database URL configured (target: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else 'local'})"
    )
else:
    logger.warning(
        "No DATABASE_URL or DATABASE_PUBLIC_URL provided. App will degrade gracefully."
    )

PORT = int(os.environ.get("PORT", "8000"))
logger.info(f"PORT configured: {PORT}")

API_KEY = os.environ.get("API_KEY")
if API_KEY:
    logger.info("API_KEY is configured.")
else:
    logger.warning("API_KEY not configured. Local dev mode fallback enabled.")

TEMPORALLAYR_DEMO_API_KEY = os.environ.get("TEMPORALLAYR_DEMO_API_KEY", "demo-key")
TEMPORALLAYR_DEMO_TENANT = os.environ.get("TEMPORALLAYR_DEMO_TENANT", "demo-tenant")
logger.info("Demo tenant/keys initialized.")

TEMPORALLAYR_API_KEY = os.environ.get("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")
EXPECTED = TEMPORALLAYR_API_KEY
TEMPORALLAYR_DEV_KEYS = os.environ.get("TEMPORALLAYR_DEV_KEYS", "dev-test-key").split(
    ","
)
logger.info(f"Loaded {len(TEMPORALLAYR_DEV_KEYS)} dev keys.")
