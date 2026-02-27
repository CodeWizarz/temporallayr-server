import os
import logging

logger = logging.getLogger("temporallayr.config")


def get_database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        url = os.environ.get("DATABASE_PUBLIC_URL")
    return url


DATABASE_URL = get_database_url()

if DATABASE_URL:
    logger.info(
        f"Using database URL from environment: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else 'configured'}"
    )
else:
    logger.warning("No DATABASE_URL or DATABASE_PUBLIC_URL provided in environment.")

PORT = int(os.environ.get("PORT", "8000"))

API_KEY = os.environ.get("API_KEY")

TEMPORALLAYR_DEMO_API_KEY = os.environ.get("TEMPORALLAYR_DEMO_API_KEY", "demo-key")
TEMPORALLAYR_DEMO_TENANT = os.environ.get("TEMPORALLAYR_DEMO_TENANT", "demo-tenant")

EXPECTED = os.environ.get("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")
TEMPORALLAYR_DEV_KEYS = os.environ.get("TEMPORALLAYR_DEV_KEYS", "dev-test-key").split(
    ","
)
