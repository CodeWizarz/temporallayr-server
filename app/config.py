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

TEMPORALLAYR_DEMO_API_KEY = os.environ.get("TEMPORALLAYR_DEMO_API_KEY", "demo-key")
TEMPORALLAYR_DEMO_TENANT = os.environ.get("TEMPORALLAYR_DEMO_TENANT", "demo-tenant")

TEMPORALLAYR_API_KEY = os.environ.get("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")
EXPECTED = TEMPORALLAYR_API_KEY
TEMPORALLAYR_DEV_KEYS = os.environ.get("TEMPORALLAYR_DEV_KEYS", "dev-test-key").split(
    ","
)


def log_environment_status():
    """Logs the presence of critical environment variables without leaking secrets."""
    logger.info("--- TEMPORALLAYR ENVIRONMENT STATUS ---")
    vars_to_check = [
        "DATABASE_URL",
        "DATABASE_PUBLIC_URL",
        "PORT",
        "API_KEY",
        "TEMPORALLAYR_DEMO_API_KEY",
        "TEMPORALLAYR_DEMO_TENANT",
        "TEMPORALLAYR_API_KEY",
        "TEMPORALLAYR_DEV_KEYS",
    ]
    for var in vars_to_check:
        val = os.environ.get(var)
        status = "SET (Length: " + str(len(val)) + ")" if val else "NOT SET / DEFAULT"
        logger.info(f"{var}: {status}")
    logger.info("---------------------------------------")


log_environment_status()
