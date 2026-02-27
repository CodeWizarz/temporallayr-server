FROM python:3.11-slim

WORKDIR /app

# Install native dependencies cleanly
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Export specific package requirements natively securing dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Securely copy routing modules over explicitly mapping /app structures
COPY ./app /app/app

# Drop root privileges natively mapped under dynamic users
RUN useradd -m temporallayr
USER temporallayr

# Expose standard production runtime ports mapping container registries natively
EXPOSE 8000

# Exec entrypoint invoking multithreaded fast uvicorn workers natively
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --log-level info
