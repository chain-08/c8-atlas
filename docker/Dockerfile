FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tzdata && rm -rf /var/lib/apt/lists/*

# copy project files (context must be repo root)
COPY pyproject.toml /app/pyproject.toml
COPY src /app/src

ENV PYTHONPATH=/app/src
RUN pip install --no-cache-dir -e .

EXPOSE 8087
CMD ["uvicorn", "atlas.app:app", "--host", "0.0.0.0", "--port", "8087"]
