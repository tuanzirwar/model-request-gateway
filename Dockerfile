FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 GATEWAY_CONFIG=/app/deploy/gateway.docker.yaml
WORKDIR /app
COPY pyproject.toml README.md MANIFEST.in ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --uid 10001 --create-home gateway
COPY alembic.ini ./
COPY migrations ./migrations
COPY scripts ./scripts
COPY deploy ./deploy
USER gateway
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2)"
CMD ["python", "-m", "uvicorn", "model_gateway.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--limit-concurrency", "128", "--timeout-graceful-shutdown", "20", "--log-level", "warning"]
