FROM python:3.14-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/local /usr/local
COPY . .

ENV PYTHONUNBUFFERED=1

RUN groupadd -r mux && useradd -r -g mux -d /app -s /sbin/nologin mux \
    && chown -R mux:mux /app

USER mux

EXPOSE 4404

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import socket; socket.create_connection(('127.0.0.1', 4404), timeout=3).close()"

ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "src.main", "--config", "configs/config.yaml"]
