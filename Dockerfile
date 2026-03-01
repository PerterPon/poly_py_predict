FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    CUDA_VISIBLE_DEVICES=-1 \
    TF_CPP_MIN_LOG_LEVEL=2

WORKDIR /app

# libgomp required by LightGBM (OpenMP)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Required by the Setup Wizard at runtime (setup_save reads /app/.env.example)
COPY .env.example /app/.env.example
COPY VERSION /app/VERSION

COPY src /app/src
COPY templates /app/templates
COPY static /app/static

ENV PYTHONPATH=/app/src

EXPOSE 8601 8700

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8601/health').read()"

CMD ["python", "src/dashboard.py"]
