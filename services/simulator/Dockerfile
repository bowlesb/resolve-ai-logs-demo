FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx requests
RUN pip install --no-cache-dir httpx opentelemetry-distro opentelemetry-exporter-otlp opentelemetry-instrumentation-httpx
COPY sender.py /app/sender.py
CMD ["opentelemetry-instrument","--traces_exporter","otlp","--metrics_exporter","none", "python", "-u", "sender.py"]
