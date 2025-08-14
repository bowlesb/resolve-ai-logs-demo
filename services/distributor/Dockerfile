FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir \
    pytest \
    fastapi \
    uvicorn \
    gunicorn \
    grpcio \
    grpcio-tools \
    protobuf \
    pymongo \
    pydantic \
    httpx \
    prometheus-client \
    opentelemetry-distro \
    opentelemetry-exporter-otlp \
    opentelemetry-instrumentation-fastapi \
    opentelemetry-instrumentation-grpc \
    opentelemetry-instrumentation-httpx
    
COPY app /app/app
ENV PYTHONPATH=/app/app:/app
COPY proto/logs.proto /app/proto/logs.proto
RUN python -m grpc_tools.protoc -I/app/proto --python_out=/app/app --grpc_python_out=/app/app /app/proto/logs.proto
EXPOSE 8000

CMD ["opentelemetry-instrument","--traces_exporter","otlp","--metrics_exporter","none", "python","-m","uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
