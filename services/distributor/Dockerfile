FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn gunicorn grpcio grpcio-tools pymongo pydantic[dotenv] httpx
COPY app /app/app
ENV PYTHONPATH=/app/app:/app
COPY proto/logs.proto /app/proto/logs.proto
RUN python -m grpc_tools.protoc -I/app/proto --python_out=/app/app --grpc_python_out=/app/app /app/proto/logs.proto
EXPOSE 8000
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "-b", "0.0.0.0:8000", "app.main:app"]
