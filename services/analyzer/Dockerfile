FROM python:3.11-slim
ARG ANALYZER_NAME=analyzerX
ENV ANALYZER_NAME=${ANALYZER_NAME}
WORKDIR /app
RUN pip install --no-cache-dir grpcio grpcio-tools pymongo graypy requests
COPY server.py /app/server.py
COPY proto/logs.proto /app/proto/logs.proto
RUN python -m grpc_tools.protoc -I/app/proto --python_out=/app --grpc_python_out=/app /app/proto/logs.proto
EXPOSE 50051
CMD ["python", "-u", "server.py"]
