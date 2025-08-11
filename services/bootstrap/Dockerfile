FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY bootstrap.py /app/bootstrap.py
CMD ["python", "-u", "bootstrap.py"]
