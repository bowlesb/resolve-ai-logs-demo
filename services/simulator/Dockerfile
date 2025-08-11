FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx requests
COPY sender.py /app/sender.py
CMD ["python", "-u", "sender.py"]
