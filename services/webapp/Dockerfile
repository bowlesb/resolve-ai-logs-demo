FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN pip install --no-cache-dir dash==2.16.1 plotly==5.* pymongo==4.* requests==2.*

# Put the app *as a package* at /app/app
COPY app /app/app

EXPOSE 8080
CMD ["python", "-m", "app.app"]
