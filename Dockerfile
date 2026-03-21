FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD gunicorn -w 4 -b 0.0.0.0:${PORT:-8080} muavin_api:app
