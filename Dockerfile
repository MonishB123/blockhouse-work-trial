FROM python:3.8-slim

workdir /app

COPY . /app
RUN pip install --no-cache-dir -r requirements.txt


