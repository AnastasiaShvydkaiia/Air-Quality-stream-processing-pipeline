FROM python:3.10-slim

WORKDIR /metrics_exporter

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY metrics_exporter.py .

ENV MONGO_URI=mongodb://mongo:27017/
ENV PYTHONUNBUFFERED=1

CMD ["python", "metrics_exporter.py"]
