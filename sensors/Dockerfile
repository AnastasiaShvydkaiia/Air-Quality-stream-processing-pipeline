FROM python:3.11-slim

WORKDIR /sensors

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script + CSV
COPY . .

# Run producer with unbuffered output
CMD ["python", "-u", "stream_producing.py"]
