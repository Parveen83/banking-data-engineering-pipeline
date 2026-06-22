# Build from the official Apache Airflow image used by docker-compose.
FROM apache/airflow:3.2.2-python3.12

# Copy Python dependencies into the image.
COPY requirements.txt /requirements.txt

# Install project dependencies as the airflow user.
RUN pip install --no-cache-dir -r /requirements.txt