FROM apache/airflow:3.0.6

# Switch to root only if you need system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential libpq-dev gcc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user before pip install
USER airflow

# Copy your Python dependencies file
COPY requirements.txt .

# Install all Python dependencies (includes duckdb, etc.)
RUN pip install --no-cache-dir -r requirements.txt
