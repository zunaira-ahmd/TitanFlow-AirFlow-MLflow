# Custom Airflow image with ML packages pre-installed
# ─────────────────────────────────────────────────────
# Build command (run automatically by docker compose):
#   docker compose build
# This bakes all required packages into the image so
# they are available inside every Airflow worker task.

FROM apache/airflow:2.9.2

# Install ML packages at build time (reliable, no startup delay)
RUN pip install --no-cache-dir \
    mlflow==2.11.1 \
    scikit-learn==1.4.2 \
    pandas==2.1.4 \
    numpy==1.26.4 \
    psycopg2-binary==2.9.9
