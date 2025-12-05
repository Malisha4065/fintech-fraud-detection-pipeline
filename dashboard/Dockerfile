FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    streamlit==1.29.0 \
    pandas==2.1.3 \
    psycopg2-binary==2.9.9 \
    pyarrow==14.0.1 \
    plotly==5.18.0 \
    sqlalchemy==2.0.23

# Copy application
COPY app.py /app/app.py

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
