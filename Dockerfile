FROM python:3.12-slim

# Install system deps for duckdb + duckdb UI
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-warm duckdb extensions
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL ducklake'); c.execute('LOAD ducklake'); c.close()"

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p /app/data/catalog /app/data/parquet \
    /app/mesh_simulator/inbox /app/mesh_simulator/processing \
    /app/mesh_simulator/archive /app/mesh_simulator/logs

# Default environment
ENV DATA_DIR=/app/data \
    MESH_DIR=/app/mesh_simulator \
    PORT=8765 \
    PYTHONUNBUFFERED=1

EXPOSE 8765

CMD ["python", "start.py"]