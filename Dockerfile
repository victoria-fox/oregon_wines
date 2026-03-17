FROM python:3.12-slim

# GDAL and geospatial system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY prism_poc.py .
COPY oregon_avas.geojson .

# Railway injects DATABASE_URL automatically.
# Override any setting via Railway service variables.
ENV AVA_FILE=oregon_avas.geojson
ENV NAME_COL=Name
ENV START=2020-01
ENV END=2022-12
ENV CACHE_DIR=/cache

# Create cache dir
RUN mkdir -p /cache

CMD ["python", "prism_poc.py"]
