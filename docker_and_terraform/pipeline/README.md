## Prerequisites

- Docker and Docker Compose
- Python 3.13+ (for local development)
- `uv` for dependency management

## Setup

### Install Dependencies

Install dependencies using `uv`:

```bash
uv sync
```

#### Check Port Availability

Ensure no other service is running on port 5432:

**Mac:**
```bash
sudo lsof -i :5432
```

**Windows:**
```bash
netstat -ano | findstr :5432
```

**Linux:**
```bash
sudo netstat -tlnp | grep :5432
```

To run PostgreSQL on a different port, update the `-p` flag:
```bash
-p 8000:5432
```

### Application Docker Image

Build the application image (optional: if not using Docker Compose, which builds the image automatically; note: this creates one of two images in your setup; the other is the PostgreSQL image):

```bash
docker build -t test-app:latest -f Dockerfile .
```

## Running

### Docker Compose (Recommended)

Orchestrate both services (PostgreSQL and application) in separate containers using the `docker-compose.yml` file in this directory:

```bash
docker-compose up --build
```

### Separate Containers

1. **PostgreSQL Container** (runs the database; currently active):
   ```bash
   docker run -it --rm \
       -e POSTGRES_USER="root" \
       -e POSTGRES_PASSWORD="root" \
       -e POSTGRES_DB="ny_taxi" \
       -v ny_taxi_postgres_data:/var/lib/postgresql/data \
       -p 5432:5432 \
       postgres:18
   ```

2. **Application Container** (runs your pipeline):
   ```bash
   docker run --network host test-app:latest
   ```

<!-- ### Local Development

Run the pipeline locally:

```bash
uv run python pipeline.py
``` -->
