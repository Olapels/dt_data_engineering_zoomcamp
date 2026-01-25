
# NYC Taxi Data Ingestion Pipeline

Containerised data pipeline for ingesting web data into a PostgreSQL database.

---

## Overview

This pipeline:

- Downloads data from a remote source  
- Streams the data in chunks using Pandas  
- Creates and loads a PostgreSQL table via SQLAlchemy  
- Provides pgAdmin for GUI-based interaction (optional)  
- Supports pgcli for CLI-based database access  
- Is fully containerised with Docker and packaged using `uv`

---

## Prerequisites

- Docker  
- Docker Compose  

---

## Usage

### 1. Start Postgres and pgAdmin

```bash
docker compose up -d
````

This starts the PostgreSQL and pgAdmin containers and exposes the following ports:

* Postgres: `localhost:5432`
* pgAdmin: `http://localhost:8085`

You can use pgAdmin to visually inspect the database, or pgcli if you prefer the CLI.

#### pgAdmin setup

Open your browser and go to:

```
http://localhost:8085
```

Login credentials:

* Email: `admin@admin.com`
* Password: `root`

Register a new server:

* Right-click **Servers** → **Register** → **Server**

**General tab**

* Name: `Local Docker`

**Connection tab**

* Host: `pgdatabase` (container name)
* Port: `5432`
* Username: `root`
* Password: `root`

> Ensure no other services are running on these ports. You can change ports in `docker-compose.yml` if needed.

#### Database credentials

* User: `root`
* Password: `root`
* Database: `ny_taxi`

---

### 2. Build the ingestion image

```bash
docker build -t ny-taxi-ingest .
```

---

### 3. Check Docker Compose network

```bash
docker network ls
```

The network name is what you would use as the `host` when running the ingestion container.

---

### 4. Run ingestion

```bash
docker run --rm --network="host" ny-taxi-ingest \
  --year 2021 \
  --month 1
```

This loads data into the `yellow_taxi_data` table.

---

## CLI Options

```
--pg-user        Postgres user (default: root)
--pg-pass        Postgres password (default: root)
--pg-host        Postgres host (default: localhost)
--pg-port        Postgres port (default: 5432)
--pg-db          Database name (default: ny_taxi)
--year           Data year (default: 2021)
--month          Data month (default: 1)
--target-table   Target table (default: yellow_taxi_data)
--chunksize      CSV chunk size (default: 100000)
```

---

## Notes

* Table schema is created on the first chunk & subsequent chunks are appended


