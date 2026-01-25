
# Data Ingestion Pipeline with Docker 

Containerised data pipeline for ingesting web data into a PostgreSQL database.

to see the sql queries for the homework [click here](pipeline/Assignment.md)

## Overview:
-Fetches data from the [NYC TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

-Streams the datasets in chunks into a PostgreSQL database.

-Uses Docker and Docker Compose to orchestrate the environment, with Python dependencies managed via uv.

Infrastructure Components
-PostgreSQL: A containerized database instance.

-pgAdmin: A web-based GUI for database management and visualization.

-pgcli: A command-line interface with auto-completion and syntax highlighting for SQL queries.

#####  Inspect the `ingest_data.py` file to see the script for the ETL logic.

---

## Prerequisites

- Docker  
- Docker Compose  


## Usage


### 1. Clone the top-level repo into your desired folder:
```bash
git clone https://github.com/Olapels/dt_data_engineering_zoomcamp.git
```

Navigate to the pipeline folder
```bash
cd docker_and_terraform
cd pipeline
```

### 2. Start Postgres and pgAdmin

```bash
docker compose up -d
````

This starts the PostgreSQL and pgAdmin containers and exposes the following ports:

* Postgres: `localhost:5432`
* pgAdmin: `localhost:8085`


#### 3. pgAdmin setup

Open your browser and go to:

```
localhost:8085
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
### 4. Build the ingestion image

```bash
docker build -t ny-taxi-ingest .
```

---
### 5. Run ingestion

Docker Compose will automatically create a network for the services based on the top-level folder name.
`'[folder_name]_default'` --> `'pipeline_default'` in this case.

```bash
docker run -it \
  --network=pipeline_default \
  ny-taxi-ingest \
    --pg-host=pgdatabase \
    --pg-user=root \
    --pg-pass=root \
    --pg-db=ny_taxi \
    --year=2021 \
    --month=1
```

## CLI options

you can edit the configuration when creating the ingestion pipeline Docker image through the following cli options
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

### 5. Interact with your data:
 When the ingestion is complete, you can optionally query via pgcli.

 in the same pipeline folder, run:
 ``` bash
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```
and then you get a dedicated cli for interacting with the ingested data.


