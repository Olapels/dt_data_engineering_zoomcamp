# Data Engineering Zoomcamp 2026

This repository contains code and configurations for the Data Engineering Zoomcamp 2026 course. The pipeline folder contains code for building a data pipeline to ingest data into a PostgreSQL database using Docker containers.

## Project Overview

- **Data Source**: NY Taxi and Limousine Commission (TLC) trip data (e.g., yellow taxi data from 2021).
- **Destination**: PostgreSQL database running in a Docker container.
- **Tools**: Python (with pandas, SQLAlchemy, psycopg2), Docker, Docker Compose, uv for dependency management.
- **Pipeline**: Processes CSV data, applies data types and parsing, and loads into Postgres tables.

## Project Structure

- `docker_and_terraform/pipeline/`: Core pipeline code, Docker setup, database configurations, and detailed documentation.
  - Includes `pipeline.py` for data ingestion, `Dockerfile` for containerization, `docker-compose.yml` for orchestration, and test notebooks.

## Getting Started

For detailed setup, dependencies, and running instructions, refer to the [pipeline README](docker_and_terraform/pipeline/README.md).
