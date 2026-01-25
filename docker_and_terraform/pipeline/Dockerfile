#python version to run for container
FROM python:3.13.11-slim

#copying uv package manager from official source
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


#copying virtual env already set up when packaging code
WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

#copy package dependency config files
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

#copy file from folder
COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]