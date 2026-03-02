"""dlt pipeline to ingest NYC taxi trip data from custom API."""

import dlt
import requests


@dlt.resource(write_disposition="replace")
def taxi_trips():
    """Fetch NYC taxi trip data with pagination."""
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    
    while True:
        response = requests.get(base_url, params={"page": page})
        response.raise_for_status()
        data = response.json()
        
        if not data:
            break
            
        yield data
        page += 1


@dlt.source
def nyc_taxi_api_source():
    """NYC Taxi API source."""
    return taxi_trips()


pipeline = dlt.pipeline(
    pipeline_name='nyc_taxi_pipeline',
    destination='duckdb',
    dataset_name='nyc_taxi_data',
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_api_source())
    print(load_info)
