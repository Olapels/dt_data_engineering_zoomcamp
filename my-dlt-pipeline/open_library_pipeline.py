"""dlt pipeline to ingest data from Open Library API."""

import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from Open Library REST API endpoints."""
    config = {
        "client": {
            "base_url": "https://openlibrary.org/api/",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "books",
                    "params": {
                        "bibkeys": "ISBN:0451526538",
                        "format": "json",
                        "jscmd": "data",
                    },
                    "data_selector": "$",
                    "paginator": "single_page",
                },
            },
        ],
    }

    return rest_api_source(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
