import requests

response = requests.get("https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api?page=1")
data = response.json()
print(f"Type: {type(data)}")
print(f"Length: {len(data) if isinstance(data, list) else 'N/A'}")
print(f"First record: {data[0] if isinstance(data, list) and len(data) > 0 else data}")
