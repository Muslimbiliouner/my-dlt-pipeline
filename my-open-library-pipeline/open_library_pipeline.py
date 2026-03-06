import dlt
from dlt.sources.helpers.rest_client import RESTClient

@dlt.resource(name="books", write_disposition="replace")
def open_library_books():
    client = RESTClient(base_url="https://openlibrary.org")
    
    bibkeys = [
        "ISBN:0451526538",
        "ISBN:0743273567", 
        "ISBN:0743482367",
        "ISBN:0195153448",
        "ISBN:0596007973",
    ]
    
    response = client.get(
        "/api/books",
        params={
            "bibkeys": ",".join(bibkeys),
            "format": "json",
            "jscmd": "data",
        }
    )
    
    data = response.json()
    for key, value in data.items():
        yield {"bibkey": key, **value}

pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    dataset_name="open_library_data",
    progress="log",
)

if __name__ == "__main__":
    load_info = pipeline.run(open_library_books())
    print(load_info)
