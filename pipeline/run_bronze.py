# pipeline/run_bronze.py
from pipeline.extract import paginate_breweries
from pipeline.bronze import write_bronze_ndjson

if __name__ == "__main__":
    out = write_bronze_ndjson(paginate_breweries())
    print("Bronze salvo em:", out)
