# pipeline/bronze.py
import os, json
from datetime import datetime

"""
This module implements the Bronze layer of the Medallion architecture.
It is responsible for storing the raw data extracted from the API in
NDJSON format, creating a unique folder per execution to ensure
data lineage and reproducibility.

"""
LAKE_ROOT = os.getenv("BEES_LAKE_ROOT", os.path.abspath("./datalake"))

def bronze_run_paths():
    """Creates the folder structure for the current pipeline run and
    returns both the base path and the NDJSON output file path.
"""
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(LAKE_ROOT, "bronze", f"run_date={run_date}", f"run_ts={run_ts}")
    os.makedirs(base, exist_ok=True)
    return base, os.path.join(base, "breweries.ndjson")

def write_bronze_ndjson(pages_iterable):
    """
    Writes the raw API data into a single NDJSON file (one JSON object per line).
    Returns the full path of the generated file.
    """
    base, out_file = bronze_run_paths()
    with open(out_file, "w", encoding="utf-8") as f:
        for page in pages_iterable:
            for row in page:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out_file
