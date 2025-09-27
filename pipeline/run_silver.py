# pipeline/run_silver.py
import glob
from pipeline.silver import run
import os

if __name__ == "__main__":
    # localiza o NDJSON mais recente do bronze
    ndjson_files = glob.glob("datalake/bronze/run_date=*/run_ts=*/breweries.ndjson")
    if not ndjson_files:
        raise FileNotFoundError("Nenhum NDJSON encontrado. Rode o bronze primeiro.")
    latest = max(ndjson_files, key=os.path.getmtime)

    out_dir = run(latest)
    print("Silver gerado em:", out_dir)
