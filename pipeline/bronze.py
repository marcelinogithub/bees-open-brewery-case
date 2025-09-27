# pipeline/bronze.py
import os, json
from datetime import datetime

# Este step grava os dados crus (raw) da API em formato NDJSON no data lake
# criando uma pasta para cada data/hora de execução (linhagem de dados).

LAKE_ROOT = os.getenv("BEES_LAKE_ROOT", os.path.abspath("./datalake"))

def bronze_run_paths():
    """Cria estrutura de pastas para armazenar o arquivo raw deste run."""
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(LAKE_ROOT, "bronze", f"run_date={run_date}", f"run_ts={run_ts}")
    os.makedirs(base, exist_ok=True)
    return base, os.path.join(base, "breweries.ndjson")

def write_bronze_ndjson(pages_iterable):
    """Escreve os dados em NDJSON (um JSON por linha)."""
    base, out_file = bronze_run_paths()
    with open(out_file, "w", encoding="utf-8") as f:
        for page in pages_iterable:
            for row in page:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out_file
