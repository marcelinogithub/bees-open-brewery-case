# pipeline/run_gold.py
import os
from pipeline.gold import run

if __name__ == "__main__":
    lake_root = os.path.abspath("./datalake")
    silver_root = os.path.join(lake_root, "silver")

    if not os.path.exists(silver_root):
        raise FileNotFoundError("Silver não encontrado. Rode a fase Silver antes.")

    out_dir = run(silver_root, lake_root)
    print("Gold gerado em:", out_dir)
