# pipeline/dq.py
import os
from pyspark.sql import SparkSession

# DQ simples para o Gold: garante linhas > 0, colunas obrigatórias
# e ausência de brewery_count nulo/negativo. Config "Windows-friendly".

def _spark():
    if os.name == "nt":
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
    return (
        SparkSession.builder
        .appName("dq-breweries")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.hadoop.home.dir", os.environ.get("HADOOP_HOME", r"C:\hadoop"))
        .getOrCreate()
    )

def gold_path(lake_root: str) -> str:
    return os.path.join(lake_root, "gold", "breweries_by_type_and_location")

def run(lake_root: str, min_rows: int = 1):
    spark = _spark()
    path = gold_path(lake_root)
    df = spark.read.parquet(path)

    rows = df.count()
    if rows < min_rows:
        raise AssertionError(f"[DQ] Gold tem {rows} linhas (min esperado = {min_rows}).")

    required_cols = ["country", "state", "brewery_type", "brewery_count"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise AssertionError(f"[DQ] Colunas ausentes no Gold: {missing}")

    if df.filter("brewery_count is null OR brewery_count < 0").count() > 0:
        raise AssertionError("[DQ] brewery_count nulo ou negativo encontrado.")

    return {"rows": rows}
