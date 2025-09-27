# pipeline/silver.py
import os
from pyspark.sql import SparkSession, functions as F
from pipeline.bronze import LAKE_ROOT

# Este módulo lê o arquivo NDJSON da camada bronze e gera a camada silver
# em formato Parquet, com as colunas normalizadas e particionadas por país/estado.

def _spark():
    # Garante variáveis no processo (importante quando orquestrado)
    if os.name == "nt":
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    return (
        SparkSession.builder
        .appName("silver-breweries")
        .config("spark.sql.session.timeZone", "UTC")
        # --- Windows-friendly: evita uso da lib nativa do Hadoop ---
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.hadoop.home.dir", os.environ.get("HADOOP_HOME", r"C:\hadoop"))
        .getOrCreate()
    )

def silver_root():
    """Retorna o caminho da pasta silver no data lake."""
    return os.path.join(LAKE_ROOT, "silver")

def run(input_ndjson: str):
    """Processa o arquivo NDJSON e grava em formato parquet particionado."""
    spark = _spark()

    # Lê o NDJSON em DataFrame
    df = spark.read.json(input_ndjson)

    # Seleciona e normaliza colunas
    df2 = (
        df.select(
            F.col("id").cast("string"),
            F.col("name").cast("string"),
            F.col("brewery_type").alias("brewery_type"),
            F.col("city"),
            F.upper(F.col("state")).alias("state"),
            F.upper(F.col("country")).alias("country"),
            F.col("postal_code"),
            F.col("longitude"),
            F.col("latitude"),
            F.col("website_url"),
            F.col("phone")
        )
        .withColumn("brewery_type", F.lower(F.col("brewery_type")))
    )

    # Escreve particionado por country/state
    out = silver_root()
    (
        df2
        .repartition(1, "country", "state")  # junta registros por partição para demos pequenas
        .write
        .mode("overwrite")
        .partitionBy("country", "state")
        .parquet(out)
    )

    return out
