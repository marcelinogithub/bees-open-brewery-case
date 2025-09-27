# pipeline/silver.py
import os
from pyspark.sql import SparkSession, functions as F
from pipeline.bronze import LAKE_ROOT

"""
This module reads the NDJSON file from the Bronze layer and generates
the Silver layer in Parquet format. It applies schema normalization,
column casting, and partitions the data by country and state to
enable more efficient analytical queries.
"""
def _spark():
    
    if os.name == "nt":
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    return (
        SparkSession.builder
        .appName("silver-breweries")
        .config("spark.sql.session.timeZone", "UTC")
        # --- Windows-friendly ---
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

    # read NDJSON in DataFrame
    df = spark.read.json(input_ndjson)

    
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

    # partition by country and state
    out = silver_root()
    (
        df2
        .repartition(1, "country", "state")  
        .write
        .mode("overwrite")
        .partitionBy("country", "state")
        .parquet(out)
    )

    return out
