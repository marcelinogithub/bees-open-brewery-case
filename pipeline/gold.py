# pipeline/gold.py
import os
from pyspark.sql import SparkSession, functions as F


# Read the silver (Parquet) and agregate:
# count of beweries per (country, state, brewery_type)

def _spark():
    if os.name == "nt":
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    return (
        SparkSession.builder
        .appName("gold-breweries")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.hadoop.home.dir", os.environ.get("HADOOP_HOME", r"C:\hadoop"))
        .getOrCreate()
    )

def gold_root(lake_root: str) -> str:
    return os.path.join(lake_root, "gold", "breweries_by_type_and_location")

def run(silver_root: str, lake_root: str):
    spark = _spark()

    df = spark.read.parquet(silver_root)

    agg = (
        df.groupBy("country", "state", "brewery_type")
          .agg(F.count("*").alias("brewery_count"))
    )

    out = gold_root(lake_root)
    (
        agg.repartition(1)     
           .write
           .mode("overwrite")
           .parquet(out)
    )
    return out
