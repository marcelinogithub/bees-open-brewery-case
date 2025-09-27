# pipeline/check_gold.py
import os
from pyspark.sql import SparkSession

def _spark():
    return (
        SparkSession.builder
        .appName("check-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

if __name__ == "__main__":
    lake_root = os.path.abspath("./datalake")
    gold_path = os.path.join(lake_root, "gold", "breweries_by_type_and_location")
    spark = _spark()
    df = spark.read.parquet(gold_path)
    df.show(10, truncate=False)
    print(f"Linhas no Gold: {df.count()}")
