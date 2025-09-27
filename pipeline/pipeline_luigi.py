import os
import glob
import luigi
from luigi.util import requires

from pipeline.extract import paginate_breweries
from pipeline.bronze import write_bronze_ndjson, LAKE_ROOT
from pipeline.silver import run as spark_silver_run
from pipeline.gold import run as spark_gold_run
from pipeline.dq import run as dq_run


class BronzeTask(luigi.Task):
    """Extrai da API e grava NDJSON na Bronze."""
    retries = 2

    def output(self):
        return luigi.LocalTarget(os.path.join(LAKE_ROOT, "bronze", "_LATEST.flag"))

    def run(self):
        ndjson = write_bronze_ndjson(paginate_breweries())
        with self.output().open("w") as f:
            f.write(ndjson)

@requires(BronzeTask)
class SilverTask(luigi.Task):
    """Transforma Bronze em Silver (Parquet particionado)."""
    retries = 2

    def output(self):
        return luigi.LocalTarget(os.path.join(LAKE_ROOT, "silver", "_SUCCESS"))

    def run(self):
        with self.input().open("r") as f:
            ndjson = f.read().strip()
        out_dir = spark_silver_run(ndjson)
        with self.output().open("w") as f:
            f.write(out_dir)

@requires(SilverTask)
class GoldTask(luigi.Task):
    """Agrega Silver para gerar Gold."""
    retries = 2

    def output(self):
        return luigi.LocalTarget(os.path.join(LAKE_ROOT, "gold", "breweries_by_type_and_location", "_SUCCESS"))

    def run(self):
        silver_root = os.path.join(LAKE_ROOT, "silver")
        out = spark_gold_run(silver_root, LAKE_ROOT)
        with self.output().open("w") as f:
            f.write(out)

@requires(GoldTask)
class DQTask(luigi.Task):
    """Valida o Gold. Falha se não houver linhas ou schema suspeito."""
    retries = 1

    def output(self):
        return luigi.LocalTarget(os.path.join(LAKE_ROOT, "gold", "breweries_by_type_and_location", "_SUCCESS_DQ"))

    def run(self):
        res = dq_run(LAKE_ROOT, min_rows=1)
        with self.output().open("w") as f:
            f.write(str(res))

class RunAll(luigi.WrapperTask):
    def requires(self):
        return DQTask()

