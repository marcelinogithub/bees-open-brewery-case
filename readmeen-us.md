# Bees – Open Brewery Case (Data Lake with Medallion Architecture)

## Initial Summary
This project implements a data ingestion, transformation, and aggregation pipeline using the **Open Brewery DB API**, following the **Medallion architecture** (Bronze → Silver → Gold) and orchestrated with **Luigi**.  
The entire solution was built step by step, including **environment setup**, **dependency installation**, **Spark configuration on Windows**, **development of each pipeline layer**, **automated tests**, and **full execution via .bat script**.

## Note:
Spark was used in local mode. For Windows compatibility, `winutils.exe` was configured.

## Environment and Dependencies

### Environment Setup
The project was developed on **Windows 11** using **VS Code** and **PowerShell**.  
A Python virtual environment was created to isolate dependencies.

# Details
This project implements a data ingestion and transformation pipeline using the Open Brewery DB API.  
It follows the Medallion architecture (Bronze → Silver → Gold), with paginated extraction, transformation into partitioned Parquet files, and analytical aggregation.  
The orchestration is done with Luigi and includes a Data Quality validation step at the end.

## Pipelines

### Bronze – Extraction and Raw Storage
- Script: `pipeline/bronze.py`
- Consumes the API with pagination and retries.
- Writes data as NDJSON files into `datalake/bronze`.
- Generates a `_LATEST.flag` file with the path of the latest execution.

### Silver – Transformation and Standardization
- Script: `pipeline/silver.py`
- Converts NDJSON into Parquet, partitioned by `country` and `state`.
- Explicitly casts column types and normalizes values to uppercase.
- Saves files to `datalake/silver` and generates `_SUCCESS`.

### Gold – Analytical Aggregation
- Script: `pipeline/gold.py`
- Aggregates brewery information by `country`, `state`, and `brewery_type`.
- Saves results to `datalake/gold/breweries_by_type_and_location`.

### Data Quality
- Script: `pipeline/dq.py`
- Validates that the Gold result contains rows and required columns.
- Generates `_SUCCESS_DQ`. If validation fails, execution is stopped.

## Orchestration with Luigi
- Main script: `pipeline/pipeline_luigi.py`
- Tasks:
  - `BronzeTask` → generates Bronze
  - `SilverTask` → depends on Bronze
  - `GoldTask` → depends on Silver
  - `DQTask` → depends on Gold
  - `RunAll` → runs the entire DAG

## Execution and Reprocessing
- To simplify execution and allow scheduling, a script `scripts/run_pipeline.bat` was created.

- This .bat file allows:
- Full reprocessing every time it is executed.
- Scheduling through Windows Task Scheduler for automatic execution.

## Automated Tests

- Tests were created to ensure pipeline functionality:
- File: `tests/test_extract.py`
- Function: validates the return of `paginate_breweries`.

## Technical Decisions

- Spark Local Mode was used to simulate a distributed environment without requiring a cluster.
- API retries with exponential backoff for resilience.
- Partitioning in Silver layer to optimize data reading.
- Luigi chosen for its simplicity and ability to run locally without cloud dependencies.
- Control flags (`_SUCCESS`, `_LATEST.flag`) to ensure idempotent execution.

## Final Project Structure

bees-open-brewery-case/
├─ pipeline/
│  ├─ extract.py
│  ├─ bronze.py
│  ├─ silver.py
│  ├─ gold.py
│  ├─ dq.py
│  └─ pipeline_luigi.py
├─ scripts/
│  └─ run_pipeline.bat
├─ tests/
│  ├─ conftest.py
│  └─ test_extract.py
├─ datalake/
├─ requirements.txt
└─ README.md
