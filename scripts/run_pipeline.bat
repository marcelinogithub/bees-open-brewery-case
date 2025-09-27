@echo off
REM === paths do projeto / venv ===
set "PROJECT_DIR=C:\Users\marcelino.sobrinho\bees-open-brewery-case\bees-open-brewery-case"
set "VENV_PY=%PROJECT_DIR%\.venv\Scripts\python.exe"

REM === Spark no Windows (Hadoop shim) ===
set "HADOOP_HOME=C:\hadoop"
set "PATH=C:\hadoop\bin;%PATH%"

REM === entra na pasta do projeto ===
cd /d "%PROJECT_DIR%"

REM === limpa flags para forçar reprocessamento ===
del /q "%PROJECT_DIR%\datalake\bronze\_LATEST.flag" 2>nul
del /q "%PROJECT_DIR%\datalake\silver\_SUCCESS" 2>nul
del /q "%PROJECT_DIR%\datalake\gold\breweries_by_type_and_location\_SUCCESS" 2>nul
del /q "%PROJECT_DIR%\datalake\gold\breweries_by_type_and_location\_SUCCESS_DQ" 2>nul

REM === executa o Luigi ===
"%VENV_PY%" -m luigi --module pipeline.pipeline_luigi RunAll --local-scheduler --log-level INFO
