# Bees – Open Brewery Case (Data Lake Medallion)

## Visão geral
Pipeline orquestrada (Luigi) que:
- Extrai da Open Brewery DB (Bronze, NDJSON)
- Transforma com Spark (Silver, Parquet particionado por `country/state`)
- Agrega com Spark (Gold: contagem por `country/state/brewery_type`)
- Valida DQ (linha > 0, schema mínimo)
- Agendada opcionalmente via Windows Task Scheduler

## Arquitetura
- **Bronze**: JSON bruto (NDJSON) com linhagem por `run_date` e `run_ts`
- **Silver**: Parquet colunar, padronização de tipos/nomes e particionamento
- **Gold**: agregação `brewery_count` por `country/state/brewery_type`
- **Orquestração**: Luigi (dependências, retries, idempotência via flags)

## Como rodar localmente (Windows)
1. Python 3.13 + venv
2. `pip install -r requirements.txt`
3. Spark no Windows:
   - `C:\hadoop\bin\winutils.exe` e `C:\hadoop\bin\hadoop.dll`
4. Executar uma vez:
   ```bash
   python -m pipeline.run_bronze
   python -m pipeline.run_silver
   python -m pipeline.run_gold
   python -m pipeline.check_gold
