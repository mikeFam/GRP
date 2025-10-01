# Airflow Any-Source Starter

Spin up a local Airflow stack (CeleryExecutor) and run data pipelines that ingest from multiple sources
(HTTP/JSON APIs, simple web pages) and land data to a local DuckDB "warehouse" for personal analytics.

## Quick start
1) Install Docker Desktop.
2) Clone/unzip this folder.
3) In a terminal inside the project folder:
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```
4) Open Airflow UI at http://localhost:8080 (user/pass in `.env`).

## Example DAGs
- `anysource_ingest.py`: Reads `dags/config/sources.yaml`, fetches data in parallel, stores raw JSON/HTML under `data/raw/{source_id}/{ds}/`,
  then normalizes into DuckDB tables in `data/warehouse/data.duckdb`.

## Add a new source
- Edit `dags/config/sources.yaml` and add another block with `type`, `url` and optional headers/params.
- For HTML scraping, provide a `selector` to extract records (example uses Hacker News front page).
- For authenticated APIs, set env vars in `.env` (e.g., `GITHUB_TOKEN`) and reference them via `${ENV:VAR_NAME}` in YAML.

## Notes
- This is intentionally lightweight. For heavier scraping (JS sites), add Playwright or Selenium to the image and run a headless browser worker.
- Data lives under `./data`. DuckDB file is portable.
- For S3/MinIO, add the `apache-airflow-providers-amazon` package and swap `RAW_DATA_DIR` / `WAREHOUSE_DIR` writes to S3 paths.

