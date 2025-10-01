from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pydantic import BaseModel, Field

RAW_DATA_DIR = os.environ.get("RAW_DATA_DIR", "/opt/airflow/data/raw")
WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "/opt/airflow/data/warehouse")
CONFIG_PATH = "/opt/airflow/dags/config/sources.yaml"

class Source(BaseModel):
    id: str
    type: str  # "http_json" | "html_list"
    url: str
    params: Dict[str, Any] | None = None
    headers: Dict[str, str] | None = None
    json_path: str | None = None
    selector: str | None = None  # for html_list

def load_config() -> List[Source]:
    import yaml, os, re
    from plugins.utils import expand_env_template

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        raw = f.read()

    # Replace ${ENV:VAR} placeholders
    raw = expand_env_template(raw)
    cfg = yaml.safe_load(raw) or {}
    sources = cfg.get("sources", [])
    return [Source(**s) for s in sources]

def http_get(url: str, params: dict | None, headers: dict | None) -> bytes:
    # Respect proxies if set via env
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        r = client.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.content

def parse_http_json(content: bytes, json_path: str | None) -> pd.DataFrame:
    data = json.loads(content.decode("utf-8"))
    if json_path:
        # support dot-path single level for simplicity
        data = data.get(json_path, data)
    return pd.json_normalize(data)

def parse_html_list(content: bytes, selector: str) -> pd.DataFrame:
    soup = BeautifulSoup(content, "html.parser")
    items = []
    for a in soup.select(selector):
        href = a.get("href")
        text = a.get_text(strip=True)
        items.append({"title": text, "url": href})
    return pd.DataFrame(items)

@dag(
    dag_id="anysource_ingest",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2},
    tags=["anysource", "personal"],
)
def anysource_ingest():
    @task
    def read_sources() -> list[dict]:
        return [s.model_dump() for s in load_config()]

    @task
    def fetch_and_land(source: dict, ds: str) -> dict:
        s = Source(**source)
        content = http_get(s.url, s.params, s.headers)
        # write raw
        raw_dir = Path(RAW_DATA_DIR) / s.id / ds
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_file = raw_dir / "response.bin"
        raw_file.write_bytes(content)
        return {"id": s.id, "type": s.type, "path": str(raw_file), "json_path": s.json_path, "selector": s.selector}

    @task
    def normalize_to_duckdb(payloads: list[dict], ds: str) -> str:
        db_path = Path(WAREHOUSE_DIR) / "data.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))
        for p in payloads:
            raw = Path(p["path"]).read_bytes()
            if p["type"] == "http_json":
                df = parse_http_json(raw, p.get("json_path"))
            elif p["type"] == "html_list":
                df = parse_html_list(raw, p.get("selector") or "a")
            else:
                continue
            # Add metadata
            df["_ingest_ds"] = ds
            df["_source_id"] = p["id"]
            # Create a table per source_id if not exist, then append
            table = f'src_{p["id"]}'.replace("-", "_")
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
        con.close()
        return str(db_path)

    sources = read_sources()
    landed = fetch_and_land.expand(source=sources, ds="{{ ds }}")
    _ = normalize_to_duckdb(landed, "{{ ds }}")

anysource_ingest()
