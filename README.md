# DuckLake Demo 🦆

A complete NHS vaccination data pipeline demo using **DuckLake** — DuckDB's native lakehouse format.

## What's Inside

- **DuckLake** catalog with staging → intermediate → marts layers
- **dbt** transformation models (deduplication, validation, dimensional modeling)
- **MESH simulator** — mimics the NHS Message Exchange for Substances and Health
- **Flask dashboard** — pipeline orchestration + built-in SQL query interface
- **Docker-ready** with Railway deployment support

## Quick Start

### Local (Python)

```bash
pip install -r requirements.txt
python start.py
```

Open http://localhost:8765

### Docker

```bash
docker build -t ducklake-demo .
docker run -p 8765:8765 -v ducklake-data:/app/data ducklake-demo
```

### Railway

Push to GitHub and connect the repo to Railway. The `Dockerfile` and `railway.toml` handle the rest.

## Pipeline Stages

| Stage | Description |
|-------|-------------|
| **Generate Data** | Creates synthetic v5 CSV vaccination records |
| **MESH Simulator** | Moves files through inbox → processing → archive |
| **Init DuckLake** | Creates catalog, schemas, and table definitions |
| **Ingest Data** | Loads CSVs from MESH archive into staging |
| **dbt Transform** | Runs dbt models: staging → intermediate → marts |

## SQL Query Interface

The dashboard includes a built-in SQL query interface under the **SQL Query** tab in the Data Explorer. Run any SQL against your DuckLake:

```sql
SELECT * FROM staging.stg_vaccinations LIMIT 50;
SELECT * FROM marts.fct_vaccination_events LIMIT 50;
SELECT vaccine_name, COUNT(*) FROM marts.fct_vaccination_events GROUP BY vaccine_name;
```

## Architecture

```
CSV Files → MESH Simulator → DuckLake Staging → dbt → DuckLake Marts
                                                    ↳ dim_patient
                                                    ↳ dim_site
                                                    ↳ dim_vaccine
                                                    ↳ fct_vaccination_events
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `/app/data` | Base data directory |
| `CATALOG_PATH` | `{DATA_DIR}/catalog/vaccination_lake.ducklake` | DuckLake catalog path |
| `DUCKLAKE_DATA_PATH` | `{DATA_DIR}/parquet` | DuckLake data (parquet) path |
| `MESH_DIR` | `/app/mesh_simulator` | MESH simulator directory |
| `PORT` | `8765` | Dashboard port |

## API Endpoints

- `GET /api/status` — Pipeline stage status
- `GET /api/run/{stage}` — Run a pipeline stage (SSE stream)
- `POST /api/clean` — Clean all data
- `GET /api/tables` — List DuckLake tables
- `GET /api/query/{table}` — Preview table data
- `POST /api/sql` — Execute arbitrary SQL
- `GET /api/sql/schemas` — List schemas
- `GET /api/sql/tables` — List tables with schema info
- `GET /api/preview/row_counts` — Row counts for staging/marts

## Tech Stack

- **DuckDB** + **DuckLake** — OLAP database with native lakehouse format
- **dbt-duckdb** — Transformation framework
- **Flask** — Dashboard and API server
- **gunicorn** — Production WSGI server (single worker for DuckDB consistency)

## License

MIT