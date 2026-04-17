# DuckLake Demo

A self-contained NHS vaccination data lakehouse demo featuring DuckLake (DuckDB's lakehouse format), dbt transformations, a MESH simulator, and an interactive web dashboard.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌─────────────┐
│  Data        │───▶│  MESH        │───▶│  DuckLake    │───▶│  dbt        │
│  Generator   │    │  Simulator   │    │  (Staging)  │    │  (Marts)    │
└─────────────┘    └──────────────┘    └──────────────┘    └─────────────┘
                                                │
                                        ┌───────▼───────┐
                                        │  Dashboard    │
                                        │  (Flask + SSE)│
                                        └───────────────┘
```

**Pipeline stages:**
1. **Generate** – Creates synthetic NHS vaccination v5 CSV data
2. **MESH** – Simulates NHS MESH file transfer (inbox → processing → archive)
3. **Init** – Initialises the DuckLake warehouse with schemas and tables
4. **Ingest** – Loads archived CSVs into the staging layer
5. **dbt** – Runs transformation models (staging → intermediate → marts)

## Quick Start (Local)

```bash
# Install dependencies
pip install -r requirements.txt

# Initialise the lakehouse and start the dashboard
python start.py
```

Open [http://localhost:8765](http://localhost:8765) and click **Run Full Pipeline**.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8765` | HTTP port (Railway sets this automatically) |
| `DATA_DIR` | `./data` | Root data directory for catalog and parquet files |
| `CATALOG_PATH` | `{DATA_DIR}/catalog/vaccination_lake.ducklake` | DuckLake catalog file |
| `DUCKLAKE_DATA_PATH` | `{DATA_DIR}/parquet` | DuckLake data storage |
| `MESH_DIR` | `./mesh_simulator` | MESH simulator directory |

## Deploy to Railway

1. Create a new repo from this template
2. Connect it to Railway
3. Railway auto-detects the Dockerfile and deploys

The app runs on the port specified by Railway's `PORT` env var (injected automatically).

### Railway CLI

```bash
railway init
railway up
```

## Tech Stack

- **DuckDB** + **DuckLake** – Embedded analytical lakehouse
- **dbt-duckdb** – Transformation layer
- **Flask** – Dashboard backend with SSE streaming
- **Vanilla JS** – Dashboard frontend (no build step needed)

## Project Structure

```
├── Dockerfile
├── railway.toml
├── requirements.txt
├── start.py              # Entry point: init + gunicorn
└── duck_lakehouse/
    ├── dashboard/
    │   ├── app.py         # Flask backend
    │   ├── requirements.txt
    │   └── static/
    │       ├── index.html
    │       ├── app.js
    │       └── styles.css
    ├── data_generator/    # Synthetic v5 vaccination data
    ├── ducklake/          # DuckLake init + ingest
    ├── mesh_simulator/    # NHS MESH simulation
    └── dbt/
        └── dbt_ducklake/ # dbt project
```

## License

MIT