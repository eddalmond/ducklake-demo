#!/usr/bin/env python3
"""
DuckLake Dashboard - Flask backend for pipeline visualization and control
"""

import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue

import duckdb
from flask import Flask, Response, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR = Path(__file__).parent.parent
MESH_DIR = Path(os.environ.get("MESH_DIR", str(BASE_DIR / "mesh_simulator")))
DBT_DIR = Path(os.environ.get("DBT_DIR", str(BASE_DIR / "dbt" / "dbt_ducklake")))
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR / "data")))

ARCHIVE_DIR = MESH_DIR / "archive"
INBOX_DIR = MESH_DIR / "inbox"
DUCKLAKE_DIR = DATA_DIR
CATALOG_PATH = Path(os.environ.get("CATALOG_PATH", str(DATA_DIR / "catalog" / "vaccination_lake.ducklake")))
DATA_PATH = Path(os.environ.get("DUCKLAKE_DATA_PATH", str(DATA_DIR / "parquet")))

# Initialization state (set by start.py background thread)
ducklake_ready = False

status = {
    "generate": {"state": "idle", "output": [], "last_run": None},
    "mesh": {"state": "idle", "output": [], "last_run": None},
    "init": {"state": "idle", "output": [], "last_run": None},
    "ingest": {"state": "idle", "output": [], "last_run": None},
    "dbt": {"state": "idle", "output": [], "last_run": None},
}

cached_tables = []


def _refresh_table_cache():
    """Populate cached_tables from DuckLake metadata."""
    global cached_tables
    try:
        conn = get_ducklake_conn()
        names = _discover_tables(conn)
        tables = []
        for fq_name in names:
            try:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM vaccination_lake.{fq_name}"
                ).fetchone()[0]
            except Exception:
                count = None
            tables.append({"name": fq_name, "rows": count})
        cached_tables = [t for t in tables if t.get("rows") is None or t["rows"] > 0]
    except Exception:
        pass

# DuckLake connection - use a shared connection per worker process
_ducklake_conn = None
_ducklake_conn_lock = threading.Lock()

def get_ducklake_conn():
    """Get a DuckLake connection, reusing the existing one if available.
    
    Since DuckLake uses file-based locking for its metadata, we can only
    have one connection per worker process. This returns a shared connection.
    """
    global _ducklake_conn
    with _ducklake_conn_lock:
        if _ducklake_conn is None:
            conn = duckdb.connect()
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
            conn.execute(
                f"ATTACH 'ducklake:{CATALOG_PATH}' "
                f"AS vaccination_lake (DATA_PATH '{DATA_PATH}', OVERRIDE_DATA_PATH true)"
            )
            conn.execute("USE vaccination_lake")
            _ducklake_conn = conn
        return _ducklake_conn


def run_command(cmd, cwd=None, stage=None):
    """Run a command and stream output via SSE."""
    status[stage]["state"] = "running"
    status[stage]["output"] = []
    status[stage]["last_run"] = datetime.now().isoformat()
    
    def stream():
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=cwd or str(BASE_DIR),
                text=True,
                bufsize=1,
                env={**os.environ, "PYTHONUNBUFFERED": "1"}
            )
            
            for line in process.stdout:
                status[stage]["output"].append(line.rstrip())
                yield f"data: {json.dumps({'stage': stage, 'line': line.rstrip()})}\n\n"
            
            process.wait()
            if process.returncode == 0:
                status[stage]["state"] = "success"
                if stage in ("ingest", "dbt"):
                    _refresh_table_cache()
            else:
                status[stage]["state"] = "error"
            yield f"data: {json.dumps({'stage': stage, 'done': True, 'exit_code': process.returncode})}\n\n"
            
        except Exception as e:
            status[stage]["state"] = "error"
            yield f"data: {json.dumps({'stage': stage, 'error': str(e)})}\n\n"
    
    return stream

@app.route("/health")
def health():
    """Lightweight health check endpoint for Railway."""
    return jsonify({"status": "ok", "service": "ducklake-demo", "ducklake_ready": ducklake_ready})

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

@app.route("/api/status")
def get_status():
    return jsonify(status)

@app.route("/api/files/<path:stage>")
def get_files(stage):
    """List files for a given stage."""
    try:
        if stage == "inbox":
            path = MESH_DIR / "inbox"
        elif stage == "processing":
            path = MESH_DIR / "processing"
        elif stage == "archive":
            path = MESH_DIR / "archive"
        elif stage == "logs":
            path = MESH_DIR / "logs"
        elif stage == "catalog":
            path = DATA_DIR / "catalog"
        elif stage == "data":
            path = DATA_DIR / "parquet"
        else:
            return jsonify({"error": "Unknown stage"}), 400
        
        if not path.exists():
            return jsonify({"files": []})
        
        files = []
        for f in sorted(path.iterdir()):
            if f.is_file():
                stat = f.stat()
                files.append({
                    "name": f.name,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
        return jsonify({"files": files})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/preview/<stage>")
def preview_data(stage):
    """Preview data from various stages."""
    try:
        if stage == "csv_sample":
            archive = MESH_DIR / "archive"
            csv_files = sorted(archive.glob("*.csv"))
            inbox_files = sorted((MESH_DIR / "inbox").glob("*.csv"))
            all_files = inbox_files + csv_files
            if not all_files:
                return jsonify({"headers": [], "rows": [], "source": "No CSV files"})
            csv_file = all_files[0]
            
            with open(csv_file, "r", encoding="utf-8") as f:
                content = f.read()
                lines = content.strip().split("\r\n") if "\r\n" in content else content.strip().split("\n")
                if not lines:
                    return jsonify({"headers": [], "rows": [], "source": str(csv_file.name)})
                
                import re
                field_re = re.compile(r'"([^"]*)"')
                headers = [m.group(1) for m in field_re.finditer(lines[0])]
                rows = []
                for line in lines[1:11]:
                    if line.strip():
                        values = [m.group(1) for m in field_re.finditer(line)]
                        rows.append(dict(zip(headers, values)))
                
                return jsonify({
                    "headers": headers[:10],
                    "rows": [{k: v for k, v in row.items() if k in headers[:10]} for row in rows],
                    "source": csv_file.name
                })
        
        elif stage == "staging":
            try:
                conn = get_ducklake_conn()
                tables = [t for t in _discover_tables(conn) if "stg_" in t]
                result = []
                columns = []
                if tables:
                    try:
                        result = conn.execute(f"SELECT * FROM vaccination_lake.{tables[0]} LIMIT 5").fetchall()
                        columns = [desc[0] for desc in conn.description]
                    except Exception:
                        pass
                
                rows = []
                for row in result:
                    row_dict = {}
                    for i, col in enumerate(columns[:8]):
                        row_dict[col] = str(row[i])[:50] if row[i] is not None else None
                    rows.append(row_dict)
                
                return jsonify({"headers": columns[:8], "rows": rows})
            except Exception as e:
                return jsonify({"error": str(e)})
        
        elif stage == "marts":
            try:
                conn = get_ducklake_conn()
                tables = [t for t in _discover_tables(conn) if "fct_" in t]
                result = []
                columns = []
                if tables:
                    try:
                        result = conn.execute(f"SELECT * FROM vaccination_lake.{tables[0]} LIMIT 5").fetchall()
                        columns = [desc[0] for desc in conn.description]
                    except Exception:
                        pass
                
                rows = []
                for row in result:
                    row_dict = {}
                    for i, col in enumerate(columns[:8]):
                        row_dict[col] = str(row[i])[:50] if row[i] is not None else None
                    rows.append(row_dict)
                
                return jsonify({"headers": columns[:8], "rows": rows})
            except Exception as e:
                return jsonify({"error": str(e)})
        
        elif stage == "row_counts":
            try:
                conn = get_ducklake_conn()
                staging_count = 0
                marts_count = 0
                for tname in _discover_tables(conn):
                    if "stg_" in tname:
                        try:
                            staging_count = conn.execute(f"SELECT COUNT(*) FROM vaccination_lake.{tname}").fetchone()[0]
                        except Exception:
                            pass
                    if "fct_" in tname:
                        try:
                            marts_count = conn.execute(f"SELECT COUNT(*) FROM vaccination_lake.{tname}").fetchone()[0]
                        except Exception:
                            pass
                return jsonify({"staging": staging_count, "marts": marts_count})
            except:
                return jsonify({"staging": 0, "marts": 0})
        
        return jsonify({"error": "Unknown preview stage"}), 400
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sample-files")
def list_sample_files():
    """List all CSV files in inbox with summary info."""
    try:
        inbox = MESH_DIR / "inbox"
        archive = MESH_DIR / "archive"
        results = []
        
        for csv_dir, location in [(inbox, "inbox"), (archive, "archive")]:
            if not csv_dir.exists():
                continue
            for f in sorted(csv_dir.glob("*.csv")):
                lines_count = 0
                vaccine_type = f.stem.split("_")[0] if "_" in f.stem else f.stem
                try:
                    with open(f, "r", encoding="utf-8") as fh:
                        for _ in fh:
                            lines_count += 1
                except Exception:
                    pass
                
                results.append({
                    "name": f.name,
                    "location": location,
                    "size": f.stat().st_size,
                    "rows": max(0, lines_count - 1),
                    "vaccine_type": vaccine_type,
                    "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                })
        
        return jsonify({"files": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sample-file/<path:filename>")
def preview_sample_file(filename):
    """Preview a specific CSV file from inbox or archive."""
    try:
        for csv_dir in [(MESH_DIR / "inbox", "inbox"), (MESH_DIR / "archive", "archive")]:
            csv_dir_path, location = csv_dir
            filepath = csv_dir_path / filename
            if filepath.exists() and filepath.suffix == ".csv":
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read()
                    lines = content.strip().split("\r\n") if "\r\n" in content else content.strip().split("\n")
                    if not lines:
                        return jsonify({"headers": [], "rows": [], "total_rows": 0, "source": filename, "location": location})
                    
                    import re
                    field_re = re.compile(r'"([^"]*)"')
                    headers = [m.group(1) for m in field_re.finditer(lines[0])]
                    rows = []
                    for line in lines[1:51]:
                        if line.strip():
                            values = [m.group(1) for m in field_re.finditer(line)]
                            rows.append(dict(zip(headers, values)))
                    
                    return jsonify({
                        "headers": headers,
                        "rows": rows,
                        "total_rows": len(lines) - 1,
                        "source": filename,
                        "location": location,
                        "size": filepath.stat().st_size,
                    })
        
        return jsonify({"error": f"File not found: {filename}"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

EXCLUDE_SCHEMAS = {"main", "information_schema", "pg_catalog"}
EXCLUDE_PREFIXES = ("ducklake_", "__ducklake_metadata")


def _discover_tables(conn=None):
    """Query DuckLake metadata to discover all user tables and views."""
    if conn is None:
        try:
            conn = get_ducklake_conn()
        except Exception:
            return []
    try:
        tables = conn.execute("""
            SELECT schema_name, table_name
            FROM duckdb_tables()
            WHERE database_name = 'vaccination_lake'
        """).fetchall()
        views = conn.execute("""
            SELECT schema_name, view_name
            FROM duckdb_views()
            WHERE database_name = 'vaccination_lake'
        """).fetchall()

        result = []
        for schema, name in tables + views:
            if schema in EXCLUDE_SCHEMAS:
                continue
            if any(schema.startswith(p) or name.startswith(p) for p in EXCLUDE_PREFIXES):
                continue
            result.append(f"{schema}.{name}")
        return sorted(result)
    except Exception:
        return []


@app.route("/api/tables")
def list_tables():
    """List tables from cache, refreshing if empty."""
    if not cached_tables:
        _refresh_table_cache()
    return jsonify({"tables": cached_tables})

@app.route("/api/query/<path:table_name>")
def query_table(table_name):
    """Query a DuckLake table with pagination support."""
    allowed = {t["name"] for t in cached_tables} or set(_discover_tables())
    if table_name not in allowed:
        return jsonify({"error": f"Table not found: {table_name}"}), 400

    try:
        conn = get_ducklake_conn()
    except Exception:
        return jsonify({"error": "Cannot connect to DuckLake"}), 500
    try:
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
        limit = min(limit, 200)
        offset = max(offset, 0)

        total = conn.execute(f"SELECT COUNT(*) FROM vaccination_lake.{table_name}").fetchone()[0]

        result = conn.execute(
            f"SELECT * FROM vaccination_lake.{table_name} LIMIT {limit} OFFSET {offset}"
        ).fetchall()
        columns = [desc[0] for desc in conn.description]

        rows = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                if val is None:
                    row_dict[col] = None
                elif isinstance(val, (int, float)):
                    row_dict[col] = val
                else:
                    row_dict[col] = str(val)[:200]
            rows.append(row_dict)

        return jsonify({
            "table": table_name,
            "columns": columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/run/<stage>")
def run_stage(stage):
    """Stream output from running a pipeline stage."""
    def generate():
        if stage == "generate":
            cmd = [sys.executable, "-m", "duck_lakehouse.data_generator", 
                   "--output", str(INBOX_DIR), 
                   "--records", "100", "--type", "all"]
            yield from run_command(cmd, cwd=str(BASE_DIR), stage=stage)()
        elif stage == "mesh":
            cmd = [sys.executable, "-m", "duck_lakehouse.mesh_simulator",
                   "--base-dir", str(MESH_DIR), "--once"]
            yield from run_command(cmd, cwd=str(BASE_DIR), stage=stage)()
        elif stage == "init":
            # Use shared connection instead of subprocess to avoid lock conflict
            status[stage]["state"] = "running"
            status[stage]["output"] = []
            try:
                yield f"data: {json.dumps({'line': 'Using shared DuckLake connection...'})}\n\n"
                conn = get_ducklake_conn()
                
                # Re-run table creation using shared connection
                from duck_lakehouse.ducklake.init_ducklake import (
                    create_schemas, create_staging_tables, 
                    create_intermediate_tables, create_mart_tables, create_reference_tables
                )
                
                lake_name = "vaccination_lake"
                create_schemas(conn, lake_name)
                yield f"data: {json.dumps({'line': 'Schemas created'})}\n\n"
                
                create_staging_tables(conn, lake_name)
                yield f"data: {json.dumps({'line': 'Staging tables created'})}\n\n"
                
                create_intermediate_tables(conn, lake_name)
                yield f"data: {json.dumps({'line': 'Intermediate tables created'})}\n\n"
                
                create_mart_tables(conn, lake_name)
                yield f"data: {json.dumps({'line': 'Mart tables created'})}\n\n"
                
                create_reference_tables(conn, lake_name)
                yield f"data: {json.dumps({'line': 'Reference tables created'})}\n\n"
                
                result = conn.execute("SHOW ALL TABLES").fetchall()
                yield f"data: {json.dumps({'line': f'Total tables: {len(result)}'})}\n\n"
                
                status[stage]["state"] = "completed"
                status[stage]["last_run"] = datetime.now().isoformat()
                _refresh_table_cache()
                yield f"data: {json.dumps({'done': True, 'returncode': 0})}\n\n"
            except Exception as e:
                status[stage]["state"] = "failed"
                yield f"data: {json.dumps({'line': f'Error: {e}'})}\n\n"
                yield f"data: {json.dumps({'done': True, 'returncode': 1, 'error': str(e)})}\n\n"
        elif stage == "ingest":
            # Use shared connection instead of subprocess to avoid lock conflict
            status[stage]["state"] = "running"
            status[stage]["output"] = []
            try:
                yield f"data: {json.dumps({'line': 'Using shared DuckLake connection...'})}\n\n"
                conn = get_ducklake_conn()
                
                from duck_lakehouse.ducklake.ingest import ingest_files
                total = ingest_files(
                    archive_dir=str(ARCHIVE_DIR),
                    conn=conn
                )
                yield f"data: {json.dumps({'line': f'Ingested {total} records'})}\n\n"
                status[stage]["state"] = "completed"
                status[stage]["last_run"] = datetime.now().isoformat()
                _refresh_table_cache()
                yield f"data: {json.dumps({'done': True, 'returncode': 0})}\n\n"
            except Exception as e:
                status[stage]["state"] = "failed"
                yield f"data: {json.dumps({'line': f'Error: {e}'})}\n\n"
                yield f"data: {json.dumps({'done': True, 'returncode': 1, 'error': str(e)})}\n\n"
        elif stage == "dbt":
            cmd = ["dbt", "run", "--profiles-dir", str(DBT_DIR),
                   "--project-dir", str(DBT_DIR)]
            yield from run_command(cmd, cwd=str(BASE_DIR), stage=stage)()
        else:
            yield f"data: {json.dumps({'error': 'Unknown stage'})}\n\n"
    
    return Response(generate(), mimetype="text/event-stream")

@app.route("/api/run/dbt-test")
def run_dbt_test():
    """Run dbt tests."""
    def generate():
        cmd = ["dbt", "test", "--profiles-dir", str(DBT_DIR),
               "--project-dir", str(DBT_DIR)]
        yield from run_command(cmd, cwd=str(BASE_DIR), stage="dbt")()
    
    return Response(generate(), mimetype="text/event-stream")

@app.route("/api/clean", methods=["POST"])
def clean_all():
    """Clean generated data."""
    try:
        import shutil
        
        # Clean MESH directories
        for subdir in ["inbox", "processing", "archive"]:
            path = MESH_DIR / subdir
            if path.exists():
                for f in path.glob("*.csv"):
                    f.unlink()
        
        # Clean logs
        logs = MESH_DIR / "logs"
        if logs.exists():
            for f in logs.glob("*.jsonl"):
                f.unlink()
        
        # Clean DuckLake catalog and data
        if Path(CATALOG_PATH).exists():
            shutil.rmtree(Path(CATALOG_PATH).parent)
        if Path(DATA_PATH).exists():
            shutil.rmtree(Path(DATA_PATH))
        
        # Reset status
        for key in status:
            status[key]["state"] = "idle"
            status[key]["output"] = []
        cached_tables.clear()
        
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# DuckDB UI is now managed by the aiohttp server layer (server.py)
# via duckdb_ui.py's Python API approach, not the CLI subprocess.
# The Flask routes below are fallbacks for standalone Flask mode.

@app.route("/api/duckdb-ui/status")
def duckdb_ui_status():
    """Check if DuckDB UI is running."""
    return jsonify({"running": False, "message": "Run with server.py for DuckDB UI support"})


@app.route("/api/duckdb-ui/start")
def duckdb_ui_start():
    """Start the DuckDB UI."""
    return jsonify({"status": "error", "message": "Run with server.py for DuckDB UI support"}), 501


@app.route("/duckdb-ui")
@app.route("/duckdb-ui/")
@app.route("/duckdb-ui/<path:subpath>")
def duckdb_ui_proxy(subpath=""):
    """DuckDB UI proxy - requires aiohttp server."""
    return jsonify({"error": "DuckDB UI requires running under server.py"}), 503


# --- SQL Query API ---
# Direct SQL execution endpoint for the DuckLake demo.
# This lets users run SQL queries against DuckLake from the dashboard
# without needing MotherDuck authentication.

@app.route("/api/sql", methods=["POST"])
def execute_sql():
    """Execute a SQL query against DuckLake and return results."""
    data = request.get_json(silent=True) or {}
    sql = data.get("query", "").strip()
    if not sql:
        return jsonify({"error": "No query provided"}), 400
    try:
        # Use shared connection instead of creating new one
        conn = get_ducklake_conn()
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description] if result.description else []
        rows = result.fetchall()
        return jsonify({
            "columns": columns,
            "rows": [list(r) for r in rows],
            "rowCount": len(rows),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/sql/tables")
def list_sql_tables():
    """List all DuckLake tables."""
    try:
        # Use shared connection
        conn = get_ducklake_conn()
        result = conn.execute(
            "SELECT table_schema, table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', '__ducklake_metadata') "
            "ORDER BY table_schema, table_name"
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return jsonify({
            "columns": columns,
            "rows": [list(r) for r in rows],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sql/schemas")
def list_sql_schemas():
    """List DuckLake schemas."""
    try:
        # Use shared connection
        conn = get_ducklake_conn()
        result = conn.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema', '__ducklake_metadata') "
            "ORDER BY schema_name"
        )
        schemas = [r[0] for r in result.fetchall()]
        return jsonify({"schemas": schemas})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("DUCKLAKE_PORT", "8765"))
    host = os.environ.get("DUCKLAKE_HOST", "0.0.0.0")
    print("Starting DuckLake Dashboard (standalone - no DuckDB UI)")
    print("For DuckDB UI support, run: python server.py")
    print(f"Listening on http://{host}:{port}")
    app.run(debug=True, host=host, port=port, threaded=True)
