#!/usr/bin/env python3
"""
DuckLake Demo - Startup script for Railway deployment

Initialises the DuckLake warehouse and starts the Flask dashboard.
"""

import os
import sys
import subprocess
from pathlib import Path

# Base directory is wherever this script lives
BASE_DIR = Path(__file__).parent

# Environment variable defaults
DATA_DIR = os.environ.get("DATA_DIR", str(BASE_DIR / "data"))
CATALOG_PATH = os.environ.get("CATALOG_PATH", str(Path(DATA_DIR) / "catalog" / "vaccination_lake.ducklake"))
DUCKLAKE_DATA_PATH = os.environ.get("DUCKLAKE_DATA_PATH", str(Path(DATA_DIR) / "parquet"))
MESH_DIR = os.environ.get("MESH_DIR", str(BASE_DIR / "mesh_simulator"))
PORT = int(os.environ.get("PORT", "8765"))


def ensure_dirs():
    """Create required directories if they don't exist."""
    for d in [CATALOG_PATH, DUCKLAKE_DATA_PATH, MESH_DIR]:
        Path(d).parent.mkdir(parents=True, exist_ok=True)
    for subdir in ["inbox", "processing", "archive", "logs"]:
        Path(MESH_DIR).joinpath(subdir).mkdir(parents=True, exist_ok=True)


def init_ducklake():
    """Initialise the DuckLake warehouse."""
    print("Initialising DuckLake...")
    from duck_lakehouse.ducklake.init_ducklake import main as init_main
    init_main(catalog_path=CATALOG_PATH, data_path=DUCKLAKE_DATA_PATH)
    print("DuckLake initialised.")


def main():
    ensure_dirs()
    init_ducklake()

    # Set env vars for the Flask app to pick up
    os.environ.setdefault("DUCKLAKE_PORT", str(PORT))
    os.environ.setdefault("DUCKLAKE_HOST", "0.0.0.0")
    os.environ.setdefault("CATALOG_PATH", CATALOG_PATH)
    os.environ.setdefault("DUCKLAKE_DATA_PATH", DUCKLAKE_DATA_PATH)
    os.environ.setdefault("MESH_DIR", MESH_DIR)
    os.environ.setdefault("DATA_DIR", DATA_DIR)

    # Start the dashboard
    print(f"Starting DuckLake Dashboard on 0.0.0.0:{PORT}")
    from duck_lakehouse.dashboard.app import app

    # Use gunicorn in production, flask dev server as fallback
    try:
        import gunicorn.app.base

        class StandaloneApplication(gunicorn.app.base.BaseApplication):
            def __init__(self, app, options=None):
                self.options = options or {}
                self.application = app
                super().__init__()

            def load_config(self):
                for key, value in self.options.items():
                    if key in self.cfg.settings and value is not None:
                        self.cfg.set(key.lower(), value)

            def load(self):
                return self.application

        options = {
            "bind": f"0.0.0.0:{PORT}",
            "workers": 1,  # single worker for DuckDB state
            "timeout": 120,
            "threads": 4,
        }
        StandaloneApplication(app, options).run()
    except ImportError:
        app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()