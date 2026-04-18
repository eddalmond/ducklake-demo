#!/usr/bin/env python3
"""
DuckLake Demo - Entry point for Railway deployment

Starts the Flask dashboard under gunicorn (single worker for DuckDB
consistency) and initialises DuckLake in the background so the health
check endpoint can respond immediately.
"""

import logging
import os
import threading
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

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
    """Initialise the DuckLake warehouse (runs in background thread)."""
    global _ducklake_ready
    try:
        logger.info("Initialising DuckLake...")
        from duck_lakehouse.ducklake.init_ducklake import main as init_main
        init_main(catalog_path=CATALOG_PATH, data_path=DUCKLAKE_DATA_PATH)
        logger.info("DuckLake initialised.")
        # Signal readiness to the Flask app
        from duck_lakehouse.dashboard.app import ducklake_ready
        # We can't set this directly from a thread safely without the module ref,
        # so set it on the module object
        import duck_lakehouse.dashboard.app as app_module
        app_module.ducklake_ready = True
    except Exception:
        logger.exception("DuckLake initialisation failed (non-fatal, dashboard will still serve)")


def main():
    ensure_dirs()

    # Set env vars for the Flask app
    os.environ.setdefault("DUCKLAKE_PORT", str(PORT))
    os.environ.setdefault("DUCKLAKE_HOST", "0.0.0.0")
    os.environ.setdefault("CATALOG_PATH", CATALOG_PATH)
    os.environ.setdefault("DUCKLAKE_DATA_PATH", DUCKLAKE_DATA_PATH)
    os.environ.setdefault("MESH_DIR", MESH_DIR)
    os.environ.setdefault("DATA_DIR", DATA_DIR)

    # Start DuckLake init in a background thread so the web server
    # can respond to health checks immediately (fixes Railway deploy).
    init_thread = threading.Thread(target=init_ducklake, daemon=True)
    init_thread.start()

    # Try gunicorn (production), fall back to Flask dev server
    try:
        from gunicorn.app.base import BaseApplication

        class StandaloneApplication(BaseApplication):
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

        from duck_lakehouse.dashboard.app import app

        options = {
            "bind": f"0.0.0.0:{PORT}",
            "workers": 1,           # Single worker for DuckDB consistency
            "threads": 4,
            "timeout": 120,
            "accesslog": "-",
            "errorlog": "-",
            "loglevel": "info",
        }

        logger.info(f"Starting gunicorn on 0.0.0.0:{PORT}")
        StandaloneApplication(app, options).run()

    except ImportError:
        logger.info("gunicorn not available, using Flask dev server")
        from duck_lakehouse.dashboard.app import app
        app.run(host="0.0.0.0", port=PORT, threaded=True)


if __name__ == "__main__":
    main()