from typing import Generator

import pg8000.dbapi as pg

from .config import get_db_config


def get_connection() -> Generator[pg.Connection, None, None]:
    """FastAPI dependency that yields a DB connection and closes it after use."""
    cfg = get_db_config()
    conn = pg.connect(**cfg, ssl_context=True)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass
