import os
from functools import lru_cache
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv

# Load .env if present (no-ops in prod when env is set)
load_dotenv()


@lru_cache(maxsize=1)
def get_db_config():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    p = urlparse(url)
    if not all([p.scheme, p.hostname, p.username, p.path]):
        raise RuntimeError("DATABASE_URL invalid")
    return {
        "user": unquote(p.username),
        "password": unquote(p.password) if p.password else None,
        "host": p.hostname,
        "port": p.port or 5432,
        "database": p.path.lstrip("/"),
    }
