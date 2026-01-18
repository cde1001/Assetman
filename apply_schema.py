import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

import pg8000.dbapi as pg


def load_database_url(env_path: Path) -> str:
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
    if not env_path.exists():
        raise FileNotFoundError(".env not found")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip()
    raise ValueError("DATABASE_URL not found in env or .env")


def parse_database_url(url: str):
    p = urlparse(url)
    if not all([p.scheme, p.hostname, p.username, p.path]):
        raise ValueError("Invalid DATABASE_URL format")
    return {
        "user": unquote(p.username),
        "password": unquote(p.password) if p.password else None,
        "host": p.hostname,
        "port": p.port or 5432,
        "database": p.path.lstrip("/"),
    }


def split_sql(sql_text: str):
    """Split SQL by top-level semicolons; keep DO $$ ... $$ intact."""
    stmts = []
    buf = []
    in_single = False
    in_double = False
    in_dollar = False
    dollar_tag = ""
    i = 0
    length = len(sql_text)

    while i < length:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < length else ""

        if in_single:
            buf.append(ch)
            if ch == "'" and nxt == "'":
                buf.append(nxt)
                i += 1
            elif ch == "'":
                in_single = False
        elif in_double:
            buf.append(ch)
            if ch == '"' and nxt == '"':
                buf.append(nxt)
                i += 1
            elif ch == '"':
                in_double = False
        elif in_dollar:
            buf.append(ch)
            if sql_text.startswith(dollar_tag, i):
                buf.extend(dollar_tag[1:])
                i += len(dollar_tag) - 1
                in_dollar = False
        else:
            if ch == "'":
                in_single = True
                buf.append(ch)
            elif ch == '"':
                in_double = True
                buf.append(ch)
            elif ch == "$":
                # detect $tag$
                end_pos = sql_text.find("$", i + 1)
                if end_pos != -1:
                    tag = sql_text[i : end_pos + 1]
                    if tag.startswith("$") and tag.endswith("$") and all(
                        c.isalnum() or c == "_" for c in tag[1:-1]
                    ):
                        dollar_tag = tag
                        in_dollar = True
                        buf.append(ch)
                    else:
                        buf.append(ch)
                else:
                    buf.append(ch)
            elif ch == ";":
                stmt = "".join(buf).strip()
                if stmt:
                    stmts.append(stmt)
                buf = []
            else:
                buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def schema_has_tables(cur) -> bool:
    cur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'itam';"
    )
    return cur.fetchone()[0] > 0


def schema_has_data(cur) -> bool:
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'itam';"
    )
    tables = [row[0] for row in cur.fetchall()]
    for table in tables:
        cur.execute(f'SELECT 1 FROM itam."{table}" LIMIT 1;')
        if cur.fetchone():
            return True
    return False


def main():
    try:
        db_url = load_database_url(Path(".env"))
        conn_args = parse_database_url(db_url)
    except Exception as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    schema_path = Path("schema.sql")
    if not schema_path.exists():
        print("schema.sql not found", file=sys.stderr)
        sys.exit(1)

    try:
        conn = pg.connect(**conn_args, ssl_context=True)
    except Exception as exc:
        print(f"Connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        cur = conn.cursor()
        has_tables = schema_has_tables(cur)
        has_data = schema_has_data(cur) if has_tables else False

        if has_tables and has_data:
            print("Schema 'itam' already has data; skipping to avoid destructive changes.")
            return

        if has_tables and not has_data:
            print("Schema 'itam' exists with empty tables: dropping for clean apply.")
            conn.autocommit = True
            cur.execute("DROP SCHEMA IF EXISTS itam CASCADE;")
            conn.autocommit = False

        sql_text = schema_path.read_text(encoding="utf-8")
        statements = split_sql(sql_text)
        conn.autocommit = False
        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
        print(f"Applied schema.sql ({len(statements)} statements) to database.")
    except Exception as exc:
        conn.rollback()
        print(f"Failed applying schema: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
