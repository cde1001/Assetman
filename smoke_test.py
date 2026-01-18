import os
import subprocess
import sys
from urllib.parse import urlparse, unquote

import pg8000.dbapi as pg


def main():
    test_url = os.environ.get("TEST_DATABASE_URL")
    if not test_url:
        print("TEST_DATABASE_URL not set; skipping smoke test.")
        return
    reset_schema = os.environ.get("RESET_ITAM_SCHEMA") == "1"

    env = os.environ.copy()
    env["DATABASE_URL"] = test_url

    def run(cmd):
        proc = subprocess.run(
            [sys.executable, cmd], env=env, capture_output=True, text=True
        )
        if proc.returncode != 0:
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
            sys.exit(proc.returncode)
        else:
            print(proc.stdout.strip())

    if reset_schema:
        p = urlparse(test_url)
        try:
            conn = pg.connect(
                user=unquote(p.username),
                password=unquote(p.password) if p.password else None,
                host=p.hostname,
                port=p.port or 5432,
                database=p.path.lstrip("/"),
                ssl_context=True,
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS itam CASCADE;")
            print("Dropped schema itam for clean reset.")
        except Exception as exc:
            print(f"Failed to drop schema: {exc}", file=sys.stderr)
            sys.exit(1)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    run("apply_schema.py")
    run("seed_data.py")

    # simple counts to confirm data
    p = urlparse(test_url)
    conn = pg.connect(
        user=unquote(p.username),
        password=unquote(p.password) if p.password else None,
        host=p.hostname,
        port=p.port or 5432,
        database=p.path.lstrip("/"),
        ssl_context=True,
    )
    cur = conn.cursor()
    cur.execute(
        """
        select 'assets' as tbl, count(*) from itam.assets
        union all select 'asset_assignments', count(*) from itam.asset_assignments
        union all select 'licenses', count(*) from itam.licenses
        """
    )
    for row in cur.fetchall():
        print(row)
    conn.close()


if __name__ == "__main__":
    main()
