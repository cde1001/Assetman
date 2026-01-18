# Assetman (prototype)

## Setup
- Python 3.13+. Install deps: `python -m pip install --user -r requirements.txt`.
- `.env` contains `DATABASE_URL` (Postgres, do not commit).
- Auth: `APP_USERS` format `user:pass:role;user2:pass:role`. Default dev users: `admin/admin123`, `operator/op123`, `viewer/view123`. Optional `APP_SECRET_KEY` for token signing.

## Alembic
- Initial revision: `alembic/versions/20250118_0001_init.py` reads `schema.sql` (if no itam tables yet).
- Existing DB with schema: `python -m alembic stamp head`.
- Fresh DB: `python -m alembic upgrade head`.
- New migrations: `python -m alembic revision -m "..."` and add manual SQL.
- CI: `.github/workflows/ci.yml` expects `TEST_DATABASE_URL` (Postgres), runs `alembic upgrade head` + `python smoke_test.py` (schema reset per run).

## Legacy helpers
- `python apply_schema.py` (only if itam is empty; drops empty schema then reapplies).
- `python seed_data.py` (truncate + seed).

## Quick health
- DB ping: `python - <<'PY'\nfrom pathlib import Path\nfrom urllib.parse import urlparse, unquote\nimport pg8000.dbapi as pg\np=urlparse([l.split('=',1)[1].strip() for l in Path('.env').read_text().splitlines() if l.startswith('DATABASE_URL=')][0])\nconn=pg.connect(user=unquote(p.username), password=unquote(p.password), host=p.hostname, port=p.port or 5432, database=p.path.lstrip('/'), ssl_context=True)\ncur=conn.cursor(); cur.execute('select 1'); print(cur.fetchone()); conn.close()\nPY`

## API (FastAPI)
- Start: `python -m uvicorn app.main:app --reload --port 8000`.
- Auth: `POST /auth/token` (Basic -> bearer token), `GET /auth/demo-token` (viewer token for demo UI).
- `GET /health` DB ping.
- Assets: `GET /assets` (filters: `q`, `status`, `type`, `owner_org_unit_id`, `assigned`, `sort`), `POST /assets` (admin/operator), `PUT /assets/{asset_id}` (admin/operator, guarded status transitions), `DELETE /assets/{asset_id}` (admin).
- Assignments: `POST /assignments` (no retired assets; admin/operator), `PUT /assignments/{assignment_id}` (admin/operator), `DELETE /assignments/{assignment_id}` (admin).
- Frontend: `/` minimal list + filters (uses demo viewer token), visual style inspired by diakonissenhaus.de.

## Roles (prototype)
- admin: full access.
- operator: manage assets/assignments (no deletes).
- viewer: read-only.
