# Assetman (dev notes)

## Setup
- Python 3.13+ vorhanden. Install deps: `python -m pip install --user -r requirements.txt`.
- `.env` enthält `DATABASE_URL` (Postgres, nicht commiten).

## Alembic (Migrationen)
- Initiale Revision: `alembic/versions/20250118_0001_init.py` liest `schema.sql` und legt Schema an (wenn noch keine itam-Tabellen).
- Bestehende DB mit Schema? Nutze `python -m alembic stamp head` (setzt Version ohne Schema neu anzulegen).
- Frische/leer DB? `python -m alembic upgrade head` (legt Schema an).
- Weitere Migrationen: neue Revisionen per `python -m alembic revision -m "..."` und Upgrade-Logik manuell ergänzen (keine Autogenerate-Models vorhanden).
- CI: `.github/workflows/ci.yml` erwartet ein Secret `TEST_DATABASE_URL` (Postgres) und führt `alembic upgrade head` + `python smoke_test.py` aus.

## Legacy Helpers
- `python apply_schema.py` (nur nutzen, wenn itam leer ist; droppt leer-Schema und legt neu an).
- `python seed_data.py` (truncate + Seed-Daten).

## Quick health
- DB-Ping: `python - <<'PY'\nimport pg8000.dbapi as pg\nfrom urllib.parse import urlparse, unquote\nfrom pathlib import Path\np=urlparse([l.split('=',1)[1].strip() for l in Path('.env').read_text().splitlines() if l.startswith('DATABASE_URL=')][0])\nconn=pg.connect(user=unquote(p.username), password=unquote(p.password), host=p.hostname, port=p.port or 5432, database=p.path.lstrip('/'), ssl_context=True)\ncur=conn.cursor(); cur.execute('select 1'); print(cur.fetchone()); conn.close()\nPY`

## API (FastAPI, minimal)
- Start: `python -m uvicorn app.main:app --reload --port 8000`.
- Endpoints:
  - `GET /health` DB-Ping
  - `GET /assets` Liste inkl. offener Assignment
  - `POST /assets` Anlage Asset
  - `PUT /assets/{asset_id}` Update Asset (teilweise)
  - `DELETE /assets/{asset_id}` Löschen Asset (fails bei FK-Referenzen)
  - `POST /assignments` Neue Zuweisung (person_id oder location_id notwendig)
  - `PUT /assignments/{assignment_id}` Update (z.B. assigned_to setzen, purpose/notes)
  - `DELETE /assignments/{assignment_id}` Löschen Assignment
