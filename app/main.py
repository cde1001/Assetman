from typing import Any, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from .auth import User, demo_token, login, require_role
from .db import get_connection
from .schemas import (
    AssetCreate,
    AssetUpdate,
    AssignmentCreate,
    AssignmentUpdate,
)


ALLOWED_STATUS_TRANSITIONS = {
    "in_stock": {"in_use", "repair", "retired"},
    "in_use": {"in_stock", "repair", "retired"},
    "repair": {"in_use", "retired"},
    "retired": set(),
}

SORT_COLUMNS = {
    "asset_tag": "a.asset_tag",
    "status": "ast.name",
    "type": "atype.name",
    "owner": "ou.name",
    "location": "loc.name",
    "updated": "a.updated_at",
}

app = FastAPI(title="Assetman API", version="0.2.0")


def db_error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def get_status_name(conn, status_id: int) -> str:
    cur = conn.cursor()
    cur.execute("SELECT name FROM itam.asset_status WHERE status_id = %s", (status_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid status_id")
    return row[0]


def get_asset_status(conn, asset_id: int) -> Tuple[int, str]:
    cur = conn.cursor()
    cur.execute(
        "SELECT a.status_id, s.name FROM itam.assets a JOIN itam.asset_status s ON s.status_id = a.status_id WHERE a.asset_id = %s",
        (asset_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return row[0], row[1]


@app.post("/auth/token")
def auth_token(token: str = Depends(login)) -> Dict[str, str]:
    return {"access_token": token, "token_type": "bearer"}


@app.get("/auth/demo-token")
def auth_demo_token() -> Dict[str, str]:
    return {"access_token": demo_token(), "token_type": "bearer"}


@app.get("/health")
def health(conn=Depends(get_connection)) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute("SELECT 1")
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="DB check failed")
    return {"status": "ok", "db": row[0]}


@app.get("/")
def home() -> HTMLResponse:
    html = """
    <!doctype html>
    <html lang="de">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Assetman</title>
      <style>
        :root {
          --bg: #f6f7f9;
          --card: #ffffff;
          --accent: #0b486b;
          --accent-2: #7bc0e3;
          --text: #1f2d3d;
          --muted: #5b6b7a;
          --shadow: 0 12px 30px rgba(0,0,0,0.08);
        }
        * { box-sizing: border-box; }
        body {
          margin: 0; padding: 0;
          font-family: "Helvetica Neue", Arial, sans-serif;
          background: linear-gradient(135deg, #eef3f7, #f9fbfd);
          color: var(--text);
        }
        header {
          background: linear-gradient(120deg, var(--accent), #118ab2);
          color: #fff;
          padding: 32px 24px 24px;
          box-shadow: var(--shadow);
        }
        .hero {
          max-width: 1040px;
          margin: 0 auto;
          display: flex;
          flex-wrap: wrap;
          gap: 16px;
          align-items: center;
          justify-content: space-between;
        }
        .hero h1 { margin: 0; font-size: 32px; letter-spacing: 0.5px; }
        .hero p { margin: 8px 0 0; color: #e8f4fb; max-width: 640px; }
        .badge { background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.3); padding: 6px 12px; border-radius: 999px; font-size: 12px; letter-spacing: 0.2px; }
        main { max-width: 1040px; margin: 20px auto; padding: 0 16px 40px; }
        .panel { background: var(--card); border-radius: 14px; box-shadow: var(--shadow); padding: 18px; }
        .filters { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 12px; }
        input, select { width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid #d8e2eb; font-size: 14px; outline: none; }
        input:focus, select:focus { border-color: var(--accent-2); box-shadow: 0 0 0 3px rgba(17,138,178,0.12); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px 8px; text-align: left; border-bottom: 1px solid #edf1f5; font-size: 14px; }
        th { color: var(--muted); font-weight: 600; }
        tr:hover { background: #f7fbff; }
        .pill { padding: 4px 10px; border-radius: 999px; font-size: 12px; display: inline-block; }
        .pill.stock { background: #e9f5ff; color: #0b486b; }
        .pill.use { background: #e9f9ef; color: #1b7b3a; }
        .pill.repair { background: #fff6e6; color: #b57600; }
        .pill.retired { background: #f7e9f0; color: #9b1b30; }
        .footer { margin-top: 16px; color: var(--muted); font-size: 12px; }
        @media (max-width: 720px) {
          .hero { flex-direction: column; align-items: flex-start; }
          header { padding: 24px 16px; }
          .panel { padding: 14px; }
          th:nth-child(5), td:nth-child(5), th:nth-child(6), td:nth-child(6) { display: none; }
        }
      </style>
    </head>
    <body>
      <header>
        <div class="hero">
          <div>
            <div class="badge">Assetman · Diakonissenhaus inspiriert</div>
            <h1>Assets im Blick behalten</h1>
            <p>Inventar, Zuweisungen und Lizenzen für 50 Einrichtungen – klar strukturiert, schnell filterbar.</p>
          </div>
        </div>
      </header>
      <main>
        <div class="panel">
          <div class="filters">
            <input id="q" placeholder="Suche (Tag, Seriennr., Modell)">
            <select id="status">
              <option value="">Status</option>
              <option value="in_use">In Verwendung</option>
              <option value="in_stock">Auf Lager</option>
              <option value="repair">In Reparatur</option>
              <option value="retired">Ausgemustert</option>
            </select>
            <select id="sort">
              <option value="asset_tag">Sortierung: Asset-Tag</option>
              <option value="-updated">Sortierung: Zuletzt aktualisiert</option>
              <option value="status">Status</option>
              <option value="type">Typ</option>
              <option value="owner">Einrichtung</option>
            </select>
          </div>
          <table>
            <thead>
              <tr>
                <th>Asset</th><th>Typ</th><th>Status</th><th>Ort/Zuweisung</th><th>Seriennr.</th><th>Org-Einheit</th>
              </tr>
            </thead>
            <tbody id="rows">
              <tr><td colspan="6">Lade Daten...</td></tr>
            </tbody>
          </table>
          <div class="footer">Viewer-Demo mit begrenzter API-Nutzung. Rollen: admin, operator, viewer.</div>
        </div>
      </main>
      <script>
        const rows = document.getElementById('rows');
        const q = document.getElementById('q');
        const status = document.getElementById('status');
        const sort = document.getElementById('sort');
        let token = null;

        async function fetchToken() {
          const res = await fetch('/auth/demo-token');
          const data = await res.json();
          token = data.access_token;
        }

        function statusPill(name) {
          const map = {
            'in_use': 'use',
            'in_stock': 'stock',
            'repair': 'repair',
            'retired': 'retired'
          };
          const cls = map[name] || 'stock';
          return `<span class="pill ${cls}">${name}</span>`;
        }

        async function load() {
          if (!token) { await fetchToken(); }
          const params = new URLSearchParams();
          if (q.value) params.append('q', q.value);
          if (status.value) params.append('status', status.value);
          if (sort.value) params.append('sort', sort.value);
          const res = await fetch('/assets?' + params.toString(), {
            headers: { Authorization: 'Bearer ' + token }
          });
          const data = await res.json();
          if (!Array.isArray(data)) {
            rows.innerHTML = `<tr><td colspan="6">${data.detail || 'Fehler'}</td></tr>`;
            return;
          }
          if (!data.length) {
            rows.innerHTML = '<tr><td colspan="6">Keine Ergebnisse</td></tr>';
            return;
          }
          rows.innerHTML = data.map(item => `
            <tr>
              <td>${item.asset_tag || ''}</td>
              <td>${item.type || ''}</td>
              <td>${statusPill(item.status)}</td>
              <td>${item.person || item.location || '-'}</td>
              <td>${item.serial_number || ''}</td>
              <td>${item.owner || ''}</td>
            </tr>
          `).join('');
        }

        q.addEventListener('input', () => { clearTimeout(window._t); window._t = setTimeout(load, 200); });
        status.addEventListener('change', load);
        sort.addEventListener('change', load);
        load();
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/assets")
def list_assets(
    conn=Depends(get_connection),
    _: User = Depends(require_role(["admin", "operator", "viewer"])),
    q: Optional[str] = Query(None),
    status: Optional[List[str]] = Query(None),
    type: Optional[List[str]] = Query(None),
    owner_org_unit_id: Optional[int] = Query(None),
    assigned: Optional[bool] = Query(None),
    sort: Optional[str] = Query("asset_tag"),
) -> JSONResponse:
    clauses: List[str] = []
    params: List[Any] = []
    if q:
        clauses.append(
            "(a.asset_tag ILIKE %s OR a.serial_number ILIKE %s OR a.model ILIKE %s OR a.description ILIKE %s)"
        )
        like = f"%{q}%"
        params.extend([like, like, like, like])
    if status:
        placeholders = ", ".join(["%s"] * len(status))
        clauses.append(f"ast.name IN ({placeholders})")
        params.extend(status)
    if type:
        placeholders = ", ".join(["%s"] * len(type))
        clauses.append(f"atype.name IN ({placeholders})")
        params.extend(type)
    if owner_org_unit_id:
        clauses.append("ou.org_unit_id = %s")
        params.append(owner_org_unit_id)
    if assigned is True:
        clauses.append("aa.assignment_id IS NOT NULL")
    elif assigned is False:
        clauses.append("aa.assignment_id IS NULL")

    order = SORT_COLUMNS.get(sort.lstrip("-"), SORT_COLUMNS["asset_tag"])
    direction = "DESC" if sort.startswith("-") else "ASC"
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
          a.asset_id,
          a.asset_tag,
          atype.name AS type,
          ast.name AS status,
          a.manufacturer,
          a.model,
          a.serial_number,
          a.description,
          aa.assigned_from,
          aa.assigned_to,
          per.display_name AS person,
          per.email AS person_email,
          loc.name AS location,
          loc.room AS location_room,
          loc.rack AS location_rack,
          loc.rack_unit AS location_rack_unit,
          ou.name AS owner
        FROM itam.assets a
        JOIN itam.asset_types atype ON atype.type_id = a.type_id
        JOIN itam.asset_status ast ON ast.status_id = a.status_id
        LEFT JOIN itam.asset_assignments aa
          ON aa.asset_id = a.asset_id AND aa.assigned_to IS NULL
        LEFT JOIN itam.people per ON per.person_id = aa.person_id
        LEFT JOIN itam.locations loc ON loc.location_id = aa.location_id
        LEFT JOIN itam.org_units ou ON ou.org_unit_id = a.owner_org_unit_id
        {where_sql}
        ORDER BY {order} {direction}, a.asset_tag ASC
        """,
        params,
    )
    cols = [desc[0] for desc in cur.description]
    data: List[Dict[str, Any]] = [dict(zip(cols, row)) for row in cur.fetchall()]
    return JSONResponse(content=jsonable_encoder(data))


@app.post("/assets", status_code=status.HTTP_201_CREATED)
def create_asset(
    payload: AssetCreate,
    conn=Depends(get_connection),
    _: User = Depends(require_role(["admin", "operator"])),
) -> Dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO itam.assets (
              asset_tag, type_id, status_id, manufacturer, model, serial_number, description,
              purchase_date, purchase_price, currency, warranty_end, owner_org_unit_id, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING asset_id
            """,
            (
                payload.asset_tag,
                payload.type_id,
                payload.status_id,
                payload.manufacturer,
                payload.model,
                payload.serial_number,
                payload.description,
                payload.purchase_date,
                payload.purchase_price,
                payload.currency,
                payload.warranty_end,
                payload.owner_org_unit_id,
                payload.notes,
            ),
        )
        asset_id = cur.fetchone()[0]
        conn.commit()
        return {"asset_id": asset_id}
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.put("/assets/{asset_id}")
def update_asset(
    asset_id: int,
    payload: AssetUpdate,
    conn=Depends(get_connection),
    _: User = Depends(require_role(["admin", "operator"])),
) -> Dict[str, Any]:
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")

    current_status_id, current_status_name = get_asset_status(conn, asset_id)
    if "status_id" in data:
        new_status_name = get_status_name(conn, data["status_id"])
        allowed = ALLOWED_STATUS_TRANSITIONS.get(current_status_name, set())
        if new_status_name not in allowed and new_status_name != current_status_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot transition from {current_status_name} to {new_status_name}",
            )

    cur = conn.cursor()
    set_parts = [f"{col} = %s" for col in data.keys()]
    values = list(data.values()) + [asset_id]
    try:
        cur.execute(
            f"UPDATE itam.assets SET {', '.join(set_parts)} WHERE asset_id = %s RETURNING asset_id",
            values,
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
        conn.commit()
        return {"asset_id": row[0]}
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.delete("/assets/{asset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_asset(
    asset_id: int,
    conn=Depends(get_connection),
    _: User = Depends(require_role(["admin"])),
) -> None:
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM itam.assets WHERE asset_id = %s RETURNING asset_id", (asset_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.post("/assignments", status_code=status.HTTP_201_CREATED)
def create_assignment(
    payload: AssignmentCreate,
    conn=Depends(get_connection),
    user: User = Depends(require_role(["admin", "operator"])),
) -> Dict[str, Any]:
    status_id, status_name = get_asset_status(conn, payload.asset_id)
    if status_name == "retired":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Asset is retired")

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO itam.asset_assignments
              (asset_id, person_id, location_id, assigned_from, assigned_to, purpose, notes)
            VALUES (%s, %s, %s, COALESCE(%s, now()), %s, %s, %s)
            RETURNING assignment_id
            """,
            (
                payload.asset_id,
                payload.person_id,
                payload.location_id,
                payload.assigned_from,
                payload.assigned_to,
                payload.purpose,
                payload.notes,
            ),
        )
        assignment_id = cur.fetchone()[0]
        conn.commit()
        return {"assignment_id": assignment_id, "actor": user.username}
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.put("/assignments/{assignment_id}")
def update_assignment(
    assignment_id: int,
    payload: AssignmentUpdate,
    conn=Depends(get_connection),
    user: User = Depends(require_role(["admin", "operator"])),
) -> Dict[str, Any]:
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")

    cur = conn.cursor()
    set_parts = [f"{col} = %s" for col in data.keys()]
    values = list(data.values()) + [assignment_id]
    try:
        cur.execute(
            f"UPDATE itam.asset_assignments SET {', '.join(set_parts)} WHERE assignment_id = %s RETURNING assignment_id",
            values,
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
        conn.commit()
        return {"assignment_id": row[0], "actor": user.username}
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.delete("/assignments/{assignment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_assignment(
    assignment_id: int,
    conn=Depends(get_connection),
    _: User = Depends(require_role(["admin"])),
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM itam.asset_assignments WHERE assignment_id = %s RETURNING assignment_id",
            (assignment_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)
