import os
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse

from .auth import User, demo_token, is_demo_mode, login, require_role
from .db import get_connection
from .schemas import AssetCreate, AssetUpdate, AssignmentCreate, AssignmentUpdate

ALLOWED_STATUS_TRANSITIONS = {
    'in_stock': {'in_use', 'repair', 'retired'},
    'in_use': {'in_stock', 'repair', 'retired'},
    'repair': {'in_use', 'retired'},
    'retired': set(),
}

SORT_COLUMNS = {
    'asset_tag': 'a.asset_tag',
    'status': 'ast.name',
    'type': 'atype.name',
    'owner': 'ou.name',
    'location': 'loc.name',
    'updated': 'a.updated_at',
}

app = FastAPI(title='Assetman API', version='0.3.0')


def db_error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def get_status_name(conn, status_id: int) -> str:
    cur = conn.cursor()
    cur.execute('SELECT name FROM itam.asset_status WHERE status_id = %s', (status_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Invalid status_id')
    return row[0]


def get_asset_status(conn, asset_id: int) -> Tuple[int, str]:
    cur = conn.cursor()
    cur.execute(
        'SELECT a.status_id, s.name FROM itam.assets a JOIN itam.asset_status s ON s.status_id = a.status_id WHERE a.asset_id = %s',
        (asset_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Asset not found')
    return row[0], row[1]


@app.post('/auth/token')
def auth_token(token: str = Depends(login)) -> Dict[str, str]:
    return {'access_token': token, 'token_type': 'bearer'}


@app.get('/auth/demo-token')
def auth_demo_token() -> Dict[str, str]:
    return {'access_token': demo_token(), 'token_type': 'bearer'}


@app.get('/auth/me')
def auth_me(user: User = Depends(require_role(['admin', 'operator', 'viewer']))) -> Dict[str, str]:
    return {'username': user.username, 'role': user.role}


@app.get('/health')
def health(conn=Depends(get_connection)) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute('SELECT 1')
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail='DB check failed')
    return {'status': 'ok', 'db': row[0]}



@app.get("/")
def home() -> HTMLResponse:
    current_demo = is_demo_mode()
    mode_text = "Demo" if current_demo else "Live"
    demo_flag = "true" if current_demo else "false"
    html = """
    <!doctype html>
    <html lang=\"de\">
    <head>
      <meta charset=\"utf-8\">
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
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
          font-family: \"Helvetica Neue\", Arial, sans-serif;
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
          max-width: 1140px;
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
        main { max-width: 1140px; margin: 20px auto; padding: 0 16px 40px; }
        .panel { background: var(--card); border-radius: 14px; box-shadow: var(--shadow); padding: 18px; }
        .grid { display: grid; grid-template-columns: 2fr 1.1fr; gap: 14px; }
        .filters { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin-bottom: 12px; }
        input, select, textarea, button { width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid #d8e2eb; font-size: 14px; outline: none; background: #fff; }
        input:focus, select:focus, textarea:focus { border-color: var(--accent-2); box-shadow: 0 0 0 3px rgba(17,138,178,0.12); }
        button { background: var(--accent); color: #fff; border: none; cursor: pointer; font-weight: 600; }
        button:disabled { opacity: 0.5; cursor: not-allowed; }
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
        .login-card { background: rgba(255,255,255,0.12); padding: 10px 12px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.2); min-width: 220px; }
        .login-card input { width: 100%; margin-top: 6px; }
        .section-title { margin: 8px 0; font-weight: 700; font-size: 14px; color: var(--muted); }
        .muted { color: var(--muted); font-size: 12px; }
        .row-selected { background: #eef7ff; }
        @media (max-width: 900px) {
          .grid { grid-template-columns: 1fr; }
          .hero { flex-direction: column; align-items: flex-start; }
          header { padding: 24px 16px; }
        }
      </style>
    </head>
    <body>
      <header>
        <div class=\"hero\">
          <div>
            <div class=\"badge\">Assetman  Diakonissenhaus inspiriert</div>
            <h1>Assets im Blick behalten</h1>
            <p>Inventar, Zuweisungen und Lizenzen fuer 50 Einrichtungen  klar strukturiert, schnell filterbar.</p>
          </div>
          <div class=\"login-card\">
            <div id=\"mode-text\">Modus: MODE_TEXT_PLACEHOLDER</div>
            <div style=\"display:flex; gap:6px; margin-top:6px;\">
              <input id=\"user\" placeholder=\"User\" aria-label=\"User\">
              <input id=\"pass\" placeholder=\"Passwort\" type=\"password\" aria-label=\"Passwort\">
            </div>
            <button id=\"login-btn\" style=\"margin-top:8px;\">Login</button>
            <div id=\"login-msg\" class=\"muted\"></div>
          </div>
        </div>
      </header>
      <main>
        <div class=\"panel grid\">
          <div>
            <div class=\"filters\">
              <input id=\"q\" placeholder=\"Suche (Tag, Seriennr., Modell)\">
              <select id=\"status\"><option value=\"\">Status</option></select>
              <select id=\"type\"><option value=\"\">Typ</option></select>
              <select id=\"owner\"><option value=\"\">Einrichtung</option></select>
              <select id=\"sort\">
                <option value=\"asset_tag\">Sortierung: Asset-Tag</option>
                <option value=\"-updated\">Sortierung: Zuletzt aktualisiert</option>
                <option value=\"status\">Status</option>
                <option value=\"type\">Typ</option>
                <option value=\"owner\">Einrichtung</option>
              </select>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Asset</th><th>Typ</th><th>Status</th><th>Ort/Zuweisung</th><th>Seriennr.</th><th>Org-Einheit</th>
                </tr>
              </thead>
              <tbody id=\"rows\">
                <tr><td colspan=\"6\">Lade Daten...</td></tr>
              </tbody>
            </table>
            <div class=\"footer\">Rollen: admin, operator, viewer. Live-Modus: APP_USERS, APP_SECRET_KEY setzen, APP_DEMO=0.</div>
          </div>
          <div>
            <div class=\"section-title\">Auswahl / Historie</div>
            <div id=\"selected\" class=\"muted\">Kein Asset ausgewaehlt.</div>
            <div id=\"history\" class=\"muted\" style=\"margin-top:8px;\">-</div>
            <div class=\"section-title\" style=\"margin-top:12px;\">Asset anlegen (admin/operator)</div>
            <div style=\"display:grid; grid-template-columns: repeat(auto-fit,minmax(140px,1fr)); gap:8px;\">
              <input id=\"new-tag\" placeholder=\"Asset-Tag\">
              <select id=\"new-type\"></select>
              <select id=\"new-status\"></select>
              <select id=\"new-owner\"></select>
            </div>
            <textarea id=\"new-notes\" placeholder=\"Beschreibung/Notizen\" style=\"margin-top:8px; height:70px;\"></textarea>
            <button id=\"create-btn\" style=\"margin-top:8px;\">Anlegen</button>
            <div class=\"section-title\" style=\"margin-top:14px;\">Status aendern</div>
            <select id=\"update-status\"></select>
            <button id=\"update-btn\" style=\"margin-top:6px;\">Status speichern</button>
            <div class=\"section-title\" style=\"margin-top:14px;\">Assignment anlegen</div>
            <div style=\"display:grid; grid-template-columns: repeat(auto-fit,minmax(140px,1fr)); gap:8px;\">
              <input id=\"assign-person\" placeholder=\"person_id\">
              <input id=\"assign-location\" placeholder=\"location_id\">
              <input id=\"assign-purpose\" placeholder=\"Purpose/Notiz\">
            </div>
            <button id=\"assign-btn\" style=\"margin-top:6px;\">Assignment speichern</button>
            <div id=\"action-msg\" class=\"muted\" style=\"margin-top:6px;\"></div>
          </div>
        </div>
      </main>
      <script>
        const rows = document.getElementById('rows');
        const q = document.getElementById('q');
        const statusSel = document.getElementById('status');
        const typeSel = document.getElementById('type');
        const ownerSel = document.getElementById('owner');
        const sort = document.getElementById('sort');
        const selectedBox = document.getElementById('selected');
        const historyBox = document.getElementById('history');
        const createBtn = document.getElementById('create-btn');
        const updateBtn = document.getElementById('update-btn');
        const assignBtn = document.getElementById('assign-btn');
        const newTag = document.getElementById('new-tag');
        const newType = document.getElementById('new-type');
        const newStatus = document.getElementById('new-status');
        const newOwner = document.getElementById('new-owner');
        const newNotes = document.getElementById('new-notes');
        const updateStatus = document.getElementById('update-status');
        const assignPerson = document.getElementById('assign-person');
        const assignLocation = document.getElementById('assign-location');
        const assignPurpose = document.getElementById('assign-purpose');
        const actionMsg = document.getElementById('action-msg');
        const loginBtn = document.getElementById('login-btn');
        const loginMsg = document.getElementById('login-msg');
        const userInput = document.getElementById('user');
        const passInput = document.getElementById('pass');
        const isDemo = DEMO_FLAG_PLACEHOLDER;
        let token = null;
        let currentUser = null;
        let lookups = {types:[], statuses:[], org_units:[], locations:[]};
        let selectedAsset = null;

        function setAction(msg) { actionMsg.textContent = msg || ''; }
        function setLogin(msg) { loginMsg.textContent = msg || ''; }

        async function fetchTokenDemo() {
          const res = await fetch('/auth/demo-token');
          if (!res.ok) throw new Error('Demo-Token fehlgeschlagen');
          const data = await res.json();
          token = data.access_token;
          await fetchMe();
        }

        async function login() {
          if (!userInput.value || !passInput.value) { setLogin('User/Pass erforderlich'); return; }
          const hdrs = { Authorization: 'Basic ' + btoa(userInput.value + ':' + passInput.value) };
          const res = await fetch('/auth/token', { method: 'POST', headers: hdrs });
          if (!res.ok) { setLogin('Login fehlgeschlagen'); return; }
          const data = await res.json();
          token = data.access_token;
          setLogin('OK');
          await fetchMe();
          await loadLookups();
          await load();
        }

        async function fetchMe() {
          if (!token) return;
          const res = await fetch('/auth/me', { headers: { Authorization: 'Bearer ' + token } });
          if (!res.ok) { currentUser = null; return; }
          currentUser = await res.json();
          setAction('Angemeldet als ' + currentUser.username + ' (' + currentUser.role + ')');
          const disableWrites = !currentUser || currentUser.role === 'viewer';
          [createBtn, updateBtn, assignBtn, newTag, newType, newStatus, newOwner, newNotes, updateStatus, assignPerson, assignLocation, assignPurpose].forEach(el => {
            if (el) el.disabled = disableWrites && el !== newNotes;
          });
        }

        async function ensureToken() {
          if (token) return;
          if (isDemo) { await fetchTokenDemo(); return; }
          setLogin('Bitte einloggen (Live-Modus)');
          throw new Error('Login erforderlich');
        }

        function statusPill(name) {
          const map = { 'in_use': 'use', 'in_stock': 'stock', 'repair': 'repair', 'retired': 'retired' };
          const cls = map[name] || 'stock';
          return `<span class=\"pill ${cls}\">${name}</span>`;
        }

        function fillSelect(sel, items, valueKey, labelKey, placeholder) {
          sel.innerHTML = `<option value=\"\">${placeholder}</option>` + items.map(i => `<option value=\"${i[valueKey]}\">${i[labelKey]}</option>`).join('');
        }

        async function loadLookups() {
          await ensureToken();
          const res = await fetch('/lookups', { headers: { Authorization: 'Bearer ' + token } });
          if (!res.ok) { setAction('Lookups fehlgeschlagen'); return; }
          lookups = await res.json();
          fillSelect(statusSel, lookups.statuses, 'name', 'name', 'Status');
          fillSelect(typeSel, lookups.types, 'name', 'name', 'Typ');
          fillSelect(ownerSel, lookups.org_units, 'org_unit_id', 'name', 'Einrichtung');
          fillSelect(newType, lookups.types, 'type_id', 'name', 'Typ');
          fillSelect(newStatus, lookups.statuses, 'status_id', 'name', 'Status');
          fillSelect(newOwner, lookups.org_units, 'org_unit_id', 'name', 'Einrichtung');
          fillSelect(updateStatus, lookups.statuses, 'status_id', 'name', 'Status');
        }

        async function load() {
          await ensureToken();
          const params = new URLSearchParams();
          if (q.value) params.append('q', q.value);
          if (statusSel.value) params.append('status', statusSel.value);
          if (typeSel.value) params.append('type', typeSel.value);
          if (ownerSel.value) params.append('owner_org_unit_id', ownerSel.value);
          if (sort.value) params.append('sort', sort.value);
          const res = await fetch('/assets?' + params.toString(), {
            headers: { Authorization: 'Bearer ' + token }
          });
          const data = await res.json();
          if (!Array.isArray(data)) {
            rows.innerHTML = `<tr><td colspan=\"6\">${data.detail || 'Fehler'}</td></tr>`;
            return;
          }
          if (!data.length) {
            rows.innerHTML = '<tr><td colspan=\"6\">Keine Ergebnisse</td></tr>';
            return;
          }
          rows.innerHTML = data.map(item => `
            <tr data-id=\"${item.asset_id}\">
              <td>${item.asset_tag || ''}</td>
              <td>${item.type || ''}</td>
              <td>${statusPill(item.status)}</td>
              <td>${item.person || item.location || '-'}</td>
              <td>${item.serial_number || ''}</td>
              <td>${item.owner || ''}</td>
            </tr>
          `).join('');
          rows.querySelectorAll('tr').forEach(tr => tr.addEventListener('click', () => selectAsset(tr)));
        }

        async function loadHistory(assetId) {
          const res = await fetch(`/assets/${assetId}/assignments`, {
            headers: { Authorization: 'Bearer ' + token }
          });
          if (!res.ok) { historyBox.textContent = 'Historie nicht ladbar'; return; }
          const data = await res.json();
          if (!data.length) { historyBox.textContent = 'Keine Historie'; return; }
          historyBox.innerHTML = data.map(a => {
            const target = a.person || a.location || '-';
            return `<div>${a.assigned_from || ''} -> ${a.assigned_to || ''}  ${target}  ${a.purpose || ''}</div>`;
          }).join('');
        }

        function selectAsset(tr) {
          rows.querySelectorAll('tr').forEach(r => r.classList.remove('row-selected'));
          tr.classList.add('row-selected');
          selectedAsset = tr.getAttribute('data-id');
          selectedBox.textContent = 'Asset-ID ' + selectedAsset + ' ausgewaehlt';
          loadHistory(selectedAsset);
        }

        async function createAsset() {
          await ensureToken();
          if (!newTag.value || !newType.value || !newStatus.value) { setAction('Tag, Typ, Status erforderlich'); return; }
          const payload = {
            asset_tag: newTag.value,
            type_id: Number(newType.value),
            status_id: Number(newStatus.value),
            description: newNotes.value || null,
            owner_org_unit_id: newOwner.value ... Number(newOwner.value) : null
          };
          const res = await fetch('/assets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
            body: JSON.stringify(payload)
          });
          if (!res.ok) { setAction('Asset anlegen fehlgeschlagen'); return; }
          setAction('Asset angelegt');
          newTag.value = ''; newNotes.value = '';
          await load();
        }

        async function updateAssetStatus() {
          await ensureToken();
          if (!selectedAsset) { setAction('Asset waehlen'); return; }
          if (!updateStatus.value) { setAction('Status waehlen'); return; }
          const res = await fetch(`/assets/${selectedAsset}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
            body: JSON.stringify({ status_id: Number(updateStatus.value) })
          });
          if (!res.ok) { setAction('Status-Update fehlgeschlagen'); return; }
          setAction('Status aktualisiert');
          await load();
        }

        async function createAssignment() {
          await ensureToken();
          if (!selectedAsset) { setAction('Asset waehlen'); return; }
          const payload = {
            asset_id: Number(selectedAsset),
            person_id: assignPerson.value ... Number(assignPerson.value) : null,
            location_id: assignLocation.value ... Number(assignLocation.value) : null,
            purpose: assignPurpose.value || null
          };
          const res = await fetch('/assignments', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
            body: JSON.stringify(payload)
          });
          if (!res.ok) { setAction('Assignment fehlgeschlagen'); return; }
          setAction('Assignment erstellt');
          assignPurpose.value = '';
          await loadHistory(selectedAsset);
          await load();
        }

        q.addEventListener('input', () => { clearTimeout(window._t); window._t = setTimeout(load, 200); });
        statusSel.addEventListener('change', load);
        typeSel.addEventListener('change', load);
        ownerSel.addEventListener('change', load);
        sort.addEventListener('change', load);
        createBtn.addEventListener('click', createAsset);
        updateBtn.addEventListener('click', updateAssetStatus);
        assignBtn.addEventListener('click', createAssignment);
        loginBtn.addEventListener('click', login);

        (async function init() {
          try {
            if (isDemo) {
              await fetchTokenDemo();
            } else {
              setLogin('Live-Modus: bitte einloggen');
            }
            await loadLookups();
            await load();
          } catch (e) {
            setAction('Init fehlgeschlagen: ' + e.message);
          }
        })();
      </script>
    </body>
    </html>
    """
    html = html.replace("MODE_TEXT_PLACEHOLDER", mode_text).replace("DEMO_FLAG_PLACEHOLDER", demo_flag)
    return HTMLResponse(html)


@app.get('/lookups')
def lookups(
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin', 'operator', 'viewer'])),
) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute('SELECT type_id, name, category FROM itam.asset_types ORDER BY name')
    types = [
        {'type_id': row[0], 'name': row[1], 'category': row[2]}
        for row in cur.fetchall()
    ]
    cur.execute('SELECT status_id, name, is_active FROM itam.asset_status ORDER BY name')
    statuses = [
        {'status_id': row[0], 'name': row[1], 'is_active': row[2]} for row in cur.fetchall()
    ]
    cur.execute('SELECT org_unit_id, name, parent_org_unit_id FROM itam.org_units ORDER BY name')
    orgs = [
        {'org_unit_id': row[0], 'name': row[1], 'parent_org_unit_id': row[2]}
        for row in cur.fetchall()
    ]
    cur.execute('SELECT location_id, name FROM itam.locations ORDER BY name')
    locations = [{'location_id': row[0], 'name': row[1]} for row in cur.fetchall()]
    return {'types': types, 'statuses': statuses, 'org_units': orgs, 'locations': locations}


@app.get('/org-units')
def list_org_units(
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin', 'operator', 'viewer'])),
) -> JSONResponse:
    cur = conn.cursor()
    cur.execute(
        'SELECT org_unit_id, name, parent_org_unit_id FROM itam.org_units ORDER BY name'
    )
    cols = [desc[0] for desc in cur.description]
    data = [dict(zip(cols, row)) for row in cur.fetchall()]
    return JSONResponse(content=jsonable_encoder(data))


@app.get('/assets')
def list_assets(
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin', 'operator', 'viewer'])),
    q: Optional[str] = Query(None),
    status: Optional[List[str]] = Query(None),
    type: Optional[List[str]] = Query(None),
    owner_org_unit_id: Optional[int] = Query(None),
    assigned: Optional[bool] = Query(None),
    sort: Optional[str] = Query('asset_tag'),
) -> JSONResponse:
    clauses: List[str] = []
    params: List[Any] = []
    if q:
        clauses.append(
            '(a.asset_tag ILIKE %s OR a.serial_number ILIKE %s OR a.model ILIKE %s OR a.description ILIKE %s)'
        )
        like = f"%{q}%"
        params.extend([like, like, like, like])
    if status:
        placeholders = ', '.join(['%s'] * len(status))
        clauses.append(f'ast.name IN ({placeholders})')
        params.extend(status)
    if type:
        placeholders = ', '.join(['%s'] * len(type))
        clauses.append(f'atype.name IN ({placeholders})')
        params.extend(type)
    if owner_org_unit_id:
        clauses.append('ou.org_unit_id = %s')
        params.append(owner_org_unit_id)
    if assigned is True:
        clauses.append('aa.assignment_id IS NOT NULL')
    elif assigned is False:
        clauses.append('aa.assignment_id IS NULL')

    order = SORT_COLUMNS.get(sort.lstrip('-'), SORT_COLUMNS['asset_tag'])
    direction = 'DESC' if sort.startswith('-') else 'ASC'
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''

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


@app.get('/assets/{asset_id}/assignments')
def list_asset_assignments(
    asset_id: int,
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin', 'operator', 'viewer'])),
) -> JSONResponse:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          aa.assignment_id,
          aa.assigned_from,
          aa.assigned_to,
          aa.purpose,
          aa.notes,
          per.display_name AS person,
          loc.name AS location
        FROM itam.asset_assignments aa
        LEFT JOIN itam.people per ON per.person_id = aa.person_id
        LEFT JOIN itam.locations loc ON loc.location_id = aa.location_id
        WHERE aa.asset_id = %s
        ORDER BY aa.assigned_from DESC
        """,
        (asset_id,),
    )
    cols = [desc[0] for desc in cur.description]
    data: List[Dict[str, Any]] = [dict(zip(cols, row)) for row in cur.fetchall()]
    return JSONResponse(content=jsonable_encoder(data))


@app.post('/assets', status_code=status.HTTP_201_CREATED)
def create_asset(
    payload: AssetCreate,
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin', 'operator'])),
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
        return {'asset_id': asset_id}
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.put('/assets/{asset_id}')
def update_asset(
    asset_id: int,
    payload: AssetUpdate,
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin', 'operator'])),
) -> Dict[str, Any]:
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='No fields to update')

    current_status_id, current_status_name = get_asset_status(conn, asset_id)
    if 'status_id' in data:
        new_status_name = get_status_name(conn, data['status_id'])
        allowed = ALLOWED_STATUS_TRANSITIONS.get(current_status_name, set())
        if new_status_name not in allowed and new_status_name != current_status_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Cannot transition from {current_status_name} to {new_status_name}',
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Asset not found')
        conn.commit()
        return {'asset_id': row[0]}
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.delete('/assets/{asset_id}', status_code=status.HTTP_204_NO_CONTENT)
def delete_asset(
    asset_id: int,
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin'])),
) -> None:
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM itam.assets WHERE asset_id = %s RETURNING asset_id', (asset_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Asset not found')
        conn.commit()
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.post('/assignments', status_code=status.HTTP_201_CREATED)
def create_assignment(
    payload: AssignmentCreate,
    conn=Depends(get_connection),
    user: User = Depends(require_role(['admin', 'operator'])),
) -> Dict[str, Any]:
    status_id, status_name = get_asset_status(conn, payload.asset_id)
    if status_name == 'retired':
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Asset is retired')

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
        return {'assignment_id': assignment_id, 'actor': user.username}
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.put('/assignments/{assignment_id}')
def update_assignment(
    assignment_id: int,
    payload: AssignmentUpdate,
    conn=Depends(get_connection),
    user: User = Depends(require_role(['admin', 'operator'])),
) -> Dict[str, Any]:
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='No fields to update')

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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Assignment not found')
        conn.commit()
        return {'assignment_id': row[0], 'actor': user.username}
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.delete('/assignments/{assignment_id}', status_code=status.HTTP_204_NO_CONTENT)
def delete_assignment(
    assignment_id: int,
    conn=Depends(get_connection),
    _: User = Depends(require_role(['admin'])),
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            'DELETE FROM itam.asset_assignments WHERE assignment_id = %s RETURNING assignment_id',
            (assignment_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Assignment not found')
        conn.commit()
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)
