import os
import subprocess
import sys
from urllib.parse import urlparse, unquote

import pg8000.dbapi as pg
import pytest
from fastapi.testclient import TestClient

from app.main import app

TEST_USERS = "admin:adminpass:admin;operator:oppass:operator;viewer:viewpass:viewer"


def _connect(url: str):
    p = urlparse(url)
    return pg.connect(
        user=unquote(p.username),
        password=unquote(p.password) if p.password else None,
        host=p.hostname,
        port=p.port or 5432,
        database=p.path.lstrip("/"),
        ssl_context=True,
    )


@pytest.fixture(scope="session")
def test_db_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set")
    os.environ["DATABASE_URL"] = url
    os.environ["APP_USERS"] = TEST_USERS
    os.environ["APP_SECRET_KEY"] = "test-secret"
    os.environ["APP_DEMO"] = "0"
    return url


@pytest.fixture(scope="session", autouse=True)
def reset_db(test_db_url):
    env = os.environ.copy()
    env["TEST_DATABASE_URL"] = test_db_url
    env["RESET_ITAM_SCHEMA"] = "1"
    subprocess.run([sys.executable, "smoke_test.py"], check=True, env=env)


@pytest.fixture()
def client(test_db_url):
    return TestClient(app)


@pytest.fixture()
def db_conn(test_db_url):
    conn = _connect(test_db_url)
    yield conn
    conn.close()


def get_token(client: TestClient, username: str, password: str) -> str:
    resp = client.post("/auth/token", auth=(username, password))
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def get_lookup_id(conn, table: str, name: str, id_col: str = "id") -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT {id_col}, name FROM itam.{table} WHERE name = %s", (name,))
    row = cur.fetchone()
    assert row, f"{name} not found in {table}"
    return row[0]


def test_viewer_can_read_but_cannot_write(client):
    token = get_token(client, "viewer", "viewpass")
    headers = {"Authorization": f"Bearer {token}"}

    resp = client.get("/assets", headers=headers)
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

    resp = client.post("/assets", headers=headers, json={"asset_tag": "X", "type_id": 1, "status_id": 1})
    assert resp.status_code == 403


def test_status_transition_guard(client, db_conn):
    token = get_token(client, "operator", "oppass")
    headers = {"Authorization": f"Bearer {token}"}
    type_id = get_lookup_id(db_conn, "asset_types", "Laptop", "type_id")
    status_in_stock = get_lookup_id(db_conn, "asset_status", "in_stock", "status_id")
    status_retired = get_lookup_id(db_conn, "asset_status", "retired", "status_id")
    status_in_use = get_lookup_id(db_conn, "asset_status", "in_use", "status_id")

    resp = client.post(
        "/assets",
        headers=headers,
        json={
            "asset_tag": "TEST-STATUS-1",
            "type_id": type_id,
            "status_id": status_in_stock,
            "description": "tmp asset",
        },
    )
    assert resp.status_code == 201, resp.text
    asset_id = resp.json()["asset_id"]

    resp = client.put(f"/assets/{asset_id}", headers=headers, json={"status_id": status_retired})
    assert resp.status_code == 200, resp.text

    resp = client.put(f"/assets/{asset_id}", headers=headers, json={"status_id": status_in_use})
    assert resp.status_code == 400


def test_assignment_rejects_retired(client, db_conn):
    token = get_token(client, "operator", "oppass")
    headers = {"Authorization": f"Bearer {token}"}
    type_id = get_lookup_id(db_conn, "asset_types", "Laptop", "type_id")
    status_retired = get_lookup_id(db_conn, "asset_status", "retired", "status_id")
    location_id = get_lookup_id(db_conn, "locations", "Einrichtung 01 Standort", "location_id")

    resp = client.post(
        "/assets",
        headers=headers,
        json={
            "asset_tag": "TEST-RET-ASSIGN",
            "type_id": type_id,
            "status_id": status_retired,
        },
    )
    assert resp.status_code == 201, resp.text
    asset_id = resp.json()["asset_id"]

    resp = client.post(
        "/assignments",
        headers=headers,
        json={"asset_id": asset_id, "location_id": location_id, "purpose": "should fail"},
    )
    assert resp.status_code == 400


def test_lookups_contain_facilities(client):
    token = get_token(client, "admin", "adminpass")
    headers = {"Authorization": f"Bearer {token}"}
    resp = client.get("/lookups", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["org_units"]) >= 50
