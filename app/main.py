from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from .db import get_connection
from .schemas import (
    AssetCreate,
    AssetUpdate,
    AssignmentCreate,
    AssignmentUpdate,
)


app = FastAPI(title="Assetman API", version="0.1.0")


def db_error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@app.get("/health")
def health(conn=Depends(get_connection)) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute("SELECT 1")
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="DB check failed")
    return {"status": "ok", "db": row[0]}


@app.get("/assets")
def list_assets(conn=Depends(get_connection)) -> JSONResponse:
    cur = conn.cursor()
    cur.execute(
        """
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
          loc.rack_unit AS location_rack_unit
        FROM itam.assets a
        JOIN itam.asset_types atype ON atype.type_id = a.type_id
        JOIN itam.asset_status ast ON ast.status_id = a.status_id
        LEFT JOIN itam.asset_assignments aa
          ON aa.asset_id = a.asset_id AND aa.assigned_to IS NULL
        LEFT JOIN itam.people per ON per.person_id = aa.person_id
        LEFT JOIN itam.locations loc ON loc.location_id = aa.location_id
        ORDER BY a.asset_tag
        """
    )
    cols = [desc[0] for desc in cur.description]
    data: List[Dict[str, Any]] = [dict(zip(cols, row)) for row in cur.fetchall()]
    return JSONResponse(content=jsonable_encoder(data))


@app.post("/assets", status_code=status.HTTP_201_CREATED)
def create_asset(payload: AssetCreate, conn=Depends(get_connection)) -> Dict[str, Any]:
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
    asset_id: int, payload: AssetUpdate, conn=Depends(get_connection)
) -> Dict[str, Any]:
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")

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
def delete_asset(asset_id: int, conn=Depends(get_connection)) -> None:
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
    payload: AssignmentCreate, conn=Depends(get_connection)
) -> Dict[str, Any]:
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
        return {"assignment_id": assignment_id}
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.put("/assignments/{assignment_id}")
def update_assignment(
    assignment_id: int, payload: AssignmentUpdate, conn=Depends(get_connection)
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
        return {"assignment_id": row[0]}
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise db_error(exc)


@app.delete("/assignments/{assignment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_assignment(assignment_id: int, conn=Depends(get_connection)) -> None:
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
