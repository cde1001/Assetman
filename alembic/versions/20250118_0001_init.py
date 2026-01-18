"""Initial schema baseline from schema.sql."""

from pathlib import Path

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "20250118_0001_init"
down_revision = None
branch_labels = None
depends_on = None


def split_sql(sql_text: str):
    """Split SQL by top-level semicolons; keep dollar-quoted/quoted blocks intact."""
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


def schema_has_tables(conn) -> bool:
    result = conn.exec_driver_sql(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'itam';"
    )
    return result.scalar_one() > 0


def upgrade():
    conn = op.get_bind()
    if schema_has_tables(conn):
        # already present; assume baseline loaded (use alembic stamp for existing DB)
        return

    schema_path = Path(__file__).resolve().parents[2] / "schema.sql"
    if not schema_path.exists():
        raise RuntimeError("schema.sql not found for initial migration")

    sql_text = schema_path.read_text(encoding="utf-8")
    for stmt in split_sql(sql_text):
        conn.exec_driver_sql(text(stmt))


def downgrade():
    conn = op.get_bind()
    conn.exec_driver_sql("DROP SCHEMA IF EXISTS itam CASCADE;")
