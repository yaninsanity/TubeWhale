"""Utility helpers for read-only introspection of the external CLI SQLite DB.

Design goals:
- Never mutate external DB: only SELECT & PRAGMA.
- Fail closed (return empty structures) if file missing or unreadable.
- Small surface consumed by admin views for overview, preview, export.
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Iterable

from django.conf import settings


DEFAULT_ENV_DB_KEY = "DB_PATH"
MAX_PREVIEW_ROWS = 200  # hard cap to avoid heavy UI loads
MAX_EXPORT_ROWS = 10000  # safety cap


def resolve_cli_db_path() -> str | None:
    """Return path to CLI DB (can be outside Django DB) or None if not found.

    Priority:
    1. .env DB_PATH (already loaded in process env by dotenv in many places)
    2. settings.BASE_DIR / value if relative
    3. If file does not exist -> None
    """
    path = os.environ.get(DEFAULT_ENV_DB_KEY) or getattr(settings, "CLI_DB_PATH", None)
    if not path:
        return None
    if not os.path.isabs(path):
        path = os.path.join(settings.BASE_DIR, path)
    if not os.path.exists(path) or not os.path.isfile(path):
        return None
    return path


def _connect(db_path: str):
    # isolation_level=None => autocommit, safer for read-only selects
    return sqlite3.connect(db_path, timeout=2)


def list_tables(include_counts: bool = True) -> List[Dict[str, Any]]:
    path = resolve_cli_db_path()
    if not path:
        return []
    try:
        con = _connect(path)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        results = []
        for t in tables:
            if include_counts:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    count = cur.fetchone()[0]
                except Exception:
                    count = None
            else:
                count = None
            results.append({"name": t, "row_count": count})
        return results
    except Exception:
        return []
    finally:
        try:
            con.close()  # type: ignore
        except Exception:
            pass


def table_schema(table: str) -> List[Dict[str, Any]]:
    path = resolve_cli_db_path()
    if not path:
        return []
    try:
        con = _connect(path)
        cur = con.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = []
        for cid, name, col_type, notnull, dflt_value, pk in cur.fetchall():
            cols.append({
                "cid": cid,
                "name": name,
                "type": col_type,
                "notnull": bool(notnull),
                "default": dflt_value,
                "primary_key": bool(pk),
            })
        return cols
    except Exception:
        return []
    finally:
        try:
            con.close()  # type: ignore
        except Exception:
            pass


def preview_table(table: str, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    limit = min(MAX_PREVIEW_ROWS, max(1, limit))
    offset = max(0, offset)
    path = resolve_cli_db_path()
    if not path:
        return {"rows": [], "columns": [], "error": "DB file not found"}
    try:
        con = _connect(path)
        cur = con.cursor()
        cur.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?", (limit, offset))
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []
        return {"rows": rows, "columns": columns, "error": None}
    except Exception as e:
        return {"rows": [], "columns": [], "error": str(e)}
    finally:
        try:
            con.close()  # type: ignore
        except Exception:
            pass


def export_table(table: str, fmt: str = "json", limit: Optional[int] = None) -> Dict[str, Any]:
    fmt = fmt.lower()
    if fmt not in {"json", "ndjson", "csv"}:
        return {"error": f"Unsupported format: {fmt}"}
    if limit is None:
        limit = MAX_EXPORT_ROWS
    limit = min(MAX_EXPORT_ROWS, max(1, limit))
    path = resolve_cli_db_path()
    if not path:
        return {"error": "DB file not found"}
    try:
        con = _connect(path)
        cur = con.cursor()
        cur.execute(f"SELECT * FROM {table} LIMIT ?", (limit,))
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []

        import json, csv, io
        if fmt == "json":
            data = [dict(zip(columns, r)) for r in rows]
            content = json.dumps(data, ensure_ascii=False, indent=2)
            mime = "application/json"
            ext = "json"
        elif fmt == "ndjson":
            buf = io.StringIO()
            for r in rows:
                buf.write(json.dumps(dict(zip(columns, r)), ensure_ascii=False) + "\n")
            content = buf.getvalue()
            mime = "application/x-ndjson"
            ext = "ndjson"
        else:  # csv
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            writer.writerows(rows)
            content = buf.getvalue()
            mime = "text/csv"
            ext = "csv"
        return {"error": None, "content": content, "mime": mime, "ext": ext, "rows": len(rows), "columns": columns}
    except Exception as e:
        return {"error": str(e)}
    finally:
        try:
            con.close()  # type: ignore
        except Exception:
            pass
