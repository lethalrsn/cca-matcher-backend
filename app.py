from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

import os, json, time
from typing import Any, Dict, List, Optional

# -----------------------------
# CONFIG
# -----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
SQLITE_PATH = os.environ.get("DB_PATH", "stats.db")
USE_POSTGRES = bool(DATABASE_URL)

app = FastAPI(title="CCA Matcher Backend", version="3.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # you can restrict later
    allow_credentials=False,
    allow_methods=["*"],          # IMPORTANT: allows DELETE + OPTIONS preflight
    allow_headers=["*"],
)

# -----------------------------
# HELPERS
# -----------------------------
def norm(x: Any) -> str:
    return (str(x).strip()) if x is not None else ""

def safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []

def count_map_add(m: Dict[str, int], key: Optional[str], inc: int = 1) -> None:
    k = key if key and str(key).strip() else "(blank)"
    m[k] = m.get(k, 0) + inc

def sort_dict(d: Dict[str, int]) -> Dict[str, int]:
    return dict(sorted(d.items(), key=lambda kv: (-kv[1], kv[0].lower())))

# -----------------------------
# DB LAYER (Postgres or SQLite)
# -----------------------------
def init_db() -> None:
    if USE_POSTGRES:
        import psycopg
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id SERIAL PRIMARY KEY,
                        ts BIGINT NOT NULL,
                        event_type TEXT NOT NULL,

                        category_selected TEXT,
                        activity_type_selected TEXT,
                        grade TEXT,
                        gender TEXT,

                        interests_json TEXT,
                        shown_ccas_json TEXT,
                        shortlisted_cca TEXT
                    );
                """)
            conn.commit()
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                event_type TEXT NOT NULL,

                category_selected TEXT,
                activity_type_selected TEXT,
                grade TEXT,
                gender TEXT,

                interests_json TEXT,
                shown_ccas_json TEXT,
                shortlisted_cca TEXT
            );
        """)
        conn.commit()
        conn.close()

def insert_event(row: Dict[str, Any]) -> int:
    """
    Inserts an event. Returns total event count after insert (for debugging).
    """
    if USE_POSTGRES:
        import psycopg
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO events (
                        ts, event_type, category_selected, activity_type_selected,
                        grade, gender, interests_json, shown_ccas_json, shortlisted_cca
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    row["ts"], row["event_type"], row["category_selected"], row["activity_type_selected"],
                    row["grade"], row["gender"], row["interests_json"], row["shown_ccas_json"], row["shortlisted_cca"]
                ))
                cur.execute("SELECT COUNT(*) FROM events;")
                total = cur.fetchone()[0]
            conn.commit()
        return int(total)
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO events (
                ts, event_type, category_selected, activity_type_selected,
                grade, gender, interests_json, shown_ccas_json, shortlisted_cca
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            row["ts"], row["event_type"], row["category_selected"], row["activity_type_selected"],
            row["grade"], row["gender"], row["interests_json"], row["shown_ccas_json"], row["shortlisted_cca"]
        ))
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM events;")
        total = cur.fetchone()[0]
        conn.close()
        return int(total)

def fetch_all_events() -> List[Dict[str, Any]]:
    if USE_POSTGRES:
        import psycopg
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts,event_type,category_selected,activity_type_selected,grade,gender,
                           interests_json,shown_ccas_json,shortlisted_cca
                    FROM events
                    ORDER BY ts DESC
                """)
                rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "ts": r[0], "event_type": r[1],
                "category_selected": r[2], "activity_type_selected": r[3],
                "grade": r[4], "gender": r[5],
                "interests_json": r[6], "shown_ccas_json": r[7],
                "shortlisted_cca": r[8],
            })
        return out
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT ts,event_type,category_selected,activity_type_selected,grade,gender,
                   interests_json,shown_ccas_json,shortlisted_cca
            FROM events
            ORDER BY ts DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]

def clear_all_events() -> int:
    """
    Deletes all events and returns how many rows were deleted (best-effort).
    """
    if USE_POSTGRES:
        import psycopg
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # RETURNING gives us deleted count reliably on Postgres
                cur.execute("DELETE FROM events RETURNING 1;")
                deleted = cur.rowcount  # should be number of deleted rows
            conn.commit()
        return int(deleted or 0)
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM events;")
        deleted = cur.rowcount
        conn.commit()
        conn.close()
        return int(deleted or 0)

# -----------------------------
# ROUTES
# -----------------------------
@app.get("/", response_class=PlainTextResponse)
def home():
    return "OK"

@app.post("/api/events")
async def api_events(request: Request):
    init_db()

    # Safe JSON parse (never 500)
    try:
        data = await request.json()
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}

    event_type = norm(data.get("eventType"))
    if event_type not in ("generate", "shortlist"):
        return JSONResponse(
            {"ok": False, "error": "Invalid or missing eventType (use 'generate' or 'shortlist')"},
            status_code=400
        )

    row = {
        "ts": int(time.time() * 1000),
        "event_type": event_type,
        "category_selected": (norm(data.get("categorySelected")) or None),
        "activity_type_selected": (None if data.get("activityTypeSelected") is None else norm(data.get("activityTypeSelected"))),
        "grade": (None if data.get("grade") is None else norm(data.get("grade"))),
        "gender": (norm(data.get("gender")) or None),
        "interests_json": json.dumps(safe_list(data.get("interests"))[:200], ensure_ascii=False),
        "shown_ccas_json": json.dumps(safe_list(data.get("shownCCAs"))[:50], ensure_ascii=False),
        "shortlisted_cca": (norm(data.get("shortlistedCCA")) or None),
    }

    total = insert_event(row)
    return {"ok": True, "totalEventsNow": total, "storage": ("postgres" if USE_POSTGRES else "sqlite")}

@app.delete("/api/stats")
def api_clear_stats():
    init_db()
    try:
        deleted = clear_all_events()
        return {"ok": True, "status": "cleared", "deleted": deleted, "storage": ("postgres" if USE_POSTGRES else "sqlite")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def api_stats():
    init_db()
    rows = fetch_all_events()

    total_events = len(rows)

    categories: Dict[str, int] = {}
    activity_types: Dict[str, int] = {}
    grades: Dict[str, int] = {}
    genders: Dict[str, int] = {}
    interests: Dict[str, int] = {}
    shortlisted: Dict[str, int] = {}

    generate_events = 0
    shortlist_events = 0

    for r in rows:
        et = r.get("event_type")

        if et == "generate":
            generate_events += 1
            count_map_add(categories, r.get("category_selected"))
            # If not Activity, activity_type_selected may be null → treat as (n/a)
            count_map_add(activity_types, r.get("activity_type_selected") or "(n/a)")
            count_map_add(grades, r.get("grade"))
            count_map_add(genders, r.get("gender") or "Any")

            try:
                ints = json.loads(r.get("interests_json") or "[]")
            except Exception:
                ints = []

            for t in ints:
                tok = norm(t).lower()
                if tok:
                    count_map_add(interests, tok)

        elif et == "shortlist":
            shortlist_events += 1
            name = r.get("shortlisted_cca")
            if name:
                count_map_add(shortlisted, name)

    return {
        "storage": ("postgres" if USE_POSTGRES else "sqlite"),
        "totalEvents": total_events,
        "generateEvents": generate_events,
        "shortlistEvents": shortlist_events,
        "categories": sort_dict(categories),
        "activityTypes": sort_dict(activity_types),
        "grades": sort_dict(grades),
        "genders": sort_dict(genders),
        "interests": sort_dict(interests),
        "shortlisted": sort_dict(shortlisted),
    }
