from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import json
import os
import time
from typing import Any, Dict, List, Optional

DB_PATH = os.environ.get("DB_PATH", "stats.db")

app = FastAPI()

# CORS: allows GitHub Pages (HTTPS) to call Render
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can later restrict to your GitHub Pages domain
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
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
        )
    """)
    conn.commit()
    conn.close()

def norm(s: Any) -> str:
    return (str(s).strip()) if s is not None else ""

def safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []

def count_map_add(m: Dict[str, int], key: Optional[str], inc: int = 1):
    k = key if key and str(key).strip() else "(blank)"
    m[k] = m.get(k, 0) + inc

def sort_dict(d: Dict[str, int]) -> Dict[str, int]:
    return dict(sorted(d.items(), key=lambda kv: (-kv[1], kv[0].lower())))

@app.get("/")
def home():
    return PlainTextResponse("OK")

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3, json, os, time
from typing import Any, Dict, List, Optional

DB_PATH = os.environ.get("DB_PATH", "stats.db")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
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
        )
    """)
    conn.commit()
    conn.close()

def norm(x: Any) -> str:
    return (str(x).strip()) if x is not None else ""

def safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []

@app.get("/")
def home():
    return {"ok": True}

@app.post("/api/events")
async def api_events(request: Request):
    init_db()

    # ✅ SAFE JSON PARSE (no more 500 on empty body)
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

    ts = int(time.time() * 1000)

    category_selected = norm(data.get("categorySelected")) or None
    activity_type_selected = data.get("activityTypeSelected")
    activity_type_selected = None if activity_type_selected is None else norm(activity_type_selected)

    grade = data.get("grade")
    grade = None if grade is None else norm(grade)

    gender = norm(data.get("gender")) or None

    interests = safe_list(data.get("interests"))
    shown_ccas = safe_list(data.get("shownCCAs"))
    shortlisted = norm(data.get("shortlistedCCA")) or None

    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO events (
            ts, event_type, category_selected, activity_type_selected,
            grade, gender, interests_json, shown_ccas_json, shortlisted_cca
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ts,
        event_type,
        category_selected,
        activity_type_selected,
        grade,
        gender,
        json.dumps(interests, ensure_ascii=False),
        json.dumps(shown_ccas, ensure_ascii=False),
        shortlisted
    ))
    conn.commit()
    conn.close()

    return {"ok": True}


@app.get("/api/stats")
def api_stats():
    init_db()
    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY ts DESC").fetchall()
    conn.close()

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
        et = r["event_type"]

        if et == "generate":
            generate_events += 1
            count_map_add(categories, r["category_selected"])
            count_map_add(activity_types, r["activity_type_selected"] or "(n/a)")
            count_map_add(grades, r["grade"])
            count_map_add(genders, r["gender"] or "Any")

            try:
                ints = json.loads(r["interests_json"] or "[]")
            except:
                ints = []
            for t in ints:
                tok = norm(t).lower()
                if tok:
                    count_map_add(interests, tok)

        elif et == "shortlist":
            shortlist_events += 1
            name = r["shortlisted_cca"]
            if name:
                count_map_add(shortlisted, name)

    return {
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
