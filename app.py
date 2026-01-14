# app.py
# Backend for CCA Matcher (anonymous tracking + stats)
# Run: uvicorn app:app --reload --port 8000

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# -----------------------------
# Config
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "cca_matcher.db")

ALLOWED_ORIGINS = [
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "null",  # when opening index.html directly via file:// some browsers send Origin: null
]

app = FastAPI(title="CCA Matcher Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# DB helpers
# -----------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    """Create tables if they don't exist."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS generate_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                student_id TEXT NOT NULL,
                category TEXT,
                time TEXT,
                venue TEXT,
                typepref TEXT,
                interests TEXT,
                results_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS click_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                student_id TEXT NOT NULL,
                cca_name TEXT NOT NULL
            )
            """
        )
        conn.commit()


@app.on_event("startup")
def on_startup():
    init_db()


# -----------------------------
# Request models
# -----------------------------
class ClickEvent(BaseModel):
    student_id: str = Field(..., min_length=1, max_length=200)
    cca_name: str = Field(..., min_length=1, max_length=300)


class GenerateResult(BaseModel):
    name: str
    desc: str | None = None
    cat: str | None = None
    venue: str | None = None
    time: str | None = None
    type: str | None = None


class GenerateEvent(BaseModel):
    student_id: str = Field(..., min_length=1, max_length=200)
    category: str | None = None
    time: str | None = None
    venue: str | None = None
    typepref: str | None = None
    interests: str | None = None
    results: List[GenerateResult] = []


# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/track/click")
def track_click(payload: ClickEvent):
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO click_events (ts, student_id, cca_name)
            VALUES (?, ?, ?)
            """,
            (now_iso(), payload.student_id, payload.cca_name),
        )
        conn.commit()
    return {"ok": True}


@app.post("/track/generate")
def track_generate(payload: GenerateEvent):
    init_db()
    results_json = json.dumps([r.model_dump() for r in payload.results], ensure_ascii=False)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO generate_events (ts, student_id, category, time, venue, typepref, interests, results_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_iso(),
                payload.student_id,
                payload.category,
                payload.time,
                payload.venue,
                payload.typepref,
                payload.interests,
                results_json,
            ),
        )
        conn.commit()
    return {"ok": True}


@app.get("/stats/summary")
def stats_summary():
    """
    Returns:
    - totals for generates and clicks
    - top clicked CCAs
    - distributions for category/time/venue/typepref
    """
    init_db()

    def top_dist(col: str):
        # Use a fresh connection each time (prevents "closed database" issues)
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # Quote column names (time is a common conflict)
            qcol = f'"{col}"'
            rows = cur.execute(
                f"""
                SELECT {qcol} AS value, COUNT(*) AS c
                FROM generate_events
                GROUP BY {qcol}
                ORDER BY c DESC
                LIMIT 10
                """
            ).fetchall()

            return [{"value": r["value"], "count": r["c"]} for r in rows]

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        total_generates = cur.execute("SELECT COUNT(*) AS n FROM generate_events").fetchone()["n"]
        total_clicks = cur.execute("SELECT COUNT(*) AS n FROM click_events").fetchone()["n"]

        top_ccas = cur.execute(
            """
            SELECT cca_name, COUNT(*) AS c
            FROM click_events
            GROUP BY cca_name
            ORDER BY c DESC
            LIMIT 10
            """
        ).fetchall()

    return {
        "totals": {"generates": total_generates, "clicks": total_clicks},
        "top_clicked_ccas": [{"cca": r["cca_name"], "count": r["c"]} for r in top_ccas],
        "distributions": {
            "category": top_dist("category"),
            "time": top_dist("time"),
            "venue": top_dist("venue"),
            "typepref": top_dist("typepref"),
        },
    }


# Optional: silence favicon 404s
@app.get("/favicon.ico")
def favicon():
    # Return 204 No Content (prevents noisy 404 logs)
    from fastapi import Response
    return Response(status_code=204)
