# app.py (Postgres / shared tracking version)
# Works on Render (or any host) using DATABASE_URL env var.
# Run locally: set DATABASE_URL, then: uvicorn app:app --reload --port 8000

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

from fastapi.middleware.cors import CORSMiddleware
from fastapi import Response
from pydantic import BaseModel, Field

# -----------------------------
# Config
# -----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")  # REQUIRED on Render

# CORS: allow your frontend(s). For quick deployment, allow all origins.
# If you want stricter: set FRONTEND_ORIGINS="https://yoursite.com,https://www.yoursite.com"
FRONTEND_ORIGINS = os.environ.get("FRONTEND_ORIGINS", "*")
ALLOWED_ORIGINS = (
    ["*"]
    if FRONTEND_ORIGINS.strip() == "*"
    else [o.strip() for o in FRONTEND_ORIGINS.split(",") if o.strip()]
)

app = FastAPI(title="CCA Matcher Backend (Postgres)", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Helpers
# -----------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_conn():
    """
    Open a new Postgres connection (safe for concurrency).
    Render provides DATABASE_URL. If it's missing, the app can't run globally.
    """
    if not DATABASE_URL:
        # This makes the error obvious in logs if you forgot to set env var.
        raise RuntimeError("DATABASE_URL is not set. Add it in your host environment (Render).")
    # dict_row makes SELECT results come back like dicts
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db() -> None:
    """Create tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS generate_events (
                    id BIGSERIAL PRIMARY KEY,
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
                    id BIGSERIAL PRIMARY KEY,
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
# Models
# -----------------------------
class ClickEvent(BaseModel):
    student_id: str = Field(..., min_length=1, max_length=200)
    cca_name: str = Field(..., min_length=1, max_length=300)


class GenerateResult(BaseModel):
    name: str
    desc: Optional[str] = None
    cat: Optional[str] = None
    venue: Optional[str] = None
    time: Optional[str] = None
    type: Optional[str] = None


class GenerateEvent(BaseModel):
    student_id: str = Field(..., min_length=1, max_length=200)
    category: Optional[str] = None
    time: Optional[str] = None
    venue: Optional[str] = None
    typepref: Optional[str] = None
    interests: Optional[str] = None
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
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO click_events (ts, student_id, cca_name)
                VALUES (%s, %s, %s)
                """
                ,
                (now_iso(), payload.student_id, payload.cca_name),
            )
        conn.commit()
    return {"ok": True}


@app.post("/track/generate")
def track_generate(payload: GenerateEvent):
    init_db()
    results_json = json.dumps([r.model_dump() for r in payload.results], ensure_ascii=False)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO generate_events (ts, student_id, category, time, venue, typepref, interests, results_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
    - totals: generates + clicks
    - top_clicked_ccas: top 10 clicked
    - distributions: category/time/venue/typepref based on generate_events
    """
    init_db()

    def top_dist(col: str) -> List[Dict[str, Any]]:
        # Quote column safely (Postgres uses double-quotes for identifiers)
        qcol = f'"{col}"'
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {qcol} AS value, COUNT(*) AS c
                    FROM generate_events
                    GROUP BY {qcol}
                    ORDER BY c DESC
                    LIMIT 10
                    """
                )
                rows = cur.fetchall()
        return [{"value": r["value"], "count": r["c"]} for r in rows]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM generate_events")
            total_generates = cur.fetchone()["n"]

            cur.execute("SELECT COUNT(*) AS n FROM click_events")
            total_clicks = cur.fetchone()["n"]

            cur.execute(
                """
                SELECT cca_name, COUNT(*) AS c
                FROM click_events
                GROUP BY cca_name
                ORDER BY c DESC
                LIMIT 10
                """
            )
            top_ccas = cur.fetchall()

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
    return Response(status_code=204)
