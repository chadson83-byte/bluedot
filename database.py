# -*- coding: utf-8 -*-
"""BLUEDOT - SQLite DB (사용자, 크레딧, 분석리포트, 결제이력)"""
import sqlite3
import os
import json
from datetime import datetime
from contextlib import contextmanager

_BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_BASE, "bluedot.db")


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                email TEXT,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider, provider_id)
            );
            CREATE TABLE IF NOT EXISTS user_credits (
                user_id INTEGER NOT NULL,
                credits INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS analysis_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                report_data TEXT NOT NULL,
                region_name TEXT,
                dept_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                plan_type TEXT,
                credits_added INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE INDEX IF NOT EXISTS idx_reports_user ON analysis_reports(user_id);
            CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);
        """)


def get_or_create_user(provider: str, provider_id: str, email: str = None, name: str = None) -> int:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT id FROM users WHERE provider=? AND provider_id=?",
            (provider, str(provider_id))
        )
        row = cur.fetchone()
        if row:
            uid = row["id"]
            conn.execute(
                "UPDATE users SET email=?, name=?, created_at=CURRENT_TIMESTAMP WHERE id=?",
                (email or "", name or "", uid)
            )
            return uid
        conn.execute(
            "INSERT INTO users (provider, provider_id, email, name) VALUES (?,?,?,?)",
            (provider, str(provider_id), email or "", name or "")
        )
        uid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT INTO user_credits (user_id, credits) VALUES (?,0)", (uid,))
        return uid


def get_user_by_id(user_id: int) -> dict:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM users WHERE id=?", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_user_credits(user_id: int) -> int:
    with get_db() as conn:
        cur = conn.execute("SELECT credits FROM user_credits WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return int(row["credits"]) if row else 0


def use_credit(user_id: int) -> bool:
    with get_db() as conn:
        cur = conn.execute("SELECT credits FROM user_credits WHERE user_id=? AND credits>0", (user_id,))
        row = cur.fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE user_credits SET credits = credits - 1, updated_at = CURRENT_TIMESTAMP WHERE user_id=?",
            (user_id,)
        )
        return True


def save_report(user_id: int, report_data: dict, region_name: str = "", dept_name: str = "") -> int:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO analysis_reports (user_id, report_data, region_name, dept_name) VALUES (?,?,?,?)",
            (user_id, json.dumps(report_data, ensure_ascii=False), region_name or "", dept_name or "")
        )
        return cur.lastrowid


def get_reports(user_id: int, limit: int = 50) -> list:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT id, region_name, dept_name, created_at FROM analysis_reports WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        )
        return [dict(row) for row in cur.fetchall()]


def get_report(user_id: int, report_id: int) -> dict:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM analysis_reports WHERE id=? AND user_id=?",
            (report_id, user_id)
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        d["report_data"] = json.loads(d["report_data"])
        return d


def add_payment(user_id: int, amount: int, plan_type: str, credits_added: int):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO payments (user_id, amount, plan_type, credits_added) VALUES (?,?,?,?)",
            (user_id, amount, plan_type, credits_added)
        )
        add_credits_raw(conn, user_id, credits_added)


def add_credits_raw(conn, user_id: int, amount: int):
    conn.execute(
        "INSERT OR IGNORE INTO user_credits (user_id, credits) VALUES (?,0)",
        (user_id,)
    )
    conn.execute(
        "UPDATE user_credits SET credits = credits + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id=?",
        (amount, user_id)
    )


def get_payments(user_id: int, limit: int = 30) -> list:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM payments WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        )
        return [dict(row) for row in cur.fetchall()]


def add_user_credits(user_id: int, amount: int):
    """결제 외 크레딧 추가 (테스트 등)"""
    with get_db() as conn:
        add_credits_raw(conn, user_id, amount)
