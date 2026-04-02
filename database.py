# -*- coding: utf-8 -*-
"""BLUEDOT - SQLite DB (사용자, 크레딧, 분석리포트, 결제이력)"""
import sqlite3
import os
import json
import math
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

_BASE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB_FILE = os.path.join(_BASE, "bluedot.db")
# Fly Volume 등: BLUEDOT_DB_PATH=/data/bluedot.db (fly.toml [env] + [[mounts]])
DB_PATH = (os.environ.get("BLUEDOT_DB_PATH") or "").strip() or _DEFAULT_DB_FILE


def _ensure_db_parent_dir() -> None:
    parent = os.path.dirname(os.path.abspath(DB_PATH))
    if parent:
        try:
            os.makedirs(parent, exist_ok=True)
        except OSError:
            pass


@contextmanager
def get_db():
    _ensure_db_parent_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    _ensure_db_parent_dir()
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

            CREATE TABLE IF NOT EXISTS retail_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_ref TEXT,
                title TEXT NOT NULL,
                address TEXT,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                floor INTEGER,
                floors_total INTEGER,
                footprint_geojson TEXT,
                building_height_m REAL,
                competing_pois_json TEXT,
                notes TEXT,
                meta_json TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_retail_listings_external_ref
                ON retail_listings(external_ref)
                WHERE external_ref IS NOT NULL AND length(trim(external_ref)) > 0;
            CREATE INDEX IF NOT EXISTS idx_retail_listings_lat_lng ON retail_listings(lat, lng);
            CREATE INDEX IF NOT EXISTS idx_retail_listings_active ON retail_listings(is_active);
        """)
        _migrate_retail_listings_columns(conn)


def _migrate_retail_listings_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='retail_listings'")
    if not cur.fetchone():
        return
    cur = conn.execute("PRAGMA table_info(retail_listings)")
    cols = {row[1] for row in cur.fetchall()}
    if "kind_code" not in cols:
        conn.execute("ALTER TABLE retail_listings ADD COLUMN kind_code TEXT DEFAULT 'SG'")
    if "deal_code" not in cols:
        conn.execute("ALTER TABLE retail_listings ADD COLUMN deal_code TEXT DEFAULT 'B2'")


def _row_matches_listing_filter(row: Dict[str, Any], kind: str = "SG", deal: str = "B2") -> bool:
    """네이버 a=상가(SG), b=월세(B2) 에 맞는 행만."""
    k = (row.get("kind_code") or "SG").strip().upper()
    d = (row.get("deal_code") or "B2").strip().upper()
    if deal and deal.upper() == "B2":
        if d and d != "B2":
            return False
    if kind and kind.upper() == "SG":
        if not k or k == "SG":
            return True
        parts = [p.strip() for p in k.split(":") if p.strip()]
        return "SG" in parts
    return True


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


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p = math.pi / 180.0
    a = 0.5 - math.cos((lat2 - lat1) * p) / 2.0
    a += math.cos(lat1 * p) * math.cos(lat2 * p) * (1.0 - math.cos((lng2 - lng1) * p)) / 2.0
    return r * 2.0 * math.asin(math.sqrt(min(1.0, max(0.0, a))))


def list_retail_listings_nearby(lat: float, lng: float, radius_m: float, limit: int = 200) -> List[Dict[str, Any]]:
    """활성 매물만 반경 필터 (대략 bbox 후 정밀 거리)."""
    radius_m = float(max(10.0, min(radius_m, 12000.0)))
    limit = int(max(1, min(limit, 500)))
    dlat = radius_m / 110574.0
    dlng = radius_m / max(1e-6, 111320.0 * math.cos(math.radians(lat)))
    lat0, lat1 = lat - dlat, lat + dlat
    lng0, lng1 = lng - dlng, lng + dlng
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT * FROM retail_listings
            WHERE is_active = 1 AND lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?
            LIMIT ?
            """,
            (lat0, lat1, lng0, lng1, limit * 3),
        )
        rows = [dict(r) for r in cur.fetchall()]
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = _haversine_m(lat, lng, float(r["lat"]), float(r["lng"]))
        if d <= radius_m:
            rr = dict(r)
            rr["distance_from_query_m"] = round(d, 1)
            out.append(rr)
    out.sort(key=lambda x: x["distance_from_query_m"])
    return out[:limit]


def _serialize_listing_row(r: Dict[str, Any]) -> Dict[str, Any]:
    o: Dict[str, Any] = {
        "id": r["id"],
        "external_ref": r.get("external_ref"),
        "title": r["title"],
        "address": r.get("address"),
        "lat": float(r["lat"]),
        "lng": float(r["lng"]),
        "floor": r.get("floor"),
        "floors_total": r.get("floors_total"),
        "building_height_m": r.get("building_height_m"),
        "notes": r.get("notes"),
        "is_active": bool(r.get("is_active", 1)),
        "distance_from_query_m": r.get("distance_from_query_m"),
        "kind_code": r.get("kind_code") or "SG",
        "deal_code": r.get("deal_code") or "B2",
    }
    fp = r.get("footprint_geojson")
    if fp:
        try:
            o["footprint"] = json.loads(fp)
        except (TypeError, ValueError):
            o["footprint"] = None
    else:
        o["footprint"] = None
    cj = r.get("competing_pois_json")
    if cj:
        try:
            o["competing_pois"] = json.loads(cj)
        except (TypeError, ValueError):
            o["competing_pois"] = []
    else:
        o["competing_pois"] = []
    mj = r.get("meta_json")
    if mj:
        try:
            o["meta"] = json.loads(mj)
        except (TypeError, ValueError):
            o["meta"] = {}
    else:
        o["meta"] = {}
    return o


def list_retail_listings_nearby_api(lat: float, lng: float, radius_m: float, limit: int = 200) -> List[Dict[str, Any]]:
    raw = list_retail_listings_nearby(lat, lng, radius_m, limit=limit)
    return [_serialize_listing_row(r) for r in raw]


def nearest_retail_listing_for_anchor(
    lat: float,
    lng: float,
    radius_m: float = 8000.0,
    kind_code: str = "SG",
    deal_code: str = "B2",
) -> Optional[Dict[str, Any]]:
    """탭한 좌표에서 가장 가까운 1건(상가·월세 필터). 거리순 첫 매칭."""
    raw = list_retail_listings_nearby(lat, lng, radius_m, limit=500)
    for r in raw:
        if _row_matches_listing_filter(r, kind=kind_code or "SG", deal=deal_code or "B2"):
            return _serialize_listing_row(r)
    return None


def upsert_retail_listing(data: Dict[str, Any]) -> int:
    """external_ref 가 있으면 동일 키 행을 갱신, 없으면 삽입."""
    title = (data.get("title") or "").strip()
    if not title:
        raise ValueError("title 필수")
    lat = float(data["lat"])
    lng = float(data["lng"])
    ext = (data.get("external_ref") or "").strip() or None
    address = (data.get("address") or "").strip() or None
    floor = data.get("floor")
    floors_total = data.get("floors_total")
    fp = data.get("footprint") or data.get("footprint_geojson")
    fp_s = json.dumps(fp, ensure_ascii=False) if fp else None
    bh = data.get("building_height_m")
    comp = data.get("competing_pois") or []
    comp_s = json.dumps(comp, ensure_ascii=False) if comp else None
    notes = (data.get("notes") or "").strip() or None
    meta = data.get("meta") or {}
    meta_s = json.dumps(meta, ensure_ascii=False) if meta else None
    is_act = 1 if data.get("is_active", True) else 0
    kind_code = (data.get("kind_code") or "SG").strip() or "SG"
    deal_code = (data.get("deal_code") or "B2").strip() or "B2"
    with get_db() as conn:
        if ext:
            row = conn.execute("SELECT id FROM retail_listings WHERE external_ref = ?", (ext,)).fetchone()
            if row:
                rid = int(row["id"])
                conn.execute(
                    """
                    UPDATE retail_listings SET
                        title=?, address=?, lat=?, lng=?, floor=?, floors_total=?,
                        footprint_geojson=?, building_height_m=?, competing_pois_json=?,
                        notes=?, meta_json=?, is_active=?, kind_code=?, deal_code=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (
                        title,
                        address,
                        lat,
                        lng,
                        floor,
                        floors_total,
                        fp_s,
                        bh,
                        comp_s,
                        notes,
                        meta_s,
                        is_act,
                        kind_code,
                        deal_code,
                        rid,
                    ),
                )
                return rid
        cur = conn.execute(
            """
            INSERT INTO retail_listings (
                external_ref, title, address, lat, lng, floor, floors_total,
                footprint_geojson, building_height_m, competing_pois_json, notes, meta_json, is_active,
                kind_code, deal_code
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ext,
                title,
                address,
                lat,
                lng,
                floor,
                floors_total,
                fp_s,
                bh,
                comp_s,
                notes,
                meta_s,
                is_act,
                kind_code,
                deal_code,
            ),
        )
        return int(cur.lastrowid)


def delete_retail_listing(listing_id: int) -> bool:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM retail_listings WHERE id = ?", (int(listing_id),))
        return cur.rowcount > 0


def count_retail_listings_active() -> int:
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM retail_listings WHERE is_active = 1").fetchone()
        return int(row["c"]) if row else 0
