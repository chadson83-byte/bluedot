# -*- coding: utf-8 -*-
"""
건축물대장 조회 결과 Postgres 캐시 (gis_db)

키: (sigunguCd, bjdongCd, bun, ji)
값: useAprDay, age_years, elevator_total, parking_total, fetched_at

DB 장애 시에도 앱이 죽지 않도록: 실패하면 None 반환/무시.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None


def _conn_params():
    return dict(
        host=os.getenv("POSTGIS_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGIS_PORT", "5432")),
        dbname=os.getenv("POSTGIS_DB", "gis_db"),
        user=os.getenv("POSTGIS_USER", "postgres"),
        password=os.getenv("POSTGIS_PASSWORD", "postgres"),
        connect_timeout=3,
    )


def ensure_cache_table() -> None:
    if psycopg2 is None:
        return
    sql = """
    CREATE TABLE IF NOT EXISTS bluedot_building_cache (
      sigungu_cd VARCHAR(5) NOT NULL,
      bjdong_cd VARCHAR(5) NOT NULL,
      bun VARCHAR(4) NOT NULL,
      ji VARCHAR(4) NOT NULL,
      payload JSONB NOT NULL,
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY(sigungu_cd, bjdong_cd, bun, ji)
    );
    """
    try:
        conn = psycopg2.connect(**_conn_params())
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        finally:
            conn.close()
    except Exception:
        return


def get_cached(sigungu_cd: str, bjdong_cd: str, bun: str, ji: str) -> Optional[Dict[str, Any]]:
    if psycopg2 is None:
        return None
    try:
        conn = psycopg2.connect(**_conn_params())
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM bluedot_building_cache WHERE sigungu_cd=%s AND bjdong_cd=%s AND bun=%s AND ji=%s",
                    (sigungu_cd, bjdong_cd, bun, ji),
                )
                row = cur.fetchone()
            if not row:
                return None
            return row[0]
        finally:
            conn.close()
    except Exception:
        return None


def upsert_cached(sigungu_cd: str, bjdong_cd: str, bun: str, ji: str, payload: Dict[str, Any]) -> None:
    if psycopg2 is None:
        return
    try:
        conn = psycopg2.connect(**_conn_params())
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bluedot_building_cache(sigungu_cd,bjdong_cd,bun,ji,payload,fetched_at)
                    VALUES(%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT(sigungu_cd,bjdong_cd,bun,ji)
                    DO UPDATE SET payload=EXCLUDED.payload, fetched_at=EXCLUDED.fetched_at
                    """,
                    (sigungu_cd, bjdong_cd, bun, ji, json.dumps(payload, ensure_ascii=False)),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        return

