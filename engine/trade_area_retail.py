# -*- coding: utf-8 -*-
"""
한국부동산원 ES1001AY — 주요 상권별 상가영업 현황(전국, 상권 폴리곤).
ES1013(도로단위)이 비어 있거나 시군구 매칭 실패 시 상권활성도 보조 프록시로 사용.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import Point
from shapely import wkt as shp_wkt
from shapely.geometry.base import BaseGeometry

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_LOCK = threading.Lock()
_LOADED = False
# (geometry, meta dict with bbox tuple for fast reject)
_ROWS: List[Tuple[BaseGeometry, Dict[str, Any]]] = []


def _db_path() -> str:
    p = (os.environ.get("BLUEDOT_DB_PATH") or "").strip()
    if p:
        return p
    return os.path.join(_BASE, "bluedot.db")


def invalidate_trade_area_retail_cache() -> None:
    global _LOADED, _ROWS
    with _LOCK:
        _LOADED = False
        _ROWS = []


def _geom_from_wkt(wkt_s: str) -> Optional[BaseGeometry]:
    s = (wkt_s or "").strip()
    if not s:
        return None
    try:
        g = shp_wkt.loads(s)
        if g is None or g.is_empty:
            return None
        if not g.is_valid:
            g = g.buffer(0)
        return g
    except Exception as e:
        logging.debug("trade_area_retail WKT parse: %s", e)
        return None


def _ensure_loaded() -> None:
    global _LOADED, _ROWS
    with _LOCK:
        if _LOADED:
            return
        _ROWS = []
        path = _db_path()
        if not os.path.isfile(path):
            _LOADED = True
            return
        try:
            conn = sqlite3.connect(path)
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_area_retail_kreb'"
            )
            if not cur.fetchone():
                conn.close()
                _LOADED = True
                return
            q = """
                SELECT trdar_no, trdar_nm, ctpr_nm, signgu_nm,
                       opbn_rate, bnse_rate, cus_rate, tcbiz_rate,
                       min_lng, min_lat, max_lng, max_lat, wkt
                FROM trade_area_retail_kreb
            """
            for row in conn.execute(q):
                trdar_no, trdar_nm, ctpr_nm, signgu_nm, opbn, bnse, cus, tcbiz, min_lng, min_lat, max_lng, max_lat, wkt = row
                geom = _geom_from_wkt(str(wkt or ""))
                if geom is None:
                    continue
                meta = {
                    "trdar_no": str(trdar_no or ""),
                    "trdar_nm": str(trdar_nm or ""),
                    "ctpr_nm": str(ctpr_nm or ""),
                    "signgu_nm": str(signgu_nm or ""),
                    "opbn_rate": float(opbn or 0),
                    "bnse_rate": float(bnse or 0),
                    "cus_rate": float(cus or 0),
                    "tcbiz_rate": float(tcbiz or 0),
                    "bbox": (float(min_lng), float(min_lat), float(max_lng), float(max_lat)),
                }
                _ROWS.append((geom, meta))
            conn.close()
        except Exception as e:
            logging.warning("trade_area_retail: load failed: %s", e)
            _ROWS = []
        _LOADED = True


def lookup_trade_area_vitality(lat: float, lng: float) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    좌표가 속한 주요 상권(폴리곤) 중 면적이 가장 작은 것을 선택(중첩 시 하위 상권 우선).
    프록시: 영업중 상가 건물 비율(0~100)을 그대로 0~100 점수로 사용.
    """
    _ensure_loaded()
    if not _ROWS:
        return None, {"matched": False, "block_reason": "trade_area_empty"}

    la, lo = float(lat), float(lng)
    pt = Point(lo, la)
    hits: List[Tuple[float, Dict[str, Any]]] = []
    for geom, rec in _ROWS:
        minx, miny, maxx, maxy = rec["bbox"]
        if not (minx <= lo <= maxx and miny <= la <= maxy):
            continue
        try:
            if geom.contains(pt):
                hits.append((geom.area, rec))
        except Exception:
            continue

    if not hits:
        return None, {"matched": False, "block_reason": "trade_area_miss"}

    hits.sort(key=lambda x: x[0])
    best = hits[0][1]
    op = max(0.0, min(100.0, float(best.get("opbn_rate") or 0)))
    meta: Dict[str, Any] = {
        "matched": True,
        "vitality_source": "kreb_es1001ay",
        "trdar_no": best.get("trdar_no"),
        "trdar_nm": best.get("trdar_nm"),
        "ctpr_nm": best.get("ctpr_nm"),
        "signgu_nm": best.get("signgu_nm"),
        "avg_vtlz_idex": round(op, 2),
        "percentile_band_ko": "주요상권 보조",
        "proxy_0_100": round(op, 2),
        "n_roads": 0,
        "strd_yr": "ES1001AY",
    }
    return op, meta
