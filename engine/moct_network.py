# -*- coding: utf-8 -*-
"""
국가 교통망 노드·링크(MOCT) SQLite 캐시 조회.
- scripts/import_moct_nodelink.py 로 data/moct_network.sqlite 생성 후 사용.
- BLUEDOT_MOCT_DB 환경변수로 경로 지정 가능(미설정 시 프로젝트 data/moct_network.sqlite).
"""
from __future__ import annotations

import math
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CELL_DEG = 0.025  # ~2.7km — 그리드 셀당 후보 수 제한용
_LAT0 = 33.0
_LNG0 = 124.5


def moct_sqlite_path() -> str:
    p = (os.environ.get("BLUEDOT_MOCT_DB") or "").strip()
    if p:
        return p
    return os.path.join(_BASE, "data", "moct_network.sqlite")


def moct_data_available() -> bool:
    path = moct_sqlite_path()
    return os.path.isfile(path) and os.path.getsize(path) > 10_000


_conn_lock = threading.Lock()
_row_count: Optional[int] = None


def moct_node_row_count() -> int:
    global _row_count
    if _row_count is not None:
        return _row_count
    if not moct_data_available():
        _row_count = 0
        return 0
    try:
        conn = sqlite3.connect(moct_sqlite_path())
        try:
            r = conn.execute("SELECT COUNT(*) FROM moct_nodes").fetchone()
            _row_count = int(r[0]) if r else 0
        finally:
            conn.close()
    except Exception:
        _row_count = 0
    return _row_count


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _grid_ij(lat: float, lng: float) -> Tuple[int, int]:
    gi = int((float(lat) - _LAT0) / _CELL_DEG)
    gj = int((float(lng) - _LNG0) / _CELL_DEG)
    return gi, gj


def lookup_moct_nearest(
    lat: float,
    lng: float,
    *,
    span: int = 4,
    max_scan_m: float = 800.0,
) -> Optional[Dict[str, Any]]:
    """
    최근접 MOCT 노드 1건 + 거리(m).
    차량 도로망 기준이며 보행 전용 미포함 가능.
    """
    if not moct_data_available():
        return None
    gi0, gj0 = _grid_ij(lat, lng)
    best: Optional[Tuple[float, Tuple]] = None
    try:
        conn = sqlite3.connect(moct_sqlite_path())
        try:
            for di in range(-span, span + 1):
                for dj in range(-span, span + 1):
                    rows = conn.execute(
                        """
                        SELECT node_id, lat, lng, link_degree, best_road_rank, node_type
                        FROM moct_nodes
                        WHERE grid_i = ? AND grid_j = ?
                        """,
                        (gi0 + di, gj0 + dj),
                    ).fetchall()
                    for row in rows:
                        nid, nlat, nlng, deg, br, nty = row
                        d = _haversine_m(lat, lng, float(nlat), float(nlng))
                        if d > max_scan_m:
                            continue
                        if best is None or d < best[0]:
                            best = (d, (nid, nlat, nlng, deg, br, nty))
        finally:
            conn.close()
    except Exception:
        return None
    if not best:
        return None
    d, (nid, nlat, nlng, deg, br, nty) = best
    br_i = int(br) if br is not None else 999
    deg_i = int(deg) if deg is not None else 0
    return {
        "node_id": int(nid),
        "nearest_dist_m": round(float(d), 1),
        "link_degree": deg_i,
        "best_road_rank": br_i,
        "node_type": int(nty) if nty is not None else None,
        "node_lat": float(nlat),
        "node_lng": float(nlng),
    }


def moct_macro_score_adjustment(hit: Dict[str, Any]) -> float:
    """1단계 종합점수(0~10) 소폭 보정. 음수·양수 모두 작게."""
    d = float(hit.get("nearest_dist_m") or 9999)
    deg = int(hit.get("link_degree") or 0)
    br = int(hit.get("best_road_rank") or 999)
    adj = 0.0
    if d <= 150 and br <= 103 and deg >= 3:
        adj += 0.14
    elif d <= 220 and br <= 104 and deg >= 3:
        adj += 0.10
    elif d <= 120 and deg >= 4:
        adj += 0.08
    elif d <= 180 and br <= 105 and deg >= 2:
        adj += 0.05
    if d > 450 and br >= 107 and deg <= 2:
        adj -= 0.06
    elif d > 600 and deg <= 1:
        adj -= 0.05
    return max(-0.12, min(0.18, adj))


def moct_narrative_ko(hit: Optional[Dict[str, Any]]) -> str:
    if not hit:
        return (
            "국가 노드·링크(MOCT) 캐시가 없습니다. `python scripts/import_moct_nodelink.py` 로 "
            "`data/moct_network.sqlite` 를 생성하면 교차로·도로등급 근거가 리포트에 반영됩니다."
        )
    br = int(hit.get("best_road_rank") or 0)
    deg = int(hit.get("link_degree") or 0)
    d = float(hit.get("nearest_dist_m") or 0)
    road_hint = (
        "고속·국도권 인접 가능성 큼"
        if br <= 102
        else "일반국·지방도권 인접"
        if br <= 104
        else "시군도·이면 도로 비중"
        if br <= 106
        else "세부도로 노드"
    )
    return (
        f"국가 교통망 노드 기준 최근접 약 {d:.0f}m, 연결 링크 수 {deg}, "
        f"인접 최소 도로등급코드 {br}({road_hint}). 차량 도로망 기준이며 보행 전용은 별도입니다."
    )


def moct_stage2_visibility_bonus(hit: Optional[Dict[str, Any]]) -> float:
    """score_proptech 가시성·접근 축에 더할 0~6점."""
    if not hit:
        return 0.0
    d = float(hit.get("nearest_dist_m") or 9999)
    deg = int(hit.get("link_degree") or 0)
    br = int(hit.get("best_road_rank") or 999)
    bonus = 0.0
    if d <= 100 and br <= 103 and deg >= 3:
        bonus += 4.0
    elif d <= 160 and br <= 104 and deg >= 3:
        bonus += 3.0
    elif d <= 220 and br <= 105:
        bonus += 2.0
    elif d <= 280 and deg >= 4:
        bonus += 1.5
    if deg >= 5 and d <= 200:
        bonus += 1.0
    return min(6.0, bonus)


def moct_competitor_walk_proxy(hit: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    PostGIS 대신·병행용 교차로 프록시(차량 망).
    """
    out: Dict[str, Any] = {
        "moct_used": bool(hit),
        "postgis_skipped": True,
        "skip_reason": "moct_vehicle_network",
        "near_intersection_50m": None,
        "strong_junction_35m": None,
        "max_vertex_degree": None,
        "min_vertex_dist_m": None,
    }
    if not hit:
        out["skip_reason"] = "moct_cache_missing"
        return out
    d = float(hit["nearest_dist_m"])
    deg = int(hit["link_degree"])
    out["postgis_skipped"] = False
    out["skip_reason"] = None
    out["min_vertex_dist_m"] = d
    out["max_vertex_degree"] = deg
    out["near_intersection_50m"] = bool(d <= 50 and deg >= 3)
    out["strong_junction_35m"] = bool(d <= 35 and deg >= 4)
    out["moct_best_road_rank"] = int(hit.get("best_road_rank") or 999)
    out["moct_node_id"] = int(hit.get("node_id") or 0)
    return out


def merge_walk_network_postgis_moct(
    postgis: Dict[str, Any],
    moct_hit: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    PostGIS 우선. PostGIS가 꺼져 있으면 MOCT 차량 노드망으로 교차·밀착 프록시를 채움.
    둘 다 있으면 PostGIS 지표 유지 + moct_* 보조 필드만 추가.
    """
    out = dict(postgis or {})
    out["moct_nearest"] = moct_hit
    if moct_hit:
        out["moct_nearest_dist_m"] = moct_hit.get("nearest_dist_m")
        out["moct_link_degree"] = moct_hit.get("link_degree")
        out["moct_best_road_rank"] = moct_hit.get("best_road_rank")
    pg_skip = bool(out.get("postgis_skipped"))
    if pg_skip and moct_hit:
        out.update(moct_competitor_walk_proxy(moct_hit))
        out["road_network_source"] = "moct_vehicle"
    elif not pg_skip:
        out["road_network_source"] = "postgis_pedestrian"
    else:
        out["road_network_source"] = "none"
    return out
