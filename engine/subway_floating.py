# -*- coding: utf-8 -*-
"""
지하철 역사(시나리오) 반경 유동(ES1007BD) — 최근접 허브 좌표·TOTL_FPOP 기반 0~100 프록시.
SQLite 테이블 subway_station_footfall (database.init_db).
"""
from __future__ import annotations

import bisect
import logging
import math
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from engine.sns_floating import blend_scores_0_10

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _db_path() -> str:
    p = (os.environ.get("BLUEDOT_DB_PATH") or "").strip()
    if p:
        return p
    return os.path.join(_BASE, "bluedot.db")


_R_LOCK = threading.Lock()
_LOADED = False
_LATEST_YM: str = ""
_ROWS: List[Dict[str, Any]] = []
_SORTED_LOG_TOTL: List[float] = []


def invalidate_subway_cache() -> None:
    global _LOADED, _LATEST_YM, _ROWS, _SORTED_LOG_TOTL
    with _R_LOCK:
        _LOADED = False
        _LATEST_YM = ""
        _ROWS = []
        _SORTED_LOG_TOTL = []


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _ensure_loaded() -> None:
    global _LOADED, _LATEST_YM, _ROWS, _SORTED_LOG_TOTL
    with _R_LOCK:
        if _LOADED:
            return
        _ROWS = []
        _SORTED_LOG_TOTL = []
        path = _db_path()
        if not os.path.isfile(path):
            _LOADED = True
            return
        try:
            conn = sqlite3.connect(path)
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='subway_station_footfall'"
            )
            if not cur.fetchone():
                conn.close()
                _LOADED = True
                return
            ym_row = conn.execute("SELECT MAX(data_strd_ym) FROM subway_station_footfall").fetchone()
            if not ym_row or not ym_row[0]:
                conn.close()
                _LOADED = True
                return
            _LATEST_YM = str(ym_row[0]).strip()
            q = """
                SELECT subway_scn_innb, subway_scn_nm, subway_route_nm,
                       center_lat, center_lng, totl_fpop, male_fpop, female_fpop
                FROM subway_station_footfall
                WHERE data_strd_ym = ?
            """
            logs: List[float] = []
            for row in conn.execute(q, (_LATEST_YM,)):
                innb, snm, rnm, la, lo, tt, m, f = row
                tt = float(tt or 0)
                if tt <= 0:
                    continue
                rec = {
                    "innb": str(innb),
                    "nm": str(snm),
                    "route": str(rnm or ""),
                    "lat": float(la),
                    "lng": float(lo),
                    "totl": tt,
                    "male": float(m or 0),
                    "female": float(f or 0),
                }
                _ROWS.append(rec)
                logs.append(math.log1p(tt))
            conn.close()
            _SORTED_LOG_TOTL = sorted(logs)
        except Exception as e:
            logging.warning("subway_floating: load failed: %s", e)
            _ROWS = []
            _SORTED_LOG_TOTL = []
        _LOADED = True


def subway_blend_weight_for_dept(dept: str) -> float:
    """역세권 원시 유동 볼륨 — SNS보다 낮은 2차 블렌딩 비중."""
    d = (dept or "").strip()
    if d in ("피부과", "성형외과", "치과"):
        return 0.08
    if d in ("내과", "이비인후과"):
        return 0.05
    if d == "소아과":
        return 0.06
    if d == "한의원":
        return 0.06
    if d in ("정신건강의학과", "산부인과", "안과", "정형외과"):
        return 0.07
    return 0.06


def _percentile_band_from_log(log_totl: float, sorted_logs: List[float]) -> str:
    if not sorted_logs:
        return "—"
    i = bisect.bisect_right(sorted_logs, log_totl)
    pct = 100.0 * i / len(sorted_logs)
    if pct >= 95:
        return "상위 5%"
    if pct >= 90:
        return "상위 10%"
    if pct >= 75:
        return "상위 25%"
    if pct >= 50:
        return "상위 50%"
    return "중하위권"


def _nearest_subway_meta_and_proxy(lat: float, lng: float) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    DB에 역이 있으면 항상 최근접 역 메타를 채움.
    직선 거리 3.5km 초과 시 프록시만 None(종합 블렌딩 제외) — 카드에는 역명·거리·유동 구간은 표시.
    """
    _ensure_loaded()
    meta: Dict[str, Any] = {}
    if not _ROWS or not _SORTED_LOG_TOTL:
        return None, meta
    best_d: Optional[float] = None
    best_r: Optional[Dict[str, Any]] = None
    best_totl = 0.0
    for r in _ROWS:
        d_m = _haversine_m(lat, lng, r["lat"], r["lng"])
        if best_d is None or d_m < best_d:
            best_d = d_m
            best_r = r
            best_totl = float(r["totl"])
    if best_d is None or best_r is None:
        return None, meta
    d_m = float(best_d)
    r = best_r
    totl = best_totl
    lt = math.log1p(max(0.0, totl))
    fem = float(r.get("female") or 0)
    tot = float(r.get("totl") or 1)
    fem_r = fem / tot if tot > 0 else 0.5
    meta = {
        "nearest_dist_m": round(d_m, 1),
        "nearest_nm": r["nm"],
        "nearest_lat": float(r["lat"]),
        "nearest_lng": float(r["lng"]),
        "nearest_route": r.get("route") or "",
        "nearest_totl_raw": totl,
        "female_share": round(fem_r, 3),
        "data_strd_ym": _LATEST_YM,
        "percentile_band_ko": _percentile_band_from_log(lt, _SORTED_LOG_TOTL),
        "subway_far_beyond_m": 3500.0,
    }
    if d_m > 3500:
        return None, meta
    rank_pct = 100.0 * bisect.bisect_right(_SORTED_LOG_TOTL, lt) / max(1, len(_SORTED_LOG_TOTL))
    decay = max(0.38, 1.0 - max(0.0, d_m - 350.0) / 2200.0)
    proxy = max(0.0, min(100.0, rank_pct * decay))
    return proxy, meta


def _proxy_0_100_for_nearest(lat: float, lng: float) -> Optional[Tuple[float, Dict[str, Any]]]:
    """역세권 프록시가 정의되는 경우만(≈3.5km 이내). 경쟁·분석 모듈용."""
    proxy, meta = _nearest_subway_meta_and_proxy(lat, lng)
    if proxy is None:
        return None
    return proxy, meta


def narrative_subway_ko(*, dept: str, meta: Dict[str, Any], proxy: Optional[float], weight: float) -> str:
    if proxy is None or not meta:
        return ""
    nm = meta.get("nearest_nm") or "인근 역"
    dm = meta.get("nearest_dist_m")
    band = meta.get("percentile_band_ko") or "—"
    w_pct = int(round(weight * 100))
    fr = meta.get("female_share")
    tail = ""
    if fr is not None and fr >= 0.55 and dept in ("피부과", "산부인과", "성형외과"):
        tail = " 여성 유동 비중이 높아 해당 과목 타깃 동선과 궁합이 좋습니다."
    elif fr is not None and fr <= 0.45 and dept in ("정형외과", "내과"):
        tail = " 남성 유동 비중이 상대적으로 높은 허브로 해석됩니다."
    return (
        f"최근접 지하철 허브는 「{nm}」(약 {dm:.0f}m)이며, "
        f"월간 유동 규모는 전국 역 단위 기준 {band}에 해당합니다. "
        f"종합 점수에 약 {w_pct}% 비중으로 역세권 볼륨을 추가 반영했습니다.{tail}"
    )


def apply_subway_blend_after_sns(
    sns_blended_0_10: float,
    lat: float,
    lng: float,
    dept: str,
) -> Tuple[float, Dict[str, Any], Dict[str, Any], str]:
    w = subway_blend_weight_for_dept(dept)
    _ensure_loaded()
    has_stations = bool(_ROWS)
    proxy, fields = _nearest_subway_meta_and_proxy(lat, lng)
    if not has_stations or not fields.get("nearest_nm"):
        empty_fields: Dict[str, Any] = {}
        reason = "subway_sqlite_empty"
        narr = narrative_subway_ko(dept=dept, meta=empty_fields, proxy=None, weight=w)
        meta = {"applied": False, "weight": w, "reason": reason}
        return sns_blended_0_10, meta, empty_fields, narr
    if proxy is None:
        dm = float(fields.get("nearest_dist_m") or 0)
        band = fields.get("percentile_band_ko") or "—"
        nm = fields.get("nearest_nm") or "인근 역"
        narr = (
            f"가장 가까운 지하철 허브는 「{nm}」(약 {dm:.0f}m)입니다. "
            f"역사 반경 약 3.5km 밖이라 역세권 프록시는 종합 점수에 넣지 않았고, "
            f"해당 역 월간 유동은 전국 역 단위 기준 {band} 수준으로 참고할 수 있습니다."
        )
        meta = {"applied": False, "weight": w, "reason": "subway_far_hub"}
        wb = (
            f"역세권 {nm} ({dm:.0f}m, 반경 밖) · 유동규모 {band}"
        )
        fields = dict(fields)
        fields["subway_whitebox_ko"] = wb
        return sns_blended_0_10, meta, fields, narr
    out, bmeta = blend_scores_0_10(sns_blended_0_10, proxy, w)
    bmeta = dict(bmeta)
    bmeta["weight"] = w
    narr = narrative_subway_ko(dept=dept, meta=fields, proxy=proxy, weight=w)
    wb = (
        f"역세권 {fields.get('nearest_nm', '—')} ({fields.get('nearest_dist_m', 0):.0f}m) "
        f"· 유동규모 {fields.get('percentile_band_ko', '—')} · 종합 {int(round(w * 100))}% 블렌딩"
    )
    fields["subway_whitebox_ko"] = wb
    return out, bmeta, fields, narr


def enrich_stage2_candidate_subway(c: Dict[str, Any], *, dept: str) -> None:
    la = float(c.get("lat") or 0)
    ln = float(c.get("lng") or 0)
    proxy, meta = _nearest_subway_meta_and_proxy(la, ln)
    if not meta.get("nearest_nm"):
        c["subway_fpop_proxy_0_100"] = None
        c["subway_fpop_norm"] = None
        c["subway_nearest_nm"] = None
        c["subway_nearest_dist_m"] = None
        return
    c["subway_fpop_proxy_0_100"] = round(proxy, 2) if proxy is not None else None
    c["subway_fpop_norm"] = (
        max(0.0, min(1.0, float(proxy) / 100.0)) if proxy is not None else None
    )
    c["subway_nearest_nm"] = meta.get("nearest_nm")
    c["subway_nearest_dist_m"] = meta.get("nearest_dist_m")
