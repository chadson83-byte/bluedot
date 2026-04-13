# -*- coding: utf-8 -*-
"""
오아시스 ES1007AC — 법정동·필지 단위 인당 상가공급면적(SOPSRT_SPL_DIMS).
트렌드와 별도 축: 배후 대비 상가 면적(공급) 밀도 참고 지표.
"""
from __future__ import annotations

import bisect
import logging
import os
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_LOCK = threading.Lock()
_LOADED = False
_LATEST_YM: str = ""
# legaldong key (8/10 digit) -> row
_BY_LDONG: Dict[str, Dict[str, Any]] = {}
_SORTED_AVG_SPL: List[float] = []


def _db_path() -> str:
    p = (os.environ.get("BLUEDOT_DB_PATH") or "").strip()
    if p:
        return p
    return os.path.join(_BASE, "bluedot.db")


def invalidate_retail_supply_ac_cache() -> None:
    global _LOADED, _LATEST_YM, _BY_LDONG, _SORTED_AVG_SPL
    with _LOCK:
        _LOADED = False
        _LATEST_YM = ""
        _BY_LDONG = {}
        _SORTED_AVG_SPL = []


def _percentile_band(sorted_vals: List[float], v: float) -> str:
    if not sorted_vals or v is None:
        return "—"
    i = bisect.bisect_right(sorted_vals, float(v))
    pct = 100.0 * i / len(sorted_vals)
    if pct >= 90:
        return "상위 10%"
    if pct >= 75:
        return "상위 25%"
    if pct >= 50:
        return "상위 50%"
    return "중하위권"


def _ensure_loaded() -> None:
    global _LOADED, _LATEST_YM, _BY_LDONG, _SORTED_AVG_SPL
    with _LOCK:
        if _LOADED:
            return
        _BY_LDONG = {}
        _SORTED_AVG_SPL = []
        path = _db_path()
        if not os.path.isfile(path):
            _LOADED = True
            return
        try:
            conn = sqlite3.connect(path)
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='oasis_retail_supply_ac'"
            )
            if not cur.fetchone():
                conn.close()
                _LOADED = True
                return
            ym_row = conn.execute("SELECT MAX(data_strd_ym) FROM oasis_retail_supply_ac").fetchone()
            if not ym_row or not ym_row[0]:
                conn.close()
                _LOADED = True
                return
            ym = str(ym_row[0]).strip()
            _LATEST_YM = ym
            q = """
                SELECT legaldong_cd,
                       AVG(sopsrt_spl_dims) AS av,
                       COUNT(*) AS n
                FROM oasis_retail_supply_ac
                WHERE data_strd_ym = ?
                GROUP BY legaldong_cd
            """
            avs: List[float] = []
            for row in conn.execute(q, (ym,)):
                ld, av, n = row
                if not ld or av is None:
                    continue
                key = str(ld).strip()
                dnorm = re.sub(r"\D", "", key)
                if len(dnorm) < 8:
                    continue
                rec = {
                    "avg_spl": float(av),
                    "n_parcels": int(n or 0),
                    "data_strd_ym": ym,
                    "legaldong_cd": dnorm[:10] if len(dnorm) >= 10 else dnorm,
                }
                _BY_LDONG[dnorm] = rec
                if len(dnorm) >= 8:
                    _BY_LDONG[dnorm[:8]] = rec
                if len(dnorm) >= 10:
                    _BY_LDONG[dnorm[:10]] = rec
                avs.append(float(av))
            conn.close()
            _SORTED_AVG_SPL = sorted(avs)
        except Exception as e:
            logging.warning("retail_supply_ac: load failed: %s", e)
            _BY_LDONG = {}
            _SORTED_AVG_SPL = []
        _LOADED = True


def retail_supply_ac_dataset_loaded() -> bool:
    """SQLite에 최신 기준연월이 있으면 True(원본 테이블이 비어 있지 않음)."""
    _ensure_loaded()
    return bool(_LATEST_YM)


def lookup_retail_supply_for_legaldong(legaldong_cd10: Optional[str]) -> Optional[Dict[str, Any]]:
    """법정동코드(8~10자리) → 동 단위 평균 인당 상가공급면적 및 전국 동 분포 대비 구간."""
    if not legaldong_cd10:
        return None
    digits = re.sub(r"\D", "", str(legaldong_cd10))
    if len(digits) < 8:
        return None
    _ensure_loaded()
    if not _BY_LDONG:
        return None
    hit = None
    for n in (10, 9, 8):
        if len(digits) >= n:
            hit = _BY_LDONG.get(digits[:n])
            if hit:
                break
    if not hit:
        return None
    av = float(hit["avg_spl"])
    band = _percentile_band(_SORTED_AVG_SPL, av)
    return {
        "ok": True,
        "avg_spl_dims": round(av, 3),
        "n_parcels": int(hit["n_parcels"]),
        "data_strd_ym": hit["data_strd_ym"],
        "percentile_band_ko": band,
    }


def retail_supply_whitebox_ko(meta: Dict[str, Any]) -> str:
    return (
        f"ES1007AC 기준 {meta.get('data_strd_ym', '—')} · 법정동 평균 인당 상가공급면적 "
        f"{meta.get('avg_spl_dims')} ㎡/명(추정) · 필지 {meta.get('n_parcels')}건 집계 · 참고"
    )


def retail_supply_narrative_ko(meta: Dict[str, Any]) -> str:
    return (
        f"같은 법정동 내 필지·업종별 인당 상가공급면적 평균은 약 {meta.get('avg_spl_dims')} "
        f"(전국 법정동 단위 분포 대비 {meta.get('percentile_band_ko', '—')}). "
        f"오아시스 ES1007AC(분기) 참고 지표이며 임대료·매출과 직결되지는 않습니다."
    )
