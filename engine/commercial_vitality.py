# -*- coding: utf-8 -*-
"""
상권활성도지수(ES1013 계열): 도로명 단위 CSV → 시·군·구 집계 후 좌표 매칭.
- 좌표 → 카카오 coord2address 로 시도/시군구명 추출 → DB 집계 행과 정규화 매칭
- VTLZ_IDEX(활성화지수) 전국 시군구 평균 분포로 0~100 프록시 → SNS·역세권 이후 소폭 블렌딩
"""
from __future__ import annotations

import bisect
import logging
import os
import re
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_V_LOCK = threading.Lock()
_LOADED_YR: str = ""
_BY_SIGUNGU: Dict[Tuple[str, str], Dict[str, float]] = {}
_SORTED_AVG_VTLZ: List[float] = []
_ADDR_CACHE_LOCK = threading.Lock()
# (만료 epoch, Optional[(시도, 시군구)]). 실패(None)를 24시간 캐시하면 카카오 일시 오류 후 상권활성도가 하루 종일 꺼진 것처럼 보임.
_ADDR_CACHE: Dict[Tuple[int, int], Tuple[float, Optional[Tuple[str, str]]]] = {}
_ADDR_TTL_SUCCESS = float(os.environ.get("BLUEDOT_VITALITY_ADDR_CACHE_SEC", "86400"))
_ADDR_TTL_FAIL = float(os.environ.get("BLUEDOT_VITALITY_ADDR_FAIL_CACHE_SEC", "120"))


def _db_path() -> str:
    p = (os.environ.get("BLUEDOT_DB_PATH") or "").strip()
    if p:
        return p
    return os.path.join(_BASE, "bluedot.db")


def _norm_sigungu_key(s: str) -> str:
    s = re.sub(r"\s+", "", str(s or ""))
    s = s.replace("특별시", "").replace("광역시", "").replace("특별자치시", "")
    s = s.replace("특별자치도", "")
    return s


def _ensure_loaded() -> None:
    global _LOADED_YR, _BY_SIGUNGU, _SORTED_AVG_VTLZ
    with _V_LOCK:
        path = _db_path()
        if not os.path.isfile(path):
            _LOADED_YR = ""
            _BY_SIGUNGU = {}
            _SORTED_AVG_VTLZ = []
            return
        try:
            conn = sqlite3.connect(path)
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='commercial_vitality_road'"
            )
            if not cur.fetchone():
                conn.close()
                _LOADED_YR = ""
                _BY_SIGUNGU = {}
                _SORTED_AVG_VTLZ = []
                return
            yr_row = conn.execute("SELECT MAX(strd_yr) FROM commercial_vitality_road").fetchone()
            if not yr_row or not yr_row[0]:
                conn.close()
                _LOADED_YR = ""
                _BY_SIGUNGU = {}
                _SORTED_AVG_VTLZ = []
                return
            yr = str(yr_row[0]).strip()
            if yr == _LOADED_YR and _BY_SIGUNGU:
                conn.close()
                return
            q = """
                SELECT ctpr_nm, signgu_nm,
                       AVG(vtlz_idex) AS av,
                       MAX(vtlz_idex) AS mx,
                       AVG(bsnes_cnt) AS bc,
                       COUNT(*) AS nrd
                FROM commercial_vitality_road
                WHERE strd_yr = ?
                GROUP BY ctpr_nm, signgu_nm
            """
            m: Dict[Tuple[str, str], Dict[str, float]] = {}
            avs: List[float] = []
            for row in conn.execute(q, (yr,)):
                cp, sg, av, mx, bc, nrd = row
                cpk = _norm_sigungu_key(cp)
                sgk = _norm_sigungu_key(sg)
                key = (cpk, sgk)
                m[key] = {
                    "avg_vtlz": float(av or 0),
                    "max_vtlz": float(mx or 0),
                    "avg_bsnes": float(bc or 0),
                    "n_roads": float(nrd or 0),
                    "ctpr_nm": str(cp),
                    "signgu_nm": str(sg),
                }
                if av is not None and float(av) > 0:
                    avs.append(float(av))
            conn.close()
            _LOADED_YR = yr
            _BY_SIGUNGU = m
            _SORTED_AVG_VTLZ = sorted(avs)
        except Exception as e:
            logging.warning("commercial_vitality: load failed: %s", e)
            _LOADED_YR = ""
            _BY_SIGUNGU = {}
            _SORTED_AVG_VTLZ = []


def _sido_sigungu_from_coord2address_docs(docs: List[Any]) -> Optional[Tuple[str, str]]:
    sido, sigungu = "", ""
    for doc in docs or []:
        if not isinstance(doc, dict):
            continue
        ra = doc.get("road_address")
        if isinstance(ra, dict):
            sido = str(ra.get("region_1depth_name") or "").strip()
            sigungu = str(ra.get("region_2depth_name") or "").strip()
            if sido and sigungu:
                return (sido, sigungu)
        ja = doc.get("address")
        if isinstance(ja, dict):
            sido = sido or str(ja.get("region_1depth_name") or "").strip()
            sigungu = sigungu or str(ja.get("region_2depth_name") or "").strip()
            if sido and sigungu:
                return (sido, sigungu)
    return None


def _sido_sigungu_from_coord2region_json(data: Optional[dict]) -> Optional[Tuple[str, str]]:
    """coord2regioncode: B(법정) 우선, 없으면 H(행정)에서 시도·시군구명만 추출."""
    if not data or not isinstance(data, dict):
        return None
    docs = data.get("documents") or []
    if not isinstance(docs, list):
        return None
    scored: List[Tuple[int, str, str]] = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        rt = str(doc.get("region_type") or "").strip().upper()
        pri = 0 if rt == "B" else 1 if rt == "H" else 9
        d1 = str(doc.get("region_1depth_name") or "").strip()
        d2 = str(doc.get("region_2depth_name") or "").strip()
        if d1 and d2:
            scored.append((pri, d1, d2))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0])
    return (scored[0][1], scored[0][2])


def _coord2region_request(
    lat: float, lng: float, kakao_rest_key: str, *, allow_retry: bool = True
) -> Optional[Any]:
    url = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"KakaoAK {kakao_rest_key.strip()}"},
            params={"x": str(lng), "y": str(lat)},
            timeout=6.0,
        )
        if r.status_code in (429, 503) and allow_retry:
            time.sleep(0.4)
            return _coord2region_request(lat, lng, kakao_rest_key, allow_retry=False)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        logging.debug("coord2region (vitality): %s", e)
        return None


def _coord2address_request(
    lat: float, lng: float, kakao_rest_key: str, *, allow_retry: bool = True
) -> Optional[Any]:
    url = "https://dapi.kakao.com/v2/local/geo/coord2address.json"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"KakaoAK {kakao_rest_key.strip()}"},
            params={"x": str(lng), "y": str(lat), "input_coord": "WGS84"},
            timeout=6.0,
        )
        if r.status_code in (429, 503) and allow_retry:
            time.sleep(0.4)
            return _coord2address_request(lat, lng, kakao_rest_key, allow_retry=False)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        logging.debug("coord2address: %s", e)
        return None


def kakao_sido_sigungu_from_latlng(lat: float, lng: float, kakao_rest_key: str) -> Optional[Tuple[str, str]]:
    if not (kakao_rest_key or "").strip():
        return None
    rk = round(float(lat), 4)
    ck = round(float(lng), 4)
    now = time.time()
    with _ADDR_CACHE_LOCK:
        ent = _ADDR_CACHE.get((rk, ck))
        if ent:
            expires_at, cached_val = ent
            if now <= expires_at:
                return cached_val
            try:
                del _ADDR_CACHE[(rk, ck)]
            except KeyError:
                pass

    la0, lo0 = float(lat), float(lng)
    nudges = (
        (0.0, 0.0),
        (0.0018, 0.0),
        (-0.0018, 0.0),
        (0.0, 0.0018),
        (0.0, -0.0018),
        (0.0012, 0.0012),
        (-0.0012, 0.0012),
    )
    out: Optional[Tuple[str, str]] = None
    for du, dv in nudges:
        data = _coord2address_request(la0 + du, lo0 + dv, kakao_rest_key)
        if not data:
            continue
        docs = data.get("documents") or []
        out = _sido_sigungu_from_coord2address_docs(docs if isinstance(docs, list) else [])
        if out:
            break
    # 주소 API가 빈 문서를 줄 때(해안·신축 등) 행정구역 API로 시·군·구만 확보
    if not out:
        for du, dv in nudges:
            rj = _coord2region_request(la0 + du, lo0 + dv, kakao_rest_key)
            if not rj:
                continue
            out = _sido_sigungu_from_coord2region_json(rj)
            if out:
                break

    ttl = _ADDR_TTL_SUCCESS if out else _ADDR_TTL_FAIL
    with _ADDR_CACHE_LOCK:
        _ADDR_CACHE[(rk, ck)] = (now + ttl, out)
    return out


def _percentile_band_vtlz(avg_v: float, sorted_avgs: List[float]) -> str:
    if not sorted_avgs or avg_v is None:
        return "—"
    i = bisect.bisect_right(sorted_avgs, avg_v)
    pct = 100.0 * i / len(sorted_avgs)
    if pct >= 90:
        return "상위 10%"
    if pct >= 75:
        return "상위 25%"
    if pct >= 50:
        return "상위 50%"
    return "중하위권"


def _avg_to_score_0_100(avg_v: float, sorted_avgs: List[float]) -> float:
    if not sorted_avgs or avg_v is None:
        return 0.0
    i = bisect.bisect_right(sorted_avgs, float(avg_v))
    return max(0.0, min(100.0, 100.0 * i / max(1, len(sorted_avgs))))


def _sigungu_geo_from_subway_anchor_kakao(
    lat: float, lng: float, kakao_rest_key: str
) -> Tuple[Optional[Tuple[str, str]], str]:
    """핀 좌표에서 시군구가 비면 최근접 역 좌표로 coord2address/coord2region 재시도."""
    if not (kakao_rest_key or "").strip():
        return None, ""
    from engine.subway_floating import _nearest_subway_meta_and_proxy

    _, sm = _nearest_subway_meta_and_proxy(lat, lng)
    sla = sm.get("nearest_lat")
    slo = sm.get("nearest_lng")
    if sla is None or slo is None:
        return None, ""
    g = kakao_sido_sigungu_from_latlng(float(sla), float(slo), kakao_rest_key)
    if g:
        return g, "kakao_subway_anchor"
    return None, ""


def _unique_sigungu_tuple_from_partial_label(label: str) -> Optional[Tuple[str, str]]:
    """
    '해운대구 중동'처럼 시도 없이 시군구 토큰만 있을 때 ES1013 집계와 단일 매칭되면 시도·시군구 반환.
    '중구' 등 다의적 이름은 매칭이 2건 이상이면 포기.
    """
    parts = re.split(r"\s+", str(label or "").strip())
    sig_tok = None
    for p in reversed(parts):
        if len(p) >= 2 and (p.endswith("구") or p.endswith("군")):
            sig_tok = p
            break
    if not sig_tok:
        return None
    nk = _norm_sigungu_key(sig_tok)
    hits: List[Dict[str, Any]] = []
    for _k, rec in _BY_SIGUNGU.items():
        s2 = _norm_sigungu_key(str(rec.get("signgu_nm") or ""))
        if not s2:
            continue
        if nk == s2 or nk in s2 or s2 in nk:
            hits.append(rec)
    if len(hits) != 1:
        return None
    r0 = hits[0]
    ctp = str(r0.get("ctpr_nm") or "").strip()
    sgn = str(r0.get("signgu_nm") or "").strip()
    if not ctp or not sgn:
        return None
    return (ctp, sgn)


def vitality_blend_weight_for_dept(dept: str) -> float:
    d = (dept or "").strip()
    if d in ("피부과", "성형외과", "치과"):
        return 0.04
    if d in ("내과", "이비인후과", "소아과"):
        return 0.02
    if d == "한의원":
        return 0.03
    return 0.03


def lookup_vitality_for_coordinates(
    lat: float,
    lng: float,
    kakao_rest_key: str,
    master_admin_region_name: str = "",
) -> Tuple[Optional[float], Optional[Dict[str, Any]]]:
    """시군구 평균 활성화지수 → 0~100 프록시 및 메타."""
    from engine.geo_admin_fallback import sido_sigungu_tuple_from_admin_region_label

    _ensure_loaded()
    if not _BY_SIGUNGU or not _SORTED_AVG_VTLZ:
        return None, {"matched": False, "block_reason": "es1013_empty"}
    geo = kakao_sido_sigungu_from_latlng(lat, lng, kakao_rest_key)
    geo_src = "kakao"
    if not geo:
        label = (master_admin_region_name or "").strip()
        if label:
            geo = sido_sigungu_tuple_from_admin_region_label(label)
            if geo:
                geo_src = "master_admin_label"
    if not geo:
        geo2, src2 = _sigungu_geo_from_subway_anchor_kakao(lat, lng, kakao_rest_key)
        if geo2:
            geo, geo_src = geo2, src2
    if not geo:
        label = (master_admin_region_name or "").strip()
        if label:
            geo = _unique_sigungu_tuple_from_partial_label(label)
            if geo:
                geo_src = "master_admin_partial_sigungu"
    if not geo:
        return None, {"matched": False, "block_reason": "sigungu_unresolved"}
    sido, sigungu = geo
    cpk = _norm_sigungu_key(sido)
    sgk = _norm_sigungu_key(sigungu)
    hit = _BY_SIGUNGU.get((cpk, sgk))
    if not hit:
        for (a, b), rec in _BY_SIGUNGU.items():
            if sgk and (sgk in b or b in sgk):
                hit = rec
                break
    if not hit:
        return None, {
            "kakao_sido": sido,
            "kakao_sigungu": sigungu,
            "matched": False,
            "geo_source": geo_src,
        }
    av = hit["avg_vtlz"]
    sc = _avg_to_score_0_100(av, _SORTED_AVG_VTLZ)
    band = _percentile_band_vtlz(av, _SORTED_AVG_VTLZ)
    meta = {
        "matched": True,
        "vitality_source": "es1013_sigungu",
        "strd_yr": _LOADED_YR,
        "ctpr_nm": hit.get("ctpr_nm"),
        "signgu_nm": hit.get("signgu_nm"),
        "avg_vtlz_idex": round(av, 2),
        "max_vtlz_idex": round(hit.get("max_vtlz", 0), 2),
        "n_roads": int(hit.get("n_roads", 0)),
        "percentile_band_ko": band,
        "proxy_0_100": round(sc, 2),
        "kakao_sido": sido,
        "kakao_sigungu": sigungu,
        "geo_source": geo_src,
    }
    return sc, meta


def narrative_vitality_ko(meta: Optional[Dict[str, Any]], weight: float) -> str:
    if meta and meta.get("matched") and meta.get("vitality_source") == "kreb_es1001ay":
        w = int(round(weight * 100))
        nm = meta.get("trdar_nm") or "—"
        op = meta.get("avg_vtlz_idex")
        return (
            f"한국부동산원 주요 상권 「{nm}」 영업중 상가 건물 비율 약 {op}% (ES1001AY 보조). "
            f"종합 점수에 약 {w}% 반영했습니다."
        )
    if not meta or not meta.get("matched"):
        return ""
    w = int(round(weight * 100))
    sg = meta.get("signgu_nm") or ""
    band = meta.get("percentile_band_ko") or "—"
    av = meta.get("avg_vtlz_idex")
    return (
        f"「{sg}」은(는) 도로단위 상권활성도 지수(기준연도 {meta.get('strd_yr', '')}) "
        f"시군구 평균 약 {av}로, 전국 시군구 대비 {band}에 해당합니다. "
        f"종합 점수에 약 {w}% 비중으로 반영했습니다."
    )


def apply_vitality_blend_after_subway(
    score_0_10: float,
    lat: float,
    lng: float,
    dept: str,
    kakao_rest_key: str,
    master_admin_region_name: str = "",
) -> Tuple[float, Dict[str, Any], Dict[str, Any], str]:
    from engine.sns_floating import blend_scores_0_10

    w = vitality_blend_weight_for_dept(dept)
    proxy, meta = lookup_vitality_for_coordinates(
        lat, lng, kakao_rest_key, master_admin_region_name=master_admin_region_name
    )
    if proxy is None or meta is None or not meta.get("matched"):
        from engine.trade_area_retail import lookup_trade_area_vitality

        ta_proxy, ta_meta = lookup_trade_area_vitality(lat, lng)
        if ta_proxy is not None and ta_meta and ta_meta.get("matched"):
            proxy, meta = ta_proxy, ta_meta
        else:
            narr = narrative_vitality_ko(meta, w)
            return score_0_10, {"applied": False, "weight": w}, meta or {}, narr
    out, bmeta = blend_scores_0_10(score_0_10, proxy, w)
    bmeta = dict(bmeta)
    bmeta["weight"] = w
    meta = dict(meta)
    if meta.get("vitality_source") == "kreb_es1001ay":
        meta["vitality_whitebox_ko"] = (
            f"「{meta.get('trdar_nm', '—')}」({meta.get('ctpr_nm', '')} {meta.get('signgu_nm', '')}) "
            f"영업중 상가 비율 약 {meta.get('avg_vtlz_idex')}% · ES1001AY 보조 · {w*100:.0f}% 블렌딩"
        )
    else:
        gs = meta.get("geo_source") or "kakao"
        gs_note = " · 마스터 권역명 매칭" if gs == "master_admin_label" else ""
        meta["vitality_whitebox_ko"] = (
            f"{meta.get('signgu_nm', '—')} 시군구 평균 활성화지수 {meta.get('avg_vtlz_idex')} "
            f"({meta.get('percentile_band_ko', '—')}, 도로 {int(meta.get('n_roads', 0))}개 구간 집계) · {w*100:.0f}% 블렌딩{gs_note}"
        )
    narr = narrative_vitality_ko(meta, w)
    return out, bmeta, meta, narr


def invalidate_vitality_cache() -> None:
    global _LOADED_YR, _BY_SIGUNGU, _SORTED_AVG_VTLZ
    with _V_LOCK:
        _LOADED_YR = ""
        _BY_SIGUNGU = {}
        _SORTED_AVG_VTLZ = []
    try:
        from engine.trade_area_retail import invalidate_trade_area_retail_cache

        invalidate_trade_area_retail_cache()
    except Exception:
        pass


def enrich_stage2_candidate_vitality(
    c: Dict[str, Any],
    *,
    dept: str,
    kakao_rest_key: str,
    master_admin_region_name: str = "",
) -> None:
    la = float(c.get("lat") or 0)
    ln = float(c.get("lng") or 0)
    proxy, meta = lookup_vitality_for_coordinates(
        la, ln, kakao_rest_key, master_admin_region_name=master_admin_region_name
    )
    if proxy is None or not meta or not meta.get("matched"):
        from engine.trade_area_retail import lookup_trade_area_vitality

        ta_proxy, ta_meta = lookup_trade_area_vitality(la, ln)
        if ta_proxy is not None and ta_meta and ta_meta.get("matched"):
            proxy, meta = ta_proxy, ta_meta
    if proxy is None or not meta or not meta.get("matched"):
        c["vitality_proxy_0_100"] = None
        c["vitality_norm"] = None
        return
    c["vitality_proxy_0_100"] = round(float(proxy), 2)
    c["vitality_norm"] = max(0.0, min(1.0, float(proxy) / 100.0))
    c["vitality_meta"] = {k: v for k, v in meta.items() if k != "vitality_whitebox_ko"}
