# -*- coding: utf-8 -*-
"""
SNS·유동 프록시 지수(fpop_scor) 통합:
- SQLite 테이블 sns_floating_population (database.init_db) 또는 동일 스키마 Supabase
- 법정동코드(카카오 coord2region B코드 10자리 → 앞 8자리로 CSV legaldong_cd와 매칭)
- 과목별 가중치로 종합 점수(0~10) 블렌딩
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

from engine.bjdong_mapper import lookup_code10_from_kakao_address_name

_LDONG_STATS_LOCK = threading.Lock()
_LDONG_STATS: Dict[str, Dict[str, float]] = {}
_ALL_MEANS_SORTED: List[float] = []
_STATS_LOADED = False


def invalidate_stats_cache() -> None:
    """SQLite 적재 후 프로세스 내 집계 캐시를 비웁니다."""
    global _STATS_LOADED, _LDONG_STATS, _ALL_MEANS_SORTED
    with _LDONG_STATS_LOCK:
        _STATS_LOADED = False
        _LDONG_STATS = {}
        _ALL_MEANS_SORTED = []


_B_CODE_CACHE_LOCK = threading.Lock()
# 값: (만료 시각 epoch, Optional[B코드]). 성공은 장기 캐시, 실패(None)는 짧게만 캐시해
# 일시적 카카오 오류·429·해안 B코드 공백이 24시간 동안 고정되는 현상을 막음.
_B_CODE_CACHE: Dict[Tuple[int, int], Tuple[float, Optional[str]]] = {}
_CACHE_TTL_SUCCESS_SEC = float(os.environ.get("BLUEDOT_KAKAO_BCODE_CACHE_SEC", "86400"))
_CACHE_TTL_FAIL_SEC = float(os.environ.get("BLUEDOT_KAKAO_BCODE_FAIL_CACHE_SEC", "120"))

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _db_path() -> str:
    p = (os.environ.get("BLUEDOT_DB_PATH") or "").strip()
    if p:
        return p
    return os.path.join(_BASE, "bluedot.db")


def sns_fpop_weight_for_dept(dept: str, hanui_best_type: Optional[str] = None) -> float:
    """비급여·미용 계열은 SNS·핫플 프록시 비중 ↑, 급여·내과형은 ↓."""
    d = (dept or "").strip()
    bt = (hanui_best_type or "") or ""
    if d in ("피부과", "성형외과"):
        return 0.30
    if d == "치과":
        return 0.22
    if d == "한의원":
        if "미용" in bt or "다이어트" in bt or "타입B" in bt:
            return 0.28
        if "통증" in bt or "전통" in bt or "타입A" in bt:
            return 0.10
        return 0.18
    if d in ("내과", "이비인후과"):
        return 0.10
    if d == "소아과":
        return 0.12
    if d in ("정신건강의학과", "산부인과", "안과", "정형외과"):
        return 0.16
    return 0.15


def blend_scores_0_10(base_0_10: float, fpop_0_100: Optional[float], weight: float) -> Tuple[float, Dict[str, Any]]:
    w = max(0.0, min(0.35, float(weight)))
    if fpop_0_100 is None:
        return float(base_0_10), {"applied": False, "weight": w, "reason": "sns_data_missing"}
    fp = max(0.0, min(100.0, float(fpop_0_100)))
    fp_as_10 = fp / 10.0
    out = (1.0 - w) * float(base_0_10) + w * fp_as_10
    out = max(0.0, min(10.0, out))
    meta = {
        "applied": True,
        "weight": w,
        "fpop_raw": round(fp, 2),
        "base_score": round(float(base_0_10), 3),
        "blended": round(out, 3),
    }
    return round(out, 1), meta


def _percentile_band(mean_score: float, sorted_means: List[float]) -> str:
    if not sorted_means or mean_score is None:
        return "—"
    i = bisect.bisect_right(sorted_means, mean_score)
    pct = 100.0 * i / len(sorted_means)
    if pct >= 95:
        return "상위 5%"
    if pct >= 90:
        return "상위 10%"
    if pct >= 75:
        return "상위 25%"
    if pct >= 50:
        return "상위 50%"
    return "중하위권"


def narrative_ko_for_fpop(
    *,
    dept: str,
    fpop_mean: Optional[float],
    fpop_A: Optional[float],
    fpop_C: Optional[float],
    weight: float,
    availability: str = "ok",
) -> str:
    if availability == "dong_not_in_grid":
        return ""
    if fpop_mean is None:
        return ""
    fa = fpop_A if fpop_A is not None else fpop_mean
    fc = fpop_C if fpop_C is not None else None
    w_pct = int(round(weight * 100))
    tail = ""
    if fc is not None and fc >= 60:
        tail = " 인근 식음료·리테일(A·C 계열) SNS 노출도가 높아 2030 타깃 유입에 유리한 구간으로 해석됩니다."
    elif fa is not None and fa >= 65:
        tail = " 소매·서비스 상권의 온라인 노출 신호가 강해 비급여·자발적 방문 동선과 궁합이 좋습니다."
    else:
        tail = " 온라인·현장 유동 프록시가 과목별 가중치에 맞춰 종합 점수에 반영되었습니다."
    return (
        f"이 권역은 SNS·유동 프록시 지수(동 단위 평균 약 {fpop_mean:.1f}/100)가 확보되었고, "
        f"최종 점수에 약 {w_pct}% 비중으로 반영했습니다.{tail}"
    )


def _ensure_ldong_stats_loaded() -> None:
    global _STATS_LOADED, _LDONG_STATS, _ALL_MEANS_SORTED
    with _LDONG_STATS_LOCK:
        if _STATS_LOADED:
            return
        _LDONG_STATS = {}
        path = _db_path()
        if not os.path.isfile(path):
            _STATS_LOADED = True
            _ALL_MEANS_SORTED = []
            return
        try:
            conn = sqlite3.connect(path)
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sns_floating_population'"
            )
            if not cur.fetchone():
                conn.close()
                _STATS_LOADED = True
                _ALL_MEANS_SORTED = []
                return
            q = """
                SELECT legaldong_cd,
                       AVG(fpop_scor) AS m,
                       MAX(fpop_scor) AS mx,
                       AVG(CASE WHEN substr(induty_cd,1,1)='A' THEN fpop_scor END) AS ma,
                       AVG(CASE WHEN substr(induty_cd,1,1)='B' THEN fpop_scor END) AS mb,
                       AVG(CASE WHEN substr(induty_cd,1,1)='C' THEN fpop_scor END) AS mc
                FROM sns_floating_population
                GROUP BY legaldong_cd
            """
            for row in conn.execute(q):
                ld, m, mx, ma, mb, mc = row
                if not ld:
                    continue
                key = str(ld).strip()
                stats_row = {
                    "mean": float(m) if m is not None else 0.0,
                    "max": float(mx) if mx is not None else 0.0,
                    "mean_A": float(ma) if ma is not None else float(m or 0),
                    "mean_B": float(mb) if mb is not None else float(m or 0),
                    "mean_C": float(mc) if mc is not None else float(m or 0),
                }
                _LDONG_STATS[key] = stats_row
                dnorm = re.sub(r"\D", "", key)
                if dnorm:
                    _LDONG_STATS[dnorm] = stats_row
                    if len(dnorm) >= 8:
                        _LDONG_STATS[dnorm[:8]] = stats_row
                    if len(dnorm) >= 10:
                        _LDONG_STATS[dnorm[:10]] = stats_row
            conn.close()
            _ALL_MEANS_SORTED = sorted(v["mean"] for v in _LDONG_STATS.values())
        except Exception as e:
            logging.warning("sns_floating: stats load failed: %s", e)
            _LDONG_STATS = {}
            _ALL_MEANS_SORTED = []
        _STATS_LOADED = True


def lookup_ldong_fpop(legaldong_cd10: Optional[str]) -> Optional[Dict[str, float]]:
    if not legaldong_cd10:
        return None
    digits = re.sub(r"\D", "", str(legaldong_cd10))
    if len(digits) < 8:
        return None
    _ensure_ldong_stats_loaded()
    for n in (10, 9, 8):
        if len(digits) >= n:
            key = digits[:n]
            hit = _LDONG_STATS.get(key)
            if hit:
                return hit
    return None


def trend_fpop_availability(
    *,
    kakao_rest_key: str,
    b10: Optional[str],
    stats: Optional[Dict[str, float]],
    b10_source: str = "none",
) -> str:
    """
    ok | no_kakao | no_server_data | no_kakao_coord_fail | dong_not_in_grid
    dong_not_in_grid: 카카오 B코드는 있으나 SNS 격자에 해당 법정동 행 없음 → UI에서 블록 생략.
    b10_source=master_admin_label 이면 카카오 키 없이도 마스터 행정구역명으로 법정동 매칭 성공.
    subway_anchor_kakao 는 역 좌표로 카카오 B코드를 확보한 경우(핀 좌표는 B코드 공백).
    """
    kk = (kakao_rest_key or "").strip()
    _ensure_ldong_stats_loaded()
    if not _LDONG_STATS:
        return "no_server_data"
    if not kk and b10_source not in ("master_admin_label",):
        return "no_kakao"
    if not b10:
        return "no_kakao_coord_fail" if kk else "no_kakao"
    if not stats:
        return "dong_not_in_grid"
    return "ok"


def _best_b_code_from_region_documents(data: dict) -> Optional[str]:
    """documents 중 region_type=B 전부 검사해 가장 긴(가장 하위) 법정동 코드 선택."""
    best: Optional[str] = None
    best_len = 0
    for doc in data.get("documents") or []:
        rt = str(doc.get("region_type") or "").strip().upper()
        if rt != "B":
            continue
        raw = doc.get("code")
        code = re.sub(r"\D", "", str(raw) if raw is not None else "")
        if len(code) < 8:
            continue
        b = code[:10] if len(code) >= 10 else code
        ln = len(re.sub(r"\D", "", b))
        if ln > best_len:
            best_len = ln
            best = b
    return best


def _b_code_from_region_extended(data: Optional[dict]) -> Optional[str]:
    """
    B코드 필드가 비거나 해안 등으로 B가 약할 때, H(행정동) depth1~3 이름을 이어
    법정동코드 테이블에서 역매칭(행정·법정 명칭이 겹치는 경우).
    """
    if not data or not isinstance(data, dict):
        return None
    best = _best_b_code_from_region_documents(data)
    if best:
        return best
    docs = data.get("documents") or []
    if not isinstance(docs, list):
        return None
    tried: set = set()
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        rt = str(doc.get("region_type") or "").strip().upper()
        if rt != "H":
            continue
        d1 = str(doc.get("region_1depth_name") or "").strip()
        d2 = str(doc.get("region_2depth_name") or "").strip()
        d3 = str(doc.get("region_3depth_name") or "").strip()
        if not (d1 and d2 and d3):
            continue
        synthetic = f"{d1} {d2} {d3}"
        if synthetic in tried:
            continue
        tried.add(synthetic)
        c10 = lookup_code10_from_kakao_address_name(synthetic)
        if c10:
            return c10
    return None


def kakao_b_code_for_latlng(lat: float, lng: float, kakao_rest_key: str) -> Optional[str]:
    if not (kakao_rest_key or "").strip():
        return None
    # 캐시 키 (lat,lng). 카카오 region/address API: x=경도, y=위도
    cache_lat = round(float(lat), 4)
    cache_lng = round(float(lng), 4)
    now = time.time()
    with _B_CODE_CACHE_LOCK:
        ent = _B_CODE_CACHE.get((cache_lat, cache_lng))
        if ent:
            expires_at, cached_val = ent
            if now <= expires_at:
                return cached_val
            try:
                del _B_CODE_CACHE[(cache_lat, cache_lng)]
            except KeyError:
                pass

    headers = {"Authorization": f"KakaoAK {kakao_rest_key.strip()}"}
    region_url = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
    addr_url = "https://dapi.kakao.com/v2/local/geo/coord2address.json"

    def _region_at(la: float, lo: float, *, allow_retry: bool = True) -> Optional[str]:
        try:
            r = requests.get(
                region_url,
                headers=headers,
                params={"x": str(lo), "y": str(la)},
                timeout=6.0,
            )
            if r.status_code == 429 or r.status_code == 503:
                if allow_retry:
                    time.sleep(0.4)
                    return _region_at(la, lo, allow_retry=False)
                return None
            if r.status_code != 200:
                return None
            return _b_code_from_region_extended(r.json())
        except Exception as e:
            logging.debug("kakao coord2region: %s", e)
            return None

    found: Optional[str] = None
    la0, lo0 = float(lat), float(lng)
    # 미세 오프셋: 해안·공원·신축 단지 등에서 B가 비는 경우 완화 (~200m + 대각)
    nudges = (
        (0.0, 0.0),
        (0.0018, 0.0),
        (-0.0018, 0.0),
        (0.0, 0.0018),
        (0.0, -0.0018),
        (0.0012, 0.0012),
        (-0.0012, 0.0012),
        (0.0012, -0.0012),
        (-0.0012, -0.0012),
    )
    for du, dv in nudges:
        found = _region_at(la0 + du, lo0 + dv)
        if found:
            break

    if not found:
        for du, dv in nudges:
            try:
                la, lo = la0 + du, lo0 + dv
                r2 = requests.get(
                    addr_url,
                    headers=headers,
                    params={"x": str(lo), "y": str(la), "input_coord": "WGS84"},
                    timeout=6.0,
                )
                if r2.status_code in (429, 503):
                    time.sleep(0.4)
                    r2 = requests.get(
                        addr_url,
                        headers=headers,
                        params={"x": str(lo), "y": str(la), "input_coord": "WGS84"},
                        timeout=6.0,
                    )
                if r2.status_code != 200:
                    continue
                docs = r2.json().get("documents") or []
                if not docs:
                    continue
                d0 = docs[0]
                adr = d0.get("address") or {}
                nm = str(adr.get("address_name") or "").strip()
                if not nm and d0.get("road_address"):
                    nm = str((d0.get("road_address") or {}).get("address_name") or "").strip()
                if nm:
                    found = lookup_code10_from_kakao_address_name(nm)
                    if found:
                        break
            except Exception as e:
                logging.debug("kakao coord2address / bjdong fallback: %s", e)

    ttl = _CACHE_TTL_SUCCESS_SEC if found else _CACHE_TTL_FAIL_SEC
    with _B_CODE_CACHE_LOCK:
        _B_CODE_CACHE[(cache_lat, cache_lng)] = (now + ttl, found)
    return found


def resolve_fpop_for_coordinates(
    lat: float,
    lng: float,
    kakao_rest_key: str,
    master_admin_region_name: str = "",
) -> Tuple[Optional[float], Optional[Dict[str, float]], Optional[str], str]:
    """
    동 단위 대표 지수.
    b10_source: kakao | master_admin_label | subway_anchor_kakao | none
    핀 좌표에서 B코드가 비는 경우(해안·신축 등) 최근접 역 좌표로 coord2region 재시도.
    """
    from engine.geo_admin_fallback import legaldong10_from_admin_region_label

    b10 = kakao_b_code_for_latlng(lat, lng, kakao_rest_key)
    src = "kakao" if b10 else "none"
    if not b10:
        label = (master_admin_region_name or "").strip()
        if label:
            b10 = legaldong10_from_admin_region_label(label)
            if b10:
                src = "master_admin_label"
    if not b10 and (kakao_rest_key or "").strip():
        from engine.subway_floating import _nearest_subway_meta_and_proxy

        _, sm = _nearest_subway_meta_and_proxy(lat, lng)
        sla, slo = sm.get("nearest_lat"), sm.get("nearest_lng")
        if sla is not None and slo is not None:
            b10 = kakao_b_code_for_latlng(float(sla), float(slo), kakao_rest_key)
            if b10:
                src = "subway_anchor_kakao"
    stats = lookup_ldong_fpop(b10)
    if not stats:
        return None, None, b10, src
    return stats["mean"], stats, b10, src


def dept_weighted_fpop_score(stats: Dict[str, float], dept: str) -> float:
    """과목에 따라 A/B/C 가중 평균으로 단일 0~100 점수."""
    d = (dept or "").strip()
    ma, mb, mc = stats.get("mean_A", 0), stats.get("mean_B", 0), stats.get("mean_C", 0)
    if d in ("피부과", "성형외과", "치과"):
        return 0.45 * ma + 0.25 * mc + 0.30 * mb
    if d in ("내과", "이비인후과", "소아과"):
        return 0.25 * ma + 0.20 * mc + 0.55 * mb
    if d == "한의원":
        return 0.35 * ma + 0.30 * mc + 0.35 * mb
    return stats.get("mean", 0.0)


def _vitality_region_hint_ko(v_fields: Optional[Dict[str, Any]]) -> Optional[str]:
    """ES1013는 있으나 해당 시·군·구 행만 없을 때 등, 좌표로 확보된 행정구역만 짧게 표시."""
    if not v_fields or v_fields.get("matched"):
        return None
    if v_fields.get("block_reason") == "es1013_empty":
        return None
    sido = str(v_fields.get("kakao_sido") or "").strip()
    sig = str(v_fields.get("kakao_sigungu") or "").strip()
    if sido and sig:
        return f"{sido} {sig}"
    return None


def build_trend_payload_for_node(
    *,
    lat: float,
    lng: float,
    dept: str,
    base_score_0_10: float,
    hanui_best_type: Optional[str] = None,
    kakao_rest_key: str = "",
    master_admin_region_name: str = "",
) -> Dict[str, Any]:
    w = sns_fpop_weight_for_dept(dept, hanui_best_type)
    mean_f, stats, b10, b10_src = resolve_fpop_for_coordinates(
        lat, lng, kakao_rest_key, master_admin_region_name=master_admin_region_name
    )
    f_for_blend = dept_weighted_fpop_score(stats, dept) if stats else None
    blended, blend_meta = blend_scores_0_10(base_score_0_10, f_for_blend, w)
    from engine.subway_floating import apply_subway_blend_after_sns

    final_blended, sub_meta, sub_fields, sub_narr = apply_subway_blend_after_sns(blended, lat, lng, dept)
    from engine.commercial_vitality import apply_vitality_blend_after_subway

    v_final, v_bmeta, v_fields, v_narr = apply_vitality_blend_after_subway(
        final_blended,
        lat,
        lng,
        dept,
        kakao_rest_key,
        master_admin_region_name=master_admin_region_name,
    )
    from engine.retail_supply_ac import (
        lookup_retail_supply_for_legaldong,
        retail_supply_ac_dataset_loaded,
        retail_supply_narrative_ko,
        retail_supply_whitebox_ko,
    )

    rs = lookup_retail_supply_for_legaldong(b10)
    _ac_loaded = retail_supply_ac_dataset_loaded()
    band = _percentile_band(mean_f or 0.0, _ALL_MEANS_SORTED) if mean_f is not None else "—"
    avail = trend_fpop_availability(
        kakao_rest_key=kakao_rest_key, b10=b10, stats=stats, b10_source=b10_src
    )
    narr = narrative_ko_for_fpop(
        dept=dept,
        fpop_mean=mean_f,
        fpop_A=stats.get("mean_A") if stats else None,
        fpop_C=stats.get("mean_C") if stats else None,
        weight=w,
        availability=avail,
    )
    return {
        "trend_floating_score": v_final,
        "trend_fpop_display": round(f_for_blend, 1) if f_for_blend is not None else None,
        "trend_fpop_mean_dong": round(mean_f, 2) if mean_f is not None else None,
        "trend_percentile_band_ko": band,
        "trend_weight": w,
        "trend_blend_meta": blend_meta,
        "trend_narrative_ko": narr,
        "trend_fpop_availability": avail,
        "legaldong_b_code": b10,
        "trend_geo_source": b10_src,
        "trend_fpop_components": stats,
        "subway_blend_meta": sub_meta,
        "subway_narrative_ko": sub_narr,
        "subway_card_silence": bool(sub_meta.get("reason") == "subway_no_hub_nearby"),
        "subway_hub_whitebox": sub_fields.get("subway_whitebox_ko") if sub_fields else None,
        "subway_nearest_stn_nm": sub_fields.get("nearest_nm") if sub_fields else None,
        "subway_nearest_dist_m": sub_fields.get("nearest_dist_m") if sub_fields else None,
        "subway_fpop_proxy_0_100": round(sub_meta.get("fpop_raw"), 2)
        if sub_meta.get("applied")
        else None,
        "subway_percentile_band_ko": sub_fields.get("percentile_band_ko") if sub_fields else None,
        "subway_blend_weight": sub_meta.get("weight"),
        "vitality_blend_meta": v_bmeta,
        "vitality_narrative_ko": v_narr,
        "vitality_whitebox": v_fields.get("vitality_whitebox_ko") if v_fields else None,
        "vitality_sigungu_avg": v_fields.get("avg_vtlz_idex") if v_fields else None,
        "vitality_percentile_band_ko": v_fields.get("percentile_band_ko") if v_fields else None,
        "vitality_blend_weight": v_bmeta.get("weight"),
        "vitality_proxy_0_100": round(v_fields["proxy_0_100"], 2)
        if v_fields and v_fields.get("proxy_0_100") is not None
        else None,
        "vitality_block_reason": (v_fields or {}).get("block_reason"),
        "vitality_data_source": (v_fields or {}).get("vitality_source"),
        "vitality_trdar_nm": (v_fields or {}).get("trdar_nm"),
        "vitality_region_hint_ko": _vitality_region_hint_ko(v_fields if v_fields else None),
        "retail_supply_avg": rs.get("avg_spl_dims") if rs else None,
        "retail_supply_percentile_band_ko": rs.get("percentile_band_ko") if rs else None,
        "retail_supply_n_parcels": int(rs["n_parcels"]) if rs else None,
        "retail_supply_strd_ym": rs.get("data_strd_ym") if rs else None,
        "retail_supply_narrative_ko": retail_supply_narrative_ko(rs) if rs else None,
        "retail_supply_whitebox_ko": retail_supply_whitebox_ko(rs) if rs else None,
        "retail_supply_catalog_loaded": _ac_loaded,
    }


def enrich_stage2_candidate_trend(
    c: Dict[str, Any],
    *,
    dept: str,
    kakao_rest_key: str,
) -> None:
    """후보별 fpop(0~100)를 붙이고 마커 색용 정규화."""
    la = float(c.get("lat") or 0)
    ln = float(c.get("lng") or 0)
    pname = str(c.get("parent_region_name") or "").strip()
    mean_f, stats, _, _ = resolve_fpop_for_coordinates(
        la, ln, kakao_rest_key, master_admin_region_name=pname
    )
    f100 = dept_weighted_fpop_score(stats, dept) if stats else None
    c["trend_fpop_0_100"] = round(f100, 2) if f100 is not None else None
    c["trend_fpop_mean_dong"] = round(mean_f, 2) if mean_f is not None else None
    if f100 is None:
        c["trend_fpop_norm"] = None
    else:
        c["trend_fpop_norm"] = max(0.0, min(1.0, f100 / 100.0))
    from engine.subway_floating import enrich_stage2_candidate_subway

    enrich_stage2_candidate_subway(c, dept=dept)
    from engine.commercial_vitality import enrich_stage2_candidate_vitality

    enrich_stage2_candidate_vitality(
        c, dept=dept, kakao_rest_key=kakao_rest_key, master_admin_region_name=pname
    )
