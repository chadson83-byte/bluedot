# -*- coding: utf-8 -*-
"""
미시 입지: 6축 프롭테크 스코어 + 카카오·(선택)네이버 로컬 병합 POI·심평원·마스터 CSV.
네이버: openapi.naver.com/v1/search/local (Client ID/Secret). 미설정 시 카카오만 사용.
2단계: 학교·아파트·공원은 카카오 키워드 거리로 부지 프록시(실지적 폴리곤은 추후 GIS).
"""
from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from engine.geo_utils import haversine_km, offset_lat_lng

_MICRO_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_MICRO_LOCK = threading.Lock()
_MICRO_TTL = 1800.0
_MICRO_CACHE_MAX = 400


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return float(haversine_km(lat1, lon1, lat2, lon2)) * 1000.0


def _hospital_lat_lng(h: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    try:
        la = float(h.get("lat"))
        ln = float(h.get("lng"))
        return la, ln
    except (TypeError, ValueError):
        return None


def _count_anchors_within(lat: float, lng: float, radius_m: float, anchors: List[Dict[str, Any]]) -> int:
    n = 0
    for p in anchors or []:
        try:
            la = float(p.get("lat"))
            ln = float(p.get("lng"))
        except (TypeError, ValueError):
            continue
        if haversine_m(lat, lng, la, ln) <= radius_m:
            n += 1
    return n


def _count_hospitals_within(lat: float, lng: float, radius_m: float, hospitals: List[Dict[str, Any]]) -> int:
    n = 0
    for h in hospitals or []:
        pair = _hospital_lat_lng(h)
        if not pair:
            continue
        if haversine_m(lat, lng, pair[0], pair[1]) <= radius_m:
            n += 1
    return n


# 2단계: GIS 폴리곤 없이 카카오 키워드로 학교·단지·공원 프록시 (실폴리곤 마스킹은 추후 브이월드/OSM)
LAND_USE_KAKAO_QUERIES: List[Tuple[str, str]] = [
    ("초등학교", "school"),
    ("중학교", "school"),
    ("고등학교", "school"),
    ("유치원", "school"),
    ("아파트", "apartment"),
    ("아파트단지", "apartment"),
    ("공원", "park"),
]


def collect_land_use_hazard_pois(
    *,
    kakao_key: str,
    lat: float,
    lng: float,
    radius_m: int = 1200,
) -> List[Dict[str, Any]]:
    """권역 중심 기준 학교·아파트·공원 후보 좌표(중복 제거). 폴리곤 대체 프록시."""
    if not (kakao_key or "").strip():
        return []
    merged: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(LAND_USE_KAKAO_QUERIES))) as ex:
        futs: Dict[Any, Tuple[str, str]] = {}
        for q, kind in LAND_USE_KAKAO_QUERIES:
            fut = ex.submit(
                kakao_keyword_search,
                kakao_key=kakao_key,
                lat=lat,
                lng=lng,
                radius_m=radius_m,
                query=q,
            )
            futs[fut] = (q, kind)
        for fut in as_completed(futs):
            q, kind = futs[fut]
            try:
                rows = fut.result()
            except Exception as e:
                logging.warning("land_use hazard %s: %s", q, e)
                continue
            for row in rows or []:
                key = str(row.get("id") or "").strip()
                if not key:
                    try:
                        key = f"{float(row.get('lat') or 0):.5f},{float(row.get('lng') or 0):.5f}"
                    except (TypeError, ValueError):
                        key = ""
                if not key or key in merged:
                    continue
                merged[key] = {**row, "hazard_kind": kind}
    return list(merged.values())


def evaluate_land_use_for_candidate(
    lat: float,
    lng: float,
    hazards: List[Dict[str, Any]],
    anchor_poi_count_100m: int,
    anchor_poi_count_300m: int,
    *,
    offset_dir: str = "중심",
    offset_m: float = 0.0,
    bank_poi_count_150m: int = 0,
    mart_poi_count_300m: int = 0,
) -> Dict[str, Any]:
    """
    하드 제외: 학교·공원, 아파트 단지 심부(거리·상업전면 결합).
    1단계 권역 '중심' + 근접 상업 無 → 단지핵·골목 후보로 강제 제외.
    이면도로: 무앵커·중간링 앵커 등 추가 감점.
    """
    min_s = min_a = min_p = None
    for h in hazards or []:
        try:
            plat = float(h.get("lat"))
            plng = float(h.get("lng"))
        except (TypeError, ValueError):
            continue
        d = haversine_m(lat, lng, plat, plng)
        k = str(h.get("hazard_kind") or "")
        if k == "school":
            min_s = d if min_s is None else min(min_s, d)
        elif k == "apartment":
            min_a = d if min_a is None else min(min_a, d)
        elif k == "park":
            min_p = d if min_p is None else min(min_p, d)

    reasons: List[str] = []
    hard = False
    # 학교: 교정 내부 후보 차단 (상업앵커와 무관하게 강하게)
    if min_s is not None and min_s < 118.0:
        hard = True
        reasons.append(f"학교·교육시설 인접 약 {int(min_s)}m (부지내부 후보 제외)")
    # 공원: 시설부지 인접 + 근처에 상업앵커 거의 없을 때만 하드
    if (
        not hard
        and min_p is not None
        and min_p < 58.0
        and anchor_poi_count_100m < 1
    ):
        hard = True
        reasons.append(f"공원부지 인접 약 {int(min_p)}m (제외)")
    nb = int(bank_poi_count_150m)
    nmt = int(mart_poi_count_300m)
    comm0 = anchor_poi_count_100m < 1 and nb < 1 and nmt < 1

    # 아파트: 대표 POI가 멀어도 단지 내부일 수 있어 반경 확대. 상업전면(앵커·은행·마트) 있으면 완화.
    if (
        not hard
        and min_a is not None
        and min_a < 145.0
        and anchor_poi_count_100m < 2
        and nb < 1
    ):
        hard = True
        reasons.append(f"아파트·단지 시설 인접 약 {int(min_a)}m (대로 상가 신호 부족 시 제외)")
    # 단지와 멀어도 '상업 사막'이면 단지권 추정
    if (
        not hard
        and min_a is not None
        and min_a < 260.0
        and comm0
        and anchor_poi_count_300m <= 3
    ):
        hard = True
        reasons.append("아파트권·상업POI 공백(300m 내 앵커 소수) → 제외")
    # 1단계 격자 중심 = 주거 블록 중심에 가깝다는 가정 + 주변 상업 없음
    od = (offset_dir or "중심").strip()
    om = float(offset_m or 0.0)
    if not hard and od == "중심" and om <= 1.0 and comm0 and anchor_poi_count_300m <= 4:
        hard = True
        reasons.append("권역 중심 좌표 + 근접 상업·은행·마트 없음 → 단지·내부도로 후보 제외")

    mult = 1.0
    if not hard and min_a is not None and min_a < 220.0 and anchor_poi_count_100m < 2:
        mult *= max(0.45, 0.38 + min_a / 480.0)
        reasons.append("아파트단지 중거리(상업전면 약할 때 감점)")

    # 이면도로·단지내부: 300m 앵커 소량(띠만 있고 전면 없음)
    if anchor_poi_count_100m == 0 and 2 <= anchor_poi_count_300m <= 5:
        mult *= 0.38
        reasons.append("100m무앵커·300m소량앵커 → 골목·단지내부 프록시(강감점)")
    if anchor_poi_count_100m == 0 and anchor_poi_count_300m >= 6:
        mult *= 0.48
        reasons.append("100m무앵커·300m다앵커 → 상권 띠 바깥(이면) 프록시")
    elif anchor_poi_count_100m == 0 and anchor_poi_count_300m <= 1:
        mult *= 0.62
        reasons.append("근접 상업앵커 매우 부족")

    return {
        "hard_exclude": hard,
        "score_mult": max(0.08, min(1.0, mult)),
        "reasons": reasons,
        "min_dist_school_m": min_s,
        "min_dist_apartment_m": min_a,
        "min_dist_park_m": min_p,
    }


def candidate_offsets_9(center_lat: float, center_lng: float, offset_m: float = 125.0) -> List[Tuple[float, float, float, str]]:
    """
    (lat, lng, dist_m, dir_ko) — 중심 1 + 8방향 offset_m.
    bearing: 0=북, 90=동, … (offset_lat_lng 규약)
    """
    dirs = {
        0: "북",
        45: "북동",
        90: "동",
        135: "남동",
        180: "남",
        225: "남서",
        270: "서",
        315: "북서",
    }
    pts: List[Tuple[float, float, float, str]] = [(center_lat, center_lng, 0.0, "중심")]
    for deg, label in dirs.items():
        la, ln = offset_lat_lng(center_lat, center_lng, offset_m, float(deg))
        pts.append((la, ln, offset_m, label))
    return pts


def build_region_candidate_scores(
    *,
    center_lat: float,
    center_lng: float,
    parent_name: str,
    parent_rank: int,
    eval_radius_m: int,
    anchors: List[Dict[str, Any]],
    hospitals: List[Dict[str, Any]],
    dept: str,
    df_master: Any,
    resolve_master_ctx: Callable[..., Any],
    offset_m: float = 125.0,
    kakao_key: str = "",
    naver_client_id: str = "",
    naver_client_secret: str = "",
) -> List[Dict[str, Any]]:
    """한 권역(1차 노드) 내 9개 후보 좌표에 대해 앵커·경쟁·마스터·토지용도·은행·마트 기반 미시 점수."""
    hazards = collect_land_use_hazard_pois(
        kakao_key=kakao_key,
        lat=center_lat,
        lng=center_lng,
        radius_m=min(2000, max(900, int(eval_radius_m) * 3 + 400)),
    )
    rf_radius = min(2000, max(900, int(eval_radius_m) * 3 + 500))
    bank_places, mart_places, rf_meta = collect_retail_finance_pois(
        kakao_key=kakao_key,
        naver_client_id=naver_client_id,
        naver_client_secret=naver_client_secret,
        lat=center_lat,
        lng=center_lng,
        radius_m=rf_radius,
    )
    out: List[Dict[str, Any]] = []
    for la, ln, dist_m, dir_label in candidate_offsets_9(center_lat, center_lng, offset_m=offset_m):
        n_a = _count_anchors_within(la, ln, float(eval_radius_m), anchors)
        n_c = _count_hospitals_within(la, ln, float(eval_radius_m), hospitals)
        n_a_100 = _count_anchors_within(la, ln, 100.0, anchors)
        n_a_300 = _count_anchors_within(la, ln, 300.0, anchors)
        n_c_100 = _count_hospitals_within(la, ln, 100.0, hospitals)
        n_bank_150 = _count_retail_within(la, ln, 150.0, bank_places)
        n_mart_300 = _count_retail_within(la, ln, 300.0, mart_places)
        land_use = evaluate_land_use_for_candidate(
            la,
            ln,
            hazards,
            n_a_100,
            n_a_300,
            offset_dir=dir_label,
            offset_m=float(dist_m),
            bank_poi_count_150m=n_bank_150,
            mart_poi_count_300m=n_mart_300,
        )
        ctx = None
        if df_master is not None and hasattr(df_master, "empty") and not df_master.empty:
            try:
                ctx = resolve_master_ctx(df_master, la, ln, radius_km=3.0)
            except Exception:
                ctx = None
        act = ctx.get("activity_index") if ctx else None
        young = ctx.get("young_ratio") if ctx else None
        row = (ctx.get("row") or {}) if ctx else {}
        try:
            rclat = float(row.get("center_lat")) if row.get("center_lat") is not None else None
            rclng = float(row.get("center_lng")) if row.get("center_lng") is not None else None
        except (TypeError, ValueError):
            rclat, rclng = None, None
        try:
            mpop = float(row.get("총인구 (명)")) if row.get("총인구 (명)") is not None else None
        except (TypeError, ValueError):
            mpop = None
        if mpop is None:
            try:
                mpop = float(row.get("총인구(명)")) if row.get("총인구(명)") is not None else None
            except (TypeError, ValueError):
                mpop = None
        try:
            bsc = float(row.get("bus_stop_count")) if row.get("bus_stop_count") is not None else None
        except (TypeError, ValueError):
            bsc = None
        sc = score_proptech_clinic_site(
            lat=la,
            lng=ln,
            region_center_lat=rclat,
            region_center_lng=rclng,
            offset_dir=dir_label,
            offset_m=dist_m,
            anchor_poi_count_100m=n_a_100,
            anchor_poi_count_300m=n_a_300,
            medical_facility_count_100m=n_c_100,
            master_activity_index=act,
            young_ratio=young,
            master_total_pop=mpop,
            bus_stop_count=bsc,
            land_use_hard_exclude=bool(land_use.get("hard_exclude")),
            land_use_mult=float(land_use.get("score_mult") or 1.0),
            land_use_reasons=list(land_use.get("reasons") or []),
            bank_poi_count_150m=n_bank_150,
            mart_poi_count_300m=n_mart_300,
        )
        pname = (parent_name or "").strip() or "권역"
        pr = int(parent_rank) if parent_rank else 0
        suffix = f" · {dir_label}" if dir_label != "중심" else " · 중심"
        if dist_m > 0:
            suffix += f" {int(dist_m)}m"
        out.append({
            "lat": la,
            "lng": ln,
            "label_ko": f"Rank{pr} {pname}{suffix}",
            "parent_rank": pr,
            "parent_region_name": pname,
            "offset_dir": dir_label,
            "offset_m": dist_m,
            "eval_radius_m": eval_radius_m,
            "department": dept,
            "competitor_count": n_c,
            "anchor_poi_count": n_a,
            "anchor_poi_count_100m": n_a_100,
            "anchor_poi_count_300m": n_a_300,
            "medical_facility_count_100m": n_c_100,
            "land_use_screening": land_use,
            "retail_finance_meta": rf_meta,
            "bank_poi_count_150m": n_bank_150,
            "mart_poi_count_300m": n_mart_300,
            "scoring": sc,
            "region_proxy": {
                "name": ctx.get("region_name") if ctx else None,
                "distance_km": ctx.get("distance_km") if ctx else None,
            }
            if ctx
            else None,
        })
    return out


def build_stage2_selection_rationale_ko(c: Dict[str, Any]) -> str:
    """2단계 후보별 선정 이유 — 6축 프롭테크 스코어 기준 요약."""
    sc = c.get("scoring") or {}
    comp = sc.get("components") or {}
    score = sc.get("score")
    grade = sc.get("grade_label_ko") or "-"
    parent_rank = int(c.get("parent_rank") or 0)
    parent_name = (c.get("parent_region_name") or "해당 권역").strip() or "해당 권역"
    offset_dir = c.get("offset_dir") or "중심"
    offset_m = c.get("offset_m")
    n_c = int(c.get("competitor_count") or 0)
    n_a = int(c.get("anchor_poi_count") or 0)
    n_a100 = int(c.get("anchor_poi_count_100m") or 0)
    n_m100 = int(c.get("medical_facility_count_100m") or 0)
    ft = float(comp.get("foot_traffic") or 0)
    vis = float(comp.get("visibility_access") or 0)
    res = float(comp.get("residential_proximity") or 0)
    anc = float(comp.get("anchor_franchises") or 0)
    med = float(comp.get("medical_synergy") or 0)
    if offset_dir == "중심" or not offset_m or float(offset_m) <= 0:
        loc = "권역 중심 좌표"
    else:
        loc = f"{offset_dir} 방향 약 {int(offset_m)}m 오프셋 지점"
    head = (
        f"1단계 {parent_rank}위 권역「{parent_name}」의 {loc}을 기준으로 "
        f"6축 입지 점수(유동·가시성·배후주거·앵커·메디컬시너지·주차)를 산출했습니다. "
        f"참고: 평가 반경 {int(c.get('eval_radius_m') or 0) or '—'}m 내 앵커 {n_a}곳·동일과목 의원 {n_c}곳, "
        f"100m 기준 앵커 {n_a100}곳·의료시설(동일과목) {n_m100}곳."
    )
    reasons: List[str] = []
    if ft >= 25:
        reasons.append("유동·상권 활력 프록시가 높은 편입니다.")
    if vis >= 15:
        reasons.append("코너·보행 접근성(버스정류장 프록시) 신호가 강합니다.")
    if res >= 14:
        reasons.append("행정동 핵심에 가까워 배후 수요 프록시가 유리합니다.")
    if anc >= 10:
        reasons.append("핵심 앵커 브랜드 100m 밀집이 뚜렷합니다.")
    if med >= 7:
        reasons.append("근접 의료시설(동일·유사 과목)이 다수 있어 메디컬 블록 프록시가 있습니다.")
    mid = " ".join(reasons) if reasons else "6축 합산으로 전체 후보 대비 상위 점수입니다."
    tail = f" (종합 {score}점 / 등급 {grade})"
    return head + " " + mid + tail


def enrich_stage2_top_with_rationale(rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        row["selection_rationale_ko"] = build_stage2_selection_rationale_ko(row)


def _stage2_candidate_sort_key(c: Dict[str, Any]) -> Tuple[float, float, int, int, int]:
    """동일 총점이면 좌표 기준 상업활력·근접 앵커가 높은 후보를 우선(골목·단지 동점 탈출)."""
    sc = c.get("scoring") or {}
    score = float(sc.get("score") or 0)
    na100 = int(c.get("anchor_poi_count_100m") or 0)
    na300 = int(c.get("anchor_poi_count_300m") or 0)
    nb = int(c.get("bank_poi_count_150m") or 0)
    nm = int(c.get("mart_poi_count_300m") or 0)
    vit = _commercial_vitality_01(na100, na300, nb, nm)
    return (-score, -vit, -na100, -nb, -nm)


def dedupe_pick_top(
    candidates: List[Dict[str, Any]],
    *,
    top_k: int = 5,
    min_sep_m: float = 55.0,
) -> List[Dict[str, Any]]:
    ranked = sorted(candidates, key=_stage2_candidate_sort_key)

    def _pick_pool(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for c in pool:
            la = float(c["lat"])
            ln = float(c["lng"])
            if any(haversine_m(la, ln, float(p["lat"]), float(p["lng"])) < min_sep_m for p in out):
                continue
            row = dict(c)
            row["stage2_rank"] = len(out) + 1
            out.append(row)
            if len(out) >= top_k:
                break
        return out

    # 학교·단지내부 등 하드제외 후보는 동일 권역에 대체가 있으면 표에서 제외
    safe = [
        c
        for c in ranked
        if not ((c.get("scoring") or {}).get("scoring_meta") or {}).get("hard_excluded")
    ]
    picked = _pick_pool(safe if safe else ranked)
    return picked


def _cache_get(key: str) -> Optional[List[Dict[str, Any]]]:
    now = time.time()
    with _MICRO_LOCK:
        t = _MICRO_CACHE.get(key)
        if not t:
            return None
        if now - t[0] > _MICRO_TTL:
            del _MICRO_CACHE[key]
            return None
        return t[1]


def _cache_set(key: str, val: List[Dict[str, Any]]) -> None:
    now = time.time()
    with _MICRO_LOCK:
        _MICRO_CACHE[key] = (now, val)
        while len(_MICRO_CACHE) > _MICRO_CACHE_MAX:
            try:
                del _MICRO_CACHE[next(iter(_MICRO_CACHE))]
            except (StopIteration, KeyError):
                break


def _round_key(lat: float, lng: float, radius_m: int, query: str) -> str:
    return f"{round(lat, 4)}|{round(lng, 4)}|{radius_m}|{query}"


def _strip_html_brief(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "").strip()


def _naver_mapxy_to_latlng(mapx: Any, mapy: Any) -> Optional[Tuple[float, float]]:
    """네이버 로컬 검색 mapx/mapy → WGS84 (정수 문자열 ×1e7 도 단위, 한반도 범위 검증)."""
    try:
        xi = int(str(mapx).strip())
        yi = int(str(mapy).strip())
    except (TypeError, ValueError):
        return None
    lng = xi / 1e7
    lat = yi / 1e7
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        return None
    if lat < 32.5 or lat > 39.5 or lng < 123.5 or lng > 132.5:
        return None
    return lat, lng


def merge_geo_place_lists(
    kakao_rows: List[Dict[str, Any]],
    naver_rows: List[Dict[str, Any]],
    *,
    dedupe_m: float = 48.0,
) -> List[Dict[str, Any]]:
    """동일 시설 중복 제거: 카카오 우선, 네이버는 근접 중복이 없을 때만 추가."""
    out: List[Dict[str, Any]] = []
    for r in kakao_rows or []:
        try:
            la = float(r.get("lat"))
            ln = float(r.get("lng"))
        except (TypeError, ValueError):
            continue
        x = dict(r)
        x["poi_source"] = x.get("poi_source") or "kakao"
        out.append(x)
    for r in naver_rows or []:
        try:
            la = float(r.get("lat"))
            ln = float(r.get("lng"))
        except (TypeError, ValueError):
            continue
        if any(haversine_m(la, ln, float(o.get("lat")), float(o.get("lng"))) < dedupe_m for o in out):
            continue
        x = dict(r)
        x["poi_source"] = "naver"
        out.append(x)
    return out


def naver_local_search(
    *,
    client_id: str,
    client_secret: str,
    query: str,
    lat: float,
    lng: float,
    radius_m: int,
    timeout: float = 4.0,
) -> List[Dict[str, Any]]:
    """
    네이버 지역 검색 API. 검색어 기반이라 원점 주변으로 거리 필터(mapx/mapy 변환 후).
    일일 한도·정책은 네이버 개발자센터 기준.
    """
    if not (client_id or "").strip() or not (client_secret or "").strip():
        return []
    ck = f"naver|{_round_key(lat, lng, radius_m, query)}"
    hit = _cache_get(ck)
    if hit is not None:
        return hit
    headers = {
        "X-Naver-Client-Id": client_id.strip(),
        "X-Naver-Client-Secret": client_secret.strip(),
    }
    url = "https://openapi.naver.com/v1/search/local.json"
    acc: List[Dict[str, Any]] = []
    for start in (1, 6):
        params = {"query": query, "display": 5, "start": start, "sort": "random"}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=timeout)
        except Exception as e:
            logging.warning("naver local %s: %s", query, e)
            break
        if res.status_code == 401:
            logging.warning("NAVER local 401 — Client ID/Secret 확인")
            break
        if res.status_code >= 400:
            break
        try:
            data = res.json()
        except Exception:
            break
        items = (data or {}).get("items") or []
        if not items:
            break
        for it in items:
            ll = _naver_mapxy_to_latlng(it.get("mapx"), it.get("mapy"))
            if not ll:
                continue
            plat, plng = ll
            if haversine_m(lat, lng, plat, plng) > float(radius_m):
                continue
            title = _strip_html_brief(str(it.get("title") or ""))
            if not title:
                continue
            acc.append({
                "id": f"n:{plat:.5f},{plng:.5f}:{title[:24]}",
                "place_name": title,
                "category_name": _strip_html_brief(str(it.get("category") or "")),
                "lat": plat,
                "lng": plng,
                "distance_m": int(haversine_m(lat, lng, plat, plng)),
                "poi_source": "naver",
            })
        if len(items) < 5:
            break
    _cache_set(ck, acc)
    return acc


def dual_source_places_for_query(
    *,
    kakao_key: str,
    naver_client_id: str,
    naver_client_secret: str,
    lat: float,
    lng: float,
    radius_m: int,
    query: str,
) -> List[Dict[str, Any]]:
    k_rows: List[Dict[str, Any]] = []
    if (kakao_key or "").strip():
        k_rows = kakao_keyword_search(
            kakao_key=kakao_key,
            lat=lat,
            lng=lng,
            radius_m=radius_m,
            query=query,
        )
    n_rows = naver_local_search(
        client_id=naver_client_id,
        client_secret=naver_client_secret,
        query=query,
        lat=lat,
        lng=lng,
        radius_m=radius_m,
    )
    return merge_geo_place_lists(k_rows, n_rows)


# 은행·대형마트 (키워드 수 최소화 — 호출량·한도)
RETAIL_BANK_QUERIES = ["KB국민은행", "신한은행", "우리은행", "하나은행"]
RETAIL_MART_QUERIES = ["이마트", "홈플러스", "롯데마트", "코스트코"]


def collect_retail_finance_pois(
    *,
    kakao_key: str,
    naver_client_id: str = "",
    naver_client_secret: str = "",
    lat: float,
    lng: float,
    radius_m: int = 2000,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """1금융·대형마트 후보 POI (카카오+네이버 병합, 좌표 기준 중복 제거)."""
    meta: Dict[str, Any] = {"errors": [], "naver_used": bool((naver_client_id or "").strip() and (naver_client_secret or "").strip())}
    bank_map: Dict[str, Dict[str, Any]] = {}
    mart_map: Dict[str, Dict[str, Any]] = {}

    def _ingest(target: Dict[str, Dict[str, Any]], rows: List[Dict[str, Any]], kind: str) -> None:
        for r in rows or []:
            try:
                la = float(r.get("lat"))
                ln = float(r.get("lng"))
            except (TypeError, ValueError):
                continue
            key = f"{round(la, 5)},{round(ln, 5)}"
            if key in target:
                continue
            x = dict(r)
            x["retail_kind"] = kind
            target[key] = x

    def _run_queries(queries: List[str], kind: str, dest: Dict[str, Dict[str, Any]]) -> None:
        for q in queries:
            try:
                rows = dual_source_places_for_query(
                    kakao_key=kakao_key,
                    naver_client_id=naver_client_id,
                    naver_client_secret=naver_client_secret,
                    lat=lat,
                    lng=lng,
                    radius_m=radius_m,
                    query=q,
                )
                _ingest(dest, rows, kind)
            except Exception as e:
                meta["errors"].append(f"{kind}:{q}:{e}")

    _run_queries(RETAIL_BANK_QUERIES, "bank", bank_map)
    _run_queries(RETAIL_MART_QUERIES, "mart", mart_map)
    return list(bank_map.values()), list(mart_map.values()), meta


def _count_retail_within(
    lat: float,
    lng: float,
    radius_m: float,
    places: List[Dict[str, Any]],
) -> int:
    n = 0
    for p in places or []:
        try:
            la = float(p.get("lat"))
            ln = float(p.get("lng"))
        except (TypeError, ValueError):
            continue
        if haversine_m(lat, lng, la, ln) <= radius_m:
            n += 1
    return n


def kakao_keyword_search(
    *,
    kakao_key: str,
    lat: float,
    lng: float,
    radius_m: int,
    query: str,
    timeout: float = 4.0,
) -> List[Dict[str, Any]]:
    if not (kakao_key or "").strip():
        return []
    ck = _round_key(lat, lng, radius_m, query)
    hit = _cache_get(ck)
    if hit is not None:
        return hit
    headers = {"Authorization": f"KakaoAK {kakao_key.strip()}"}
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    params = {
        "query": query,
        "x": str(lng),
        "y": str(lat),
        "radius": str(min(max(int(radius_m), 1), 20000)),
        "sort": "distance",
        "size": 15,
        "page": 1,
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=timeout)
    except Exception as e:
        logging.warning("kakao keyword %s: %s", query, e)
        return []
    if res.status_code >= 400:
        logging.warning("kakao keyword %s HTTP %s", query, res.status_code)
        return []
    try:
        data = res.json()
    except Exception:
        return []
    docs = (data or {}).get("documents") or []
    out: List[Dict[str, Any]] = []
    for d in docs:
        pid = str(d.get("id") or "")
        try:
            plat = float(d.get("y") or 0)
            plng = float(d.get("x") or 0)
        except (TypeError, ValueError):
            continue
        dist_m = d.get("distance")
        try:
            dist_m = int(dist_m) if dist_m is not None else None
        except (TypeError, ValueError):
            dist_m = None
        out.append({
            "id": pid or f"{plat:.6f},{plng:.6f}",
            "place_name": str(d.get("place_name") or ""),
            "category_name": str(d.get("category_name") or ""),
            "road_address_name": str(d.get("road_address_name") or ""),
            "distance_m": dist_m,
            "lat": plat,
            "lng": plng,
            "brand_query": query,
        })
    _cache_set(ck, out)
    return out


DEFAULT_ANCHOR_BRANDS: List[Tuple[str, str]] = [
    ("스타벅스", "스타벅스"),
    ("파리바게뜨", "파리바게뜨"),
    ("올리브영", "올리브영"),
    ("다이소", "다이소"),
    ("메가커피", "메가커피"),
    ("이디야", "이디야"),
    ("롯데리아", "롯데리아"),
]


_CORNER_DIAGONAL = frozenset({"북동", "남동", "남서", "북서"})


def _commercial_vitality_01(
    anchor_100: int,
    anchor_300: int,
    bank_150: int,
    mart_300: int,
) -> float:
    """후보 좌표 기준 상업 신호 0~1. 행정동 유동 프록시를 좌표에 맞게 내리는 데 사용."""
    v = 0.0
    if anchor_100 >= 2:
        v += 0.56
    elif anchor_100 >= 1:
        v += 0.34
    if anchor_300 >= 10:
        v += 0.3
    elif anchor_300 >= 6:
        v += 0.2
    elif anchor_300 >= 3:
        v += 0.1
    elif anchor_300 >= 1:
        v += 0.05
    if bank_150 >= 1:
        v += 0.2
    if mart_300 >= 1:
        v += 0.16
    return min(1.0, v)


def score_proptech_clinic_site(
    *,
    lat: float,
    lng: float,
    region_center_lat: Optional[float],
    region_center_lng: Optional[float],
    offset_dir: str,
    offset_m: float,
    anchor_poi_count_100m: int,
    medical_facility_count_100m: int,
    anchor_poi_count_300m: int = 0,
    master_activity_index: Optional[float],
    young_ratio: Optional[float],
    master_total_pop: Optional[float],
    bus_stop_count: Optional[float],
    has_building_parking: Optional[bool] = None,
    nearby_public_parking_100m: Optional[bool] = None,
    land_use_hard_exclude: bool = False,
    land_use_mult: float = 1.0,
    land_use_reasons: Optional[List[str]] = None,
    bank_poi_count_150m: int = 0,
    mart_poi_count_300m: int = 0,
) -> Dict[str, Any]:
    """
    의원·상가 입지 100점 만점 (6축) + S/A/B/C.
    상업 가시성 우선: 토지용도 하드제외·이면 프록시는 land_use_* 로 주입.
    """
    notes: List[str] = []
    lur = list(land_use_reasons or [])
    if lur:
        notes.extend([f"토지·동선: {x}" for x in lur[:5]])

    if land_use_hard_exclude:
        notes.append("하드제외: 학교·단지·공원 부지 인접 등(카카오 프록시) → 0점 처리.")
        return {
            "score": 0.0,
            "grade": "C",
            "grade_label_ko": "제외",
            "components": {
                "foot_traffic": 0.0,
                "visibility_access": 0.0,
                "residential_proximity": 0.0,
                "anchor_franchises": 0.0,
                "medical_synergy": 0.0,
                "parking_infrastructure": 0.0,
            },
            "scoring_meta": {
                "method": "proptech_clinic_v4_street_scaled",
                "notes": notes,
                "hard_excluded": True,
                "commercial_vitality_01": 0.0,
            },
        }

    # 1) 유동인구 지수 (max 30) — 마스터 활력·인구로 분위 프록시 (실제 50m 격자 유동인구 아님)
    ai = float(master_activity_index) if master_activity_index is not None else None
    pop = float(master_total_pop) if master_total_pop is not None and float(master_total_pop) > 0 else None
    tier_ft = 0
    if ai is not None:
        if ai >= 22:
            tier_ft = max(tier_ft, 3)
        elif ai >= 12:
            tier_ft = max(tier_ft, 2)
        elif ai >= 4:
            tier_ft = max(tier_ft, 1)
    if pop is not None:
        if pop >= 40000:
            tier_ft = max(tier_ft, 3)
        elif pop >= 18000:
            tier_ft = max(tier_ft, 2)
        elif pop >= 6000:
            tier_ft = max(tier_ft, 1)
    if tier_ft == 0 and ai is None and pop is None:
        foot = 15.0
        notes.append("유동인구: 행정동 마스터 부재 → 평균 수준(15점) 가정.")
    else:
        # 은행·마트 가점 반영을 위해 상한 소폭 조정(축 합계 100 유지)
        foot_map = {0: 5.0, 1: 13.0, 2: 22.0, 3: 27.0}
        foot = foot_map.get(tier_ft, 15.0)
        notes.append("유동인구: 실제 격자 유동인구가 아니라 마스터 활력·총인구 기반 분위 프록시입니다.")
    if young_ratio is not None and float(young_ratio) >= 0.36:
        foot = min(28.0, foot + 2.0)

    # 동일 동 안에서도 후보별로 상업전면이 다름 → 행정동 유동을 좌표 상업활력으로 스케일(스크린샷 이슈)
    na1_ft = int(anchor_poi_count_100m)
    na3_ft = int(anchor_poi_count_300m)
    nb_ft = int(bank_poi_count_150m)
    nm_ft = int(mart_poi_count_300m)
    vit = _commercial_vitality_01(na1_ft, na3_ft, nb_ft, nm_ft)
    if vit < 0.1:
        foot = min(foot, 6.0)
        notes.append(
            "유동 보정: 해당 좌표 100~300m 내 앵커·은행·마트가 거의 없어, 행정동 유동 프록시를 대폭 축소(골목·단지내부)."
        )
    elif vit < 0.24:
        foot = min(foot, 10.0)
        notes.append("유동 보정: 상업 POI 신호 미약 → 행정동 유동 프록시 상한 축소.")
    else:
        foot = foot * (0.22 + 0.78 * vit)
        if vit < 0.5:
            notes.append("유동 보정: 행정동 프록시 × 좌표별 상업활력(앵커·은행·마트) 가중.")

    # 2) 가시성·접근 (max 20) — 코너=격자 오프셋 방향 프록시, 횡단보도=버스정류장 밀도 프록시
    corner = 0.0
    od = (offset_dir or "중심").strip()
    om = float(offset_m or 0)
    if od in _CORNER_DIAGONAL and om > 0:
        corner = 10.0
    elif od != "중심" and om > 0:
        corner = 5.0
    cross = 0.0
    bsc = float(bus_stop_count) if bus_stop_count is not None else None
    if bsc is not None:
        if bsc >= 14:
            cross = 10.0
        elif bsc >= 5:
            cross = 5.0
        notes.append("횡단보도: POI 미연동 → 해당 행정동 버스정류장 수로 보행·접근성 프록시.")
    else:
        notes.append("횡단보도: 버스정류장 수 없음 → 0점(데이터 공백).")
    # 동일 동·동일 버스 정류장 수인데 골목은 대로가 아님 → 무앵커일 때 버스 프록시 축소
    if na1_ft == 0:
        cross *= 0.22 + 0.14 * min(1.0, na3_ft / 8.0)
        if cross > 0 and cross < 10.0:
            notes.append("가시성 보정: 100m 무앵커 → 행정동 버스정류장 프록시 축소(이면도로).")
    visibility = min(20.0, corner + cross)

    # 3) 배후 주거 (max 20) — 행정동 중심 프록시이나, 무앵커 전면이면 과대가점 방지(상업입지 우선)
    residential = 0.0
    na100 = int(anchor_poi_count_100m)
    if (
        region_center_lat is not None
        and region_center_lng is not None
        and abs(float(region_center_lat)) <= 90
        and abs(float(region_center_lng)) <= 180
    ):
        d_admin = haversine_m(lat, lng, float(region_center_lat), float(region_center_lng))
        if d_admin < 300:
            residential = 20.0 * max(0.0, 1.0 - d_admin / 300.0)
        notes.append("배후 주거: 행정동 중심 거리 프록시(실제 단지정문·지적 마스킹은 추후 GIS).")
    else:
        residential = 8.0
        notes.append("배후 주거: 행정동 중심좌표 없음 → 중립 프록시(8점).")
    # 100m 내 상업앵커가 없으면 '배후주거'만으로는 클리닉 전면 입지로 보지 않음
    res_anchor_scale = 0.22 + 0.78 * min(1.0, na100 / 2.0)
    residential = residential * res_anchor_scale

    # 4) 앵커·은행·대형마트 (합 max 25) — A급 프랜차이즈 100m + 150m 은행 + 300m 마트 (카카오·네이버 병합 집계)
    na = int(anchor_poi_count_100m)
    if na >= 3:
        fr = 15.0
    elif na >= 1:
        fr = 10.0
    else:
        fr = 0.0
    nb = int(bank_poi_count_150m)
    nm = int(mart_poi_count_300m)
    bank_pts = 7.0 if nb >= 2 else (5.0 if nb >= 1 else 0.0)
    mart_pts = 7.0 if nm >= 2 else (5.0 if nm >= 1 else 0.0)
    anchor_pts = min(25.0, fr + bank_pts + mart_pts)
    if bank_pts or mart_pts:
        notes.append(
            f"집객 인프라: 150m 내 은행류 {nb}곳(+{bank_pts:.0f}), 300m 내 대형마트·복합류 {nm}곳(+{mart_pts:.0f}) — 카카오·네이버 중복 제거 병합."
        )

    # 5) 메디컬 시너지 (max 10) — 동일 과목 HIRA 목록 100m (타 진료과·약국은 미포함)
    n_med = int(medical_facility_count_100m)
    if n_med >= 5:
        med = 10.0
    elif n_med >= 2:
        med = 7.0
    else:
        med = 3.0
    notes.append("메디컬 시너지: 심평원 동일(유사) 과목만 집계 — 타 과목·약국은 추후 POI로 확장 가능.")

    # 6) 주차 (max 5)
    park = 0.0
    if has_building_parking is True:
        park += 3.0
    elif has_building_parking is False:
        pass
    else:
        notes.append("건물 주차: 건축물대장/폴리곤 미연동 → 가점 미반영.")
    if nearby_public_parking_100m is True:
        park += 2.0
    elif nearby_public_parking_100m is False:
        pass
    else:
        notes.append("공영·민영 주차: 반경 100m 주차장 POI 미연동 → 가점 미반영.")
    park = min(5.0, park)

    total = foot + visibility + residential + anchor_pts + med + park
    total = max(0.0, min(100.0, total))
    lm = float(land_use_mult)
    if lm < 0.1:
        lm = 0.1
    if lm > 1.0:
        lm = 1.0
    if lm < 0.999:
        total = max(0.0, min(100.0, total * lm))
        notes.append(f"토지·동선 종합 감점 배율 ×{lm:.2f} (도로폭 미연동·카카오 프록시).")

    if total >= 90:
        grade, label = "S", "최우수"
    elif total >= 75:
        grade, label = "A", "우수"
    elif total >= 60:
        grade, label = "B", "보통"
    else:
        grade, label = "C", "주의"

    return {
        "score": round(total, 1),
        "grade": grade,
        "grade_label_ko": label,
        "components": {
            "foot_traffic": round(foot, 1),
            "visibility_access": round(visibility, 1),
            "residential_proximity": round(residential, 1),
            "anchor_franchises": round(anchor_pts, 1),
            "medical_synergy": round(med, 1),
            "parking_infrastructure": round(park, 1),
        },
        "scoring_meta": {
            "method": "proptech_clinic_v4_street_scaled",
            "notes": notes,
            "anchor_poi_count_300m": int(anchor_poi_count_300m),
            "bank_poi_count_150m": int(bank_poi_count_150m),
            "mart_poi_count_300m": int(mart_poi_count_300m),
            "commercial_vitality_01": round(vit, 3),
        },
    }


def collect_anchor_pois(
    *,
    kakao_key: str,
    lat: float,
    lng: float,
    radius_m: int,
    brands: Optional[List[Tuple[str, str]]] = None,
    naver_client_id: str = "",
    naver_client_secret: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    pairs = brands or DEFAULT_ANCHOR_BRANDS
    merged: Dict[str, Dict[str, Any]] = {}
    meta_errors: List[str] = []
    naver_on = bool((naver_client_id or "").strip() and (naver_client_secret or "").strip())
    if not (kakao_key or "").strip() and not naver_on:
        return [], {"kakao": "KAKAO·NAVER 미설정 — 앵커 POI를 조회하지 않습니다."}

    def _fetch_brand(label: str, q: str) -> Tuple[str, str, List[Dict[str, Any]]]:
        rows = dual_source_places_for_query(
            kakao_key=kakao_key or "",
            naver_client_id=naver_client_id,
            naver_client_secret=naver_client_secret,
            lat=lat,
            lng=lng,
            radius_m=radius_m,
            query=q,
        )
        return label, q, rows

    futs: Dict[Any, Tuple[str, str]] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(pairs))) as ex:
        for label, q in pairs:
            fut = ex.submit(_fetch_brand, label, q)
            futs[fut] = (label, q)
        for fut in as_completed(futs):
            label, q = futs[fut]
            try:
                _, _, rows = fut.result()
            except Exception as e:
                meta_errors.append(f"{q}:{e}")
                continue
            for row in rows:
                key = (str(row.get("id") or "").strip()
                       or f"{float(row.get('lat') or 0):.5f},{float(row.get('lng') or 0):.5f}")
                if key in merged:
                    continue
                row["brand_label"] = label
                merged[key] = row
    return list(merged.values()), {
        "errors": meta_errors,
        "naver_merged": naver_on,
        "kakao_used": bool((kakao_key or "").strip()),
    }


def build_micro_site_payload(
    *,
    lat: float,
    lng: float,
    radius_m: int,
    dept: str,
    competitors: List[Dict[str, Any]],
    kakao_key: str,
    master_ctx: Optional[Dict[str, Any]],
    naver_client_id: str = "",
    naver_client_secret: str = "",
) -> Dict[str, Any]:
    anchor_list, kmeta = collect_anchor_pois(
        kakao_key=kakao_key,
        lat=lat,
        lng=lng,
        radius_m=radius_m,
        naver_client_id=naver_client_id,
        naver_client_secret=naver_client_secret,
    )
    n_comp = len(competitors or [])
    n_a_100 = _count_anchors_within(lat, lng, 100.0, anchor_list)
    n_a_300 = _count_anchors_within(lat, lng, 300.0, anchor_list)
    n_c_100 = _count_hospitals_within(lat, lng, 100.0, competitors or [])
    bank_pl, mart_pl, rf_meta = collect_retail_finance_pois(
        kakao_key=kakao_key,
        naver_client_id=naver_client_id,
        naver_client_secret=naver_client_secret,
        lat=lat,
        lng=lng,
        radius_m=min(2000, max(radius_m * 3, 900)),
    )
    n_bank_150 = _count_retail_within(lat, lng, 150.0, bank_pl)
    n_mart_300 = _count_retail_within(lat, lng, 300.0, mart_pl)
    act = None
    young = None
    row: Dict[str, Any] = {}
    if master_ctx:
        act = master_ctx.get("activity_index")
        young = master_ctx.get("young_ratio")
        row = master_ctx.get("row") or {}
    try:
        rclat = float(row.get("center_lat")) if row.get("center_lat") is not None else None
        rclng = float(row.get("center_lng")) if row.get("center_lng") is not None else None
    except (TypeError, ValueError):
        rclat, rclng = None, None
    try:
        mpop = float(row.get("총인구 (명)")) if row.get("총인구 (명)") is not None else None
    except (TypeError, ValueError):
        mpop = None
    if mpop is None:
        try:
            mpop = float(row.get("총인구(명)")) if row.get("총인구(명)") is not None else None
        except (TypeError, ValueError):
            mpop = None
    try:
        bsc = float(row.get("bus_stop_count")) if row.get("bus_stop_count") is not None else None
    except (TypeError, ValueError):
        bsc = None
    scoring = score_proptech_clinic_site(
        lat=lat,
        lng=lng,
        region_center_lat=rclat,
        region_center_lng=rclng,
        offset_dir="중심",
        offset_m=0.0,
        anchor_poi_count_100m=n_a_100,
        medical_facility_count_100m=n_c_100,
        anchor_poi_count_300m=n_a_300,
        master_activity_index=act,
        young_ratio=young,
        master_total_pop=mpop,
        bus_stop_count=bsc,
        bank_poi_count_150m=n_bank_150,
        mart_poi_count_300m=n_mart_300,
    )
    comp_out: List[Dict[str, Any]] = []
    for h in (competitors or [])[:40]:
        comp_out.append({
            "display_name": h.get("display_name") or h.get("name"),
            "lat": h.get("lat"),
            "lng": h.get("lng"),
        })
    region_name = master_ctx.get("region_name") if master_ctx else None
    dist_km = master_ctx.get("distance_km") if master_ctx else None
    narrative = (
        f"반경 약 {radius_m}m·{dept} 기준 미시 입지 참고입니다. "
        f"앵커 프랜차이즈(근접) {len(anchor_list)}곳, 동일 과목 경쟁 {n_comp}곳."
    )
    if not (kakao_key or "").strip():
        narrative += " 카카오 REST 키가 없어 일부 POI가 제외될 수 있습니다."
    if not ((naver_client_id or "").strip() and (naver_client_secret or "").strip()):
        narrative += " 네이버 Client ID/Secret 미설정 시 카카오 결과만 병합합니다."
    return {
        "status": "success",
        "lat": lat,
        "lng": lng,
        "radius_m": radius_m,
        "department": dept,
        "region_proxy": {"name": region_name, "distance_km": dist_km},
        "macro_proxy": {
            "activity_index": act,
            "young_ratio": young,
            "note": "가장 가까운 행정동 마스터 CSV 요약입니다. 유동인구 정밀도는 상권정보 API 연동 시 강화할 수 있습니다.",
        },
        "anchors": {
            "brands_searched": [x[0] for x in DEFAULT_ANCHOR_BRANDS],
            "places": anchor_list[:60],
            "meta": kmeta,
        },
        "competitors": comp_out,
        "competitor_count": n_comp,
        "anchor_poi_count_100m": n_a_100,
        "medical_facility_count_100m": n_c_100,
        "bank_poi_count_150m": n_bank_150,
        "mart_poi_count_300m": n_mart_300,
        "retail_finance_meta": rf_meta,
        "scoring": scoring,
        "narrative": narrative,
        "data_layers": {
            "crosswalks": {"status": "planned", "message": "2단계: 횡단보도 공간데이터 연동 예정"},
            "parking": {"status": "planned", "message": "2단계: 주차장 공공 API·지자체 데이터 연동 예정"},
        },
        "disclaimer": "참고용 휴리스틱 점수입니다. 실제 개원·임대는 현장 확인 및 전문 자문이 필요합니다.",
    }
