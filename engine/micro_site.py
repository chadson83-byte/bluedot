# -*- coding: utf-8 -*-
"""
미시 입지: 6축 프롭테크 스코어(유동·가시성·배후주거·앵커·메디컬·주차) + 카카오 앵커·심평원·마스터 CSV.
격자 유동인구·횡단보도·아파트 정문·주차장 등은 프록시/미연동 구간은 scoring_meta.notes 에 명시.
"""
from __future__ import annotations

import logging
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
) -> List[Dict[str, Any]]:
    """한 권역(1차 노드) 내 9개 후보 좌표에 대해 앵커·경쟁·마스터 기반 미시 점수(카카오 재호출 없음)."""
    out: List[Dict[str, Any]] = []
    for la, ln, dist_m, dir_label in candidate_offsets_9(center_lat, center_lng, offset_m=offset_m):
        n_a = _count_anchors_within(la, ln, float(eval_radius_m), anchors)
        n_c = _count_hospitals_within(la, ln, float(eval_radius_m), hospitals)
        n_a_100 = _count_anchors_within(la, ln, 100.0, anchors)
        n_c_100 = _count_hospitals_within(la, ln, 100.0, hospitals)
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
            medical_facility_count_100m=n_c_100,
            master_activity_index=act,
            young_ratio=young,
            master_total_pop=mpop,
            bus_stop_count=bsc,
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
            "medical_facility_count_100m": n_c_100,
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


def dedupe_pick_top(
    candidates: List[Dict[str, Any]],
    *,
    top_k: int = 5,
    min_sep_m: float = 55.0,
) -> List[Dict[str, Any]]:
    ranked = sorted(candidates, key=lambda c: float((c.get("scoring") or {}).get("score") or 0), reverse=True)
    picked: List[Dict[str, Any]] = []
    for c in ranked:
        la = float(c["lat"])
        ln = float(c["lng"])
        if any(haversine_m(la, ln, float(p["lat"]), float(p["lng"])) < min_sep_m for p in picked):
            continue
        row = dict(c)
        row["stage2_rank"] = len(picked) + 1
        picked.append(row)
        if len(picked) >= top_k:
            break
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
    master_activity_index: Optional[float],
    young_ratio: Optional[float],
    master_total_pop: Optional[float],
    bus_stop_count: Optional[float],
    has_building_parking: Optional[bool] = None,
    nearby_public_parking_100m: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    의원·상가 입지 100점 만점 (6축) + S/A/B/C.
    데이터 공백 구간은 메타 notes에 명시한다.
    """
    notes: List[str] = []

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
        foot_map = {0: 5.0, 1: 15.0, 2: 25.0, 3: 30.0}
        foot = foot_map.get(tier_ft, 15.0)
        notes.append("유동인구: 실제 격자 유동인구가 아니라 마스터 활력·총인구 기반 분위 프록시입니다.")
    if young_ratio is not None and float(young_ratio) >= 0.36:
        foot = min(30.0, foot + 2.0)

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
    visibility = min(20.0, corner + cross)

    # 3) 배후 주거 (max 20) — 행정동 중심까지 거리 감쇠 (아파트 정문 데이터 없음)
    residential = 0.0
    if (
        region_center_lat is not None
        and region_center_lng is not None
        and abs(float(region_center_lat)) <= 90
        and abs(float(region_center_lng)) <= 180
    ):
        d_admin = haversine_m(lat, lng, float(region_center_lat), float(region_center_lng))
        if d_admin < 300:
            residential = 20.0 * max(0.0, 1.0 - d_admin / 300.0)
        notes.append("배후 주거: 아파트 정문 좌표 없음 → 인접 행정동 중심까지 거리 감쇠 프록시.")
    else:
        residential = 8.0
        notes.append("배후 주거: 행정동 중심좌표 없음 → 중립 프록시(8점).")

    # 4) 앵커 (max 15) — 반경 100m
    na = int(anchor_poi_count_100m)
    if na >= 3:
        anchor_pts = 15.0
    elif na >= 1:
        anchor_pts = 10.0
    else:
        anchor_pts = 0.0

    # 5) 메디컬 시너지 (max 10) — 동일 과목 HIRA 목록 100m (타 진료과·약국은 미포함)
    nm = int(medical_facility_count_100m)
    if nm >= 5:
        med = 10.0
    elif nm >= 2:
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
            "method": "proptech_clinic_v1",
            "notes": notes,
        },
    }


def collect_anchor_pois(
    *,
    kakao_key: str,
    lat: float,
    lng: float,
    radius_m: int,
    brands: Optional[List[Tuple[str, str]]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    pairs = brands or DEFAULT_ANCHOR_BRANDS
    merged: Dict[str, Dict[str, Any]] = {}
    meta_errors: List[str] = []
    if not (kakao_key or "").strip():
        return [], {"kakao": "KAKAO_REST_KEY 미설정 — 앵커 POI를 조회하지 않습니다."}
    futs: Dict[Any, Tuple[str, str]] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(pairs))) as ex:
        for label, q in pairs:
            fut = ex.submit(
                kakao_keyword_search,
                kakao_key=kakao_key,
                lat=lat,
                lng=lng,
                radius_m=radius_m,
                query=q,
            )
            futs[fut] = (label, q)
        for fut in as_completed(futs):
            label, q = futs[fut]
            try:
                rows = fut.result()
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
    return list(merged.values()), {"errors": meta_errors}


def build_micro_site_payload(
    *,
    lat: float,
    lng: float,
    radius_m: int,
    dept: str,
    competitors: List[Dict[str, Any]],
    kakao_key: str,
    master_ctx: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    anchor_list, kmeta = collect_anchor_pois(kakao_key=kakao_key, lat=lat, lng=lng, radius_m=radius_m)
    n_comp = len(competitors or [])
    n_a_100 = _count_anchors_within(lat, lng, 100.0, anchor_list)
    n_c_100 = _count_hospitals_within(lat, lng, 100.0, competitors or [])
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
        master_activity_index=act,
        young_ratio=young,
        master_total_pop=mpop,
        bus_stop_count=bsc,
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
        narrative += " 카카오 REST 키가 없어 프랜차이즈 POI는 제외되었습니다."
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
        "scoring": scoring,
        "narrative": narrative,
        "data_layers": {
            "crosswalks": {"status": "planned", "message": "2단계: 횡단보도 공간데이터 연동 예정"},
            "parking": {"status": "planned", "message": "2단계: 주차장 공공 API·지자체 데이터 연동 예정"},
        },
        "disclaimer": "참고용 휴리스틱 점수입니다. 실제 개원·임대는 현장 확인 및 전문 자문이 필요합니다.",
    }
