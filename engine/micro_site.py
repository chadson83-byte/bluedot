# -*- coding: utf-8 -*-
"""
미시 입지(MVP): 클릭 지점 반경 내 카카오 키워드 POI + 경쟁(심평원) + 마스터 CSV 거시 프록시.
횡단보도·주차장은 data_layers 에 플레이스홀더만 반환(2단계).
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
        ctx = None
        if df_master is not None and hasattr(df_master, "empty") and not df_master.empty:
            try:
                ctx = resolve_master_ctx(df_master, la, ln, radius_km=3.0)
            except Exception:
                ctx = None
        act = ctx.get("activity_index") if ctx else None
        young = ctx.get("young_ratio") if ctx else None
        sc = score_micro_site(
            competitor_count=n_c,
            anchor_poi_count=n_a,
            master_activity_index=act,
            young_ratio=young,
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
    """2단계 후보별 선정 이유(휴리스틱 설명). 동일 건물·근접 경쟁 다수도 점수에 이미 반영됨."""
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
    ap = float(comp.get("anchor_pois") or 0)
    tr = float(comp.get("transit_commercial_proxy") or 0)
    yb = float(comp.get("young_cohort_proxy") or 0)
    pen = float(comp.get("competition_penalty") or 0)
    if offset_dir == "중심" or not offset_m or float(offset_m) <= 0:
        loc = "권역 중심 좌표"
    else:
        loc = f"{offset_dir} 방향 약 {int(offset_m)}m 오프셋 지점"
    head = (
        f"1단계 {parent_rank}위 권역「{parent_name}」을 기준으로 {loc}을 평가했습니다. "
        f"미시 반경 내 앵커 프랜차이즈 근접 {n_a}곳, 동일 과목 경쟁 추정 {n_c}곳을 반영했습니다. "
        "경쟁기관이 인근·동일 건물에 있어도 감점만 반영되며 후보에서 제외하지 않습니다."
    )
    reasons: List[str] = []
    if ap >= 12:
        reasons.append("핵심 상권·앵커 신호가 강해 가산 비중이 큽니다.")
    elif ap >= 4:
        reasons.append("주변 앵커 밀도가 무난해 접근성 가산을 받았습니다.")
    if tr >= 5:
        reasons.append("행정동 단위 거시 상권 활력(프록시)이 양호합니다.")
    elif tr >= 2:
        reasons.append("거시 상권 지표가 일정 수준 이상입니다.")
    if yb >= 2.5:
        reasons.append("타겟 연령층(영·청년) 비중 추정이 높아 가점 요인입니다.")
    if pen <= -18:
        reasons.append("경쟁 밀도 감점이 크지만 앵커·거시 요인이 이겨 종합 상위에 들었습니다.")
    elif n_c <= 2:
        reasons.append("즉시 인지되는 동일 과목 경쟁 밀도는 낮은 편입니다.")
    mid = " ".join(reasons) if reasons else "앵커·경쟁·거시 프록시의 균형으로 전체 후보 대비 상위 점수입니다."
    tail = f" (미시 종합 {score}점 / 등급 {grade})"
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
    ("메가커피", "메가커피"),
    ("이디야", "이디야"),
    ("롯데리아", "롯데리아"),
]


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


def score_micro_site(
    *,
    competitor_count: int,
    anchor_poi_count: int,
    master_activity_index: Optional[float],
    young_ratio: Optional[float],
) -> Dict[str, Any]:
    base = 52.0
    anchor_pts = min(22.0, float(anchor_poi_count) * 3.5)
    penalty = min(28.0, float(competitor_count) * 2.8)
    transit = 0.0
    if master_activity_index is not None:
        transit = min(12.0, max(0.0, float(master_activity_index)) / 4.0)
    youth_bonus = 0.0
    if young_ratio is not None:
        youth_bonus = min(6.0, max(0.0, float(young_ratio)) * 8.0)
    raw = base + anchor_pts + transit + youth_bonus - penalty
    score = max(0.0, min(100.0, raw))
    if score >= 82:
        grade, label = "S", "우수"
    elif score >= 68:
        grade, label = "A", "양호"
    elif score >= 52:
        grade, label = "B", "보통"
    else:
        grade, label = "C", "주의"
    return {
        "score": round(score, 1),
        "grade": grade,
        "grade_label_ko": label,
        "components": {
            "base": base,
            "anchor_pois": round(anchor_pts, 1),
            "competition_penalty": round(-penalty, 1),
            "transit_commercial_proxy": round(transit, 1),
            "young_cohort_proxy": round(youth_bonus, 1),
        },
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
) -> Dict[str, Any]:
    anchor_list, kmeta = collect_anchor_pois(kakao_key=kakao_key, lat=lat, lng=lng, radius_m=radius_m)
    n_comp = len(competitors or [])
    act = None
    young = None
    if master_ctx:
        act = master_ctx.get("activity_index")
        young = master_ctx.get("young_ratio")
    scoring = score_micro_site(
        competitor_count=n_comp,
        anchor_poi_count=len(anchor_list),
        master_activity_index=act,
        young_ratio=young,
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
        "scoring": scoring,
        "narrative": narrative,
        "data_layers": {
            "crosswalks": {"status": "planned", "message": "2단계: 횡단보도 공간데이터 연동 예정"},
            "parking": {"status": "planned", "message": "2단계: 주차장 공공 API·지자체 데이터 연동 예정"},
        },
        "disclaimer": "참고용 휴리스틱 점수입니다. 실제 개원·임대는 현장 확인 및 전문 자문이 필요합니다.",
    }
