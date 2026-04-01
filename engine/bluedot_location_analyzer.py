# -*- coding: utf-8 -*-
"""
BLUEDOT 입지 분석기 — 상업 가시성·동선 중심 100점 스코어 + 하드 제약.

[HIRA API 범위 — 요청·추가 연동 안내]
- getHospBasisList(병원기본목록): yadmNm, 좌표, drTotCnt, pnursCnt, estbDd, clCd, dgsbjtCd,
  일부 주차·요일진료 Y/N 등 **항목 버전별로 상이**할 수 있음. 실제 365/야간/입원/교정전문 **단정**에는 부족.
- **추가로 연동 권장** (공공데이터포털 / 심평원 OpenAPI 상세 스펙 확인 후):
  - 기관별 **진료과목·진료시간** 상세 (예: getDetailInfo 계열, 서비스명은 포털 최신 문서 기준).
  - **암호화요양기관번호(ykiho)** 기준 병합 → 건축물대장·도로망과 매칭 시 정확도 향상.
- 본 모듈은 HIRA가 아니라 **POI·폴리곤·도로폭 입력** 기반 오프라인 스코어링을 담당.

[GIS 마스킹 구조 — 아파트·학교 내부 배제]
1) 데이터 소스 예시
   - 아파트 단지: 브이월드/SHP 단지경계, 또는 지자체 GIS, 또는 OSM landuse=residential + name + 면적 필터.
   - 학교: 공시지적 학교용지 폴리곤, 또는 교육청 학교 위치 + 완충버퍼(예: 0m는 경계 내부만).
2) 표현
   - Shapely Polygon/MultiPolygon (WGS84: x=경도, y=위도).
   - 여러 동을 union 후 contains/prepare 검사로 O(n) 최적화 가능.
3) 판정
   - 후보 Point(lng, lat).within(단지폴리곤) → 하드 제외(score=0).
   - 경계선상 좌표는 경계 오차를 고려해 buffer(1~2m) 안쪽만 제외하거나, “내부” 정의를 명시.

[도로 폭 / 이면도로]
- 이상적: OSM highway=primary/secondary + width/lanes, 또는 국토부 도로중심선 속성(차로·폭).
- 본 클래스는 **road_frontage_m**(건물이 접한 최대 도로폭 추정, m)을 외부 파이프라인이 채워준다고 가정.
- 미제공 시: alley_penalty 미적용(unknown), scoring_meta에 명시.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

try:
    from shapely.geometry import Point, Polygon, MultiPolygon
    from shapely.ops import unary_union

    _SHAPELY = True
except ImportError:
    Point = Polygon = MultiPolygon = None  # type: ignore
    unary_union = None  # type: ignore
    _SHAPELY = False


# --- A급 앵커(브랜드 키워드 — 카카오 place_name 등과 부분일치) ---
_ANCHOR_A_KEYWORDS = (
    "스타벅스",
    "STARBUCKS",
    "파리바게뜨",
    "파리바게트",
    "올리브영",
    "OLIVE YOUNG",
    "다이소",
    "DAISO",
    "맥도날드",
    "McDonald",
    "버거킹",
    "Burger King",
    "롯데리아",
    "KFC",
    "이디야",
    "투썸",
    "빽다방",
    "메가커피",
    "컴포즈",
)

_BANK_KEYWORDS = (
    "KB국민",
    "국민은행",
    "신한은행",
    "신한",
    "우리은행",
    "하나은행",
    "NH농협",
    "농협은행",
    "IBK",
    "기업은행",
    "SC제일",
    "한국씨티",
    "씨티은행",
    "케이뱅크",
    "카카오뱅크",
    "토스뱅크",
    "수협은행",
    "대구은행",
    "부산은행",
    "경남은행",
    "광주은행",
    "전북은행",
    "제주은행",
)

_MART_COMPLEX_KEYWORDS = (
    "이마트",
    "홈플러스",
    "롯데마트",
    "코스트코",
    "Costco",
    "트레이더스",
    "현대백화점",
    "롯데백화점",
    "신세계",
    "스타필드",
    "AK플라자",
    "NC백화점",
    "갤러리아",
    "타임스퀘어",
    "IFC",
)


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").upper())


def _poi_name(p: Dict[str, Any]) -> str:
    return str(p.get("place_name") or p.get("name") or p.get("title") or "")


def _poi_latlng(p: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    try:
        if "y" in p and "x" in p:
            return float(p["y"]), float(p["x"])
        return float(p["lat"]), float(p["lng"])
    except (TypeError, ValueError, KeyError):
        return None


def _match_keywords(name: str, keywords: Sequence[str]) -> bool:
    n = _norm(name)
    for k in keywords:
        if _norm(k) in n or _norm(k) in _norm(name.replace(" ", "")):
            return True
    return False


def _count_pois_in_radius(
    lat: float,
    lng: float,
    radius_m: float,
    pois: Sequence[Dict[str, Any]],
    predicate,
) -> int:
    n = 0
    for p in pois or []:
        pair = _poi_latlng(p)
        if not pair:
            continue
        la, ln = pair
        if _haversine_m(lat, lng, la, ln) <= radius_m and predicate(p):
            n += 1
    return n


def _polygons_to_merged(polygons: Optional[Sequence[Any]]) -> Optional[Any]:
    if not _SHAPELY or not polygons:
        return None
    geoms = []
    for g in polygons:
        if g is None:
            continue
        if isinstance(g, Polygon):
            geoms.append(g)
        elif isinstance(g, MultiPolygon):
            geoms.extend(g.geoms)
    if not geoms:
        return None
    return unary_union(geoms) if len(geoms) > 1 else geoms[0]


def _point_inside_polygon(lat: float, lng: float, merged: Any) -> bool:
    if not _SHAPELY or merged is None:
        return False
    return bool(merged.contains(Point(lng, lat)))


@dataclass
class LocationAnalyzerInput:
    """분석 입력 — POI는 카카오 로컬 등 동일 스키마(place_name, y, x 또는 lat, lng) 가정."""

    lat: float
    lng: float
    dept: str = "내과"
    pois: List[Dict[str, Any]] = field(default_factory=list)
    hospitals: List[Dict[str, Any]] = field(default_factory=list)
    pharmacies: List[Dict[str, Any]] = field(default_factory=list)
    # GIS: WGS84 Polygon/MultiPolygon 리스트 (경도, 위도 순 꼭짓점)
    residential_polygons: Optional[List[Any]] = None
    school_polygons: Optional[List[Any]] = None
    # 건물 전면이 접한 도로 중 최대 추정 폭(m). None이면 도로 제약 unknown.
    road_frontage_m: Optional[float] = None
    # 교차로 코너 건물 여부 (외부 라우팅/OSM이 채움)
    is_corner_lot: Optional[bool] = None
    # 행정동·격자 유동 상위 퍼센타일 (0~100). None이면 보행 품질 구간 미산출(0점 처리 + notes).
    foot_traffic_percentile: Optional[float] = None
    # 대로변에 가깝다는 푯값(이면도로 가중 보정용). None이면 가중 미적용.
    on_main_artery: Optional[bool] = None
    same_building_same_dept: bool = False


class BluedotLocationAnalyzer:
    """
    상업 가시성·동선 중심 입지 스코어(100점) + 하드 제약.

    사용 흐름:
        analyzer = BluedotLocationAnalyzer()
        result = analyzer.analyze(LocationAnalyzerInput(...))
        # result는 JSON 직렬화 가능한 dict.
    """

    MAIN_ROAD_MIN_WIDTH_M = 8.0
    ALLEY_SCORE_MULT = 0.5

    def analyze(self, inp: LocationAnalyzerInput) -> Dict[str, Any]:
        lat, lng = float(inp.lat), float(inp.lng)
        breakdown: List[Dict[str, Any]] = []
        notes: List[str] = []

        hard_excluded = False
        hard_reasons: List[str] = []

        res_merge = _polygons_to_merged(inp.residential_polygons)
        sch_merge = _polygons_to_merged(inp.school_polygons)

        if res_merge is not None and _point_inside_polygon(lat, lng, res_merge):
            hard_excluded = True
            hard_reasons.append("아파트(주거) 단지 폴리곤 내부 좌표")
        if sch_merge is not None and _point_inside_polygon(lat, lng, sch_merge):
            hard_excluded = True
            hard_reasons.append("학교 부지 폴리곤 내부 좌표")

        if hard_excluded:
            breakdown.append(
                {
                    "id": "hard_constraints",
                    "label": "필수 필터(하드 제약)",
                    "score": 0.0,
                    "max": 0.0,
                    "detail": {"excluded": True, "reasons": hard_reasons},
                }
            )
            return self._finalize(
                0.0,
                breakdown,
                notes
                + [
                    "하드 제약으로 총점 0. 실제 서비스에서는 폴리곤 데이터 정밀도·좌표 오차를 로그로 남길 것.",
                ],
                inp,
                alley_mult_applied=None,
            )

        s1, d1 = self._score_anchors(lat, lng, inp.pois)
        breakdown.append(
            {
                "id": "anchor_facilities",
                "label": "절대적 집객 인프라 (앵커)",
                "score": round(s1, 2),
                "max": 35.0,
                "detail": d1,
            }
        )

        s2, d2 = self._score_visibility_flow(lat, lng, inp.pois, inp.is_corner_lot)
        breakdown.append(
            {
                "id": "traffic_flow_visibility",
                "label": "보행 동선 및 가시성",
                "score": round(s2, 2),
                "max": 30.0,
                "detail": d2,
            }
        )

        s3, d3 = self._score_foot_traffic_quality(
            inp.foot_traffic_percentile, inp.on_main_artery
        )
        breakdown.append(
            {
                "id": "foot_traffic_quality",
                "label": "유동인구 퀄리티(상업 도로)",
                "score": round(s3, 2),
                "max": 25.0,
                "detail": d3,
            }
        )

        s4, d4 = self._score_competition_synergy(
            lat,
            lng,
            inp.dept,
            inp.hospitals,
            inp.pharmacies,
            inp.same_building_same_dept,
        )
        breakdown.append(
            {
                "id": "competition_synergy",
                "label": "경쟁 및 시너지",
                "score": round(s4, 2),
                "max": 10.0,
                "detail": d4,
            }
        )

        subtotal = s1 + s2 + s3 + s4
        alley_mult_applied: Optional[float] = None

        if inp.road_frontage_m is None:
            notes.append(
                "도로폭(road_frontage_m) 미제공 — 왕복 2차선(약 8m) 기준·이면 50% 감점을 적용하지 않음. "
                "OSM/국토 도로망 파이프라인 연동 필요."
            )
        elif inp.road_frontage_m < self.MAIN_ROAD_MIN_WIDTH_M:
            subtotal *= self.ALLEY_SCORE_MULT
            alley_mult_applied = self.ALLEY_SCORE_MULT
            notes.append(
                f"접도로 추정 폭 {inp.road_frontage_m:.1f}m < {self.MAIN_ROAD_MIN_WIDTH_M}m → 총점에 {self.ALLEY_SCORE_MULT} 배(이면도로 감점)."
            )
        else:
            notes.append(
                f"접도로 추정 폭 {inp.road_frontage_m:.1f}m ≥ {self.MAIN_ROAD_MIN_WIDTH_M}m — 이면 감점 미적용."
            )

        total = max(0.0, min(100.0, round(subtotal, 2)))
        return self._finalize(total, breakdown, notes, inp, alley_mult_applied)

    def _finalize(
        self,
        total: float,
        breakdown: List[Dict[str, Any]],
        notes: List[str],
        inp: LocationAnalyzerInput,
        alley_mult_applied: Optional[float],
    ) -> Dict[str, Any]:
        out = {
            "ok": True,
            "version": "bluedot_location_analyzer_v1",
            "lat": inp.lat,
            "lng": inp.lng,
            "dept": inp.dept,
            "total_score": total,
            "breakdown": breakdown,
            "scoring_meta": {
                "notes": notes,
                "alley_penalty_multiplier_applied": alley_mult_applied,
                "shapely_available": _SHAPELY,
            },
        }
        return out

    def _score_anchors(
        self, lat: float, lng: float, pois: Sequence[Dict[str, Any]]
    ) -> Tuple[float, Dict[str, Any]]:
        score = 0.0
        detail: Dict[str, Any] = {}

        def is_a_anchor(p: Dict[str, Any]) -> bool:
            return _match_keywords(_poi_name(p), _ANCHOR_A_KEYWORDS)

        n100 = _count_pois_in_radius(lat, lng, 100.0, pois, is_a_anchor)
        if n100 >= 2:
            score += 15.0
        detail["a_grade_franchise_within_100m"] = {"count": n100, "points": 15.0 if n100 >= 2 else 0.0}

        def is_bank(p: Dict[str, Any]) -> bool:
            return _match_keywords(_poi_name(p), _BANK_KEYWORDS)

        n_bank = _count_pois_in_radius(lat, lng, 150.0, pois, is_bank)
        if n_bank >= 1:
            score += 10.0
        detail["bank_within_150m"] = {"count": n_bank, "points": 10.0 if n_bank >= 1 else 0.0}

        def is_mart(p: Dict[str, Any]) -> bool:
            return _match_keywords(_poi_name(p), _MART_COMPLEX_KEYWORDS)

        n_mart = _count_pois_in_radius(lat, lng, 300.0, pois, is_mart)
        if n_mart >= 1:
            score += 10.0
        detail["mart_or_mall_within_300m"] = {"count": n_mart, "points": 10.0 if n_mart >= 1 else 0.0}

        score = min(35.0, score)
        return score, detail

    def _score_visibility_flow(
        self,
        lat: float,
        lng: float,
        pois: Sequence[Dict[str, Any]],
        is_corner: Optional[bool],
    ) -> Tuple[float, Dict[str, Any]]:
        score = 0.0
        detail: Dict[str, Any] = {}

        def is_crosswalk(p: Dict[str, Any]) -> bool:
            name = _poi_name(p)
            cat = str(p.get("category_name") or p.get("category") or "")
            return "횡단보도" in name or "횡단보도" in cat

        n_cw = _count_pois_in_radius(lat, lng, 30.0, pois, is_crosswalk)
        if n_cw >= 1:
            score += 15.0
        detail["crosswalk_within_30m"] = {"count": n_cw, "points": 15.0 if n_cw >= 1 else 0.0}

        def is_transit(p: Dict[str, Any]) -> bool:
            name = _poi_name(p)
            cat = str(p.get("category_name") or p.get("category") or "")
            t = name + cat
            return any(
                k in t
                for k in (
                    "버스정류장",
                    "지하철",
                    "역 ",
                    "입구",
                    "Subway",
                )
            )

        n_tr = _count_pois_in_radius(lat, lng, 50.0, pois, is_transit)
        if n_tr >= 1:
            score += 10.0
        detail["transit_within_50m"] = {"count": n_tr, "points": 10.0 if n_tr >= 1 else 0.0}

        corner_pts = 5.0 if is_corner is True else 0.0
        if is_corner is None:
            detail["corner_lot"] = {"known": False, "points": 0.0}
        else:
            detail["corner_lot"] = {"known": True, "is_corner": is_corner, "points": corner_pts}
        score += corner_pts

        score = min(30.0, score)
        return score, detail

    def _score_foot_traffic_quality(
        self,
        percentile: Optional[float],
        on_main_artery: Optional[bool],
    ) -> Tuple[float, Dict[str, Any]]:
        detail: Dict[str, Any] = {
            "foot_traffic_percentile": percentile,
            "on_main_artery": on_main_artery,
        }
        if percentile is None:
            detail["points"] = 0.0
            detail["tier"] = "unknown"
            return 0.0, detail

        p = float(percentile)
        p = max(0.0, min(100.0, p))
        if p <= 10.0:
            base = 25.0
            tier = "top10"
        elif p <= 30.0:
            base = 15.0
            tier = "top30"
        else:
            base = 5.0
            tier = "average_or_below"

        mult = 1.5 if on_main_artery is True else 1.0
        score = min(25.0, base * mult)
        detail["tier"] = tier
        detail["base_points"] = base
        detail["main_artery_multiplier"] = mult
        detail["points"] = round(score, 2)
        return score, detail

    def _score_competition_synergy(
        self,
        lat: float,
        lng: float,
        dept: str,
        hospitals: Sequence[Dict[str, Any]],
        pharmacies: Sequence[Dict[str, Any]],
        same_building_same_dept: bool,
    ) -> Tuple[float, Dict[str, Any]]:
        score = 0.0
        detail: Dict[str, Any] = {}

        if same_building_same_dept:
            score -= 2.0
        detail["same_building_same_dept_penalty"] = -2.0 if same_building_same_dept else 0.0

        dept_n = _normalize_dept(str(dept))

        def other_medical(h: Dict[str, Any]) -> bool:
            pair = _poi_latlng(h)
            if not pair:
                return False
            if _haversine_m(lat, lng, pair[0], pair[1]) > 50.0:
                return False
            hd = str(h.get("dept_name") or h.get("dept") or "")
            if _normalize_dept(hd) == dept_n:
                return False
            return True

        other_cnt = sum(1 for h in hospitals or [] if other_medical(h))

        def pharma_within_50(p: Dict[str, Any]) -> bool:
            pair = _poi_latlng(p)
            if not pair:
                return False
            return _haversine_m(lat, lng, pair[0], pair[1]) <= 50.0

        ph_cnt = sum(1 for p in pharmacies or [] if pharma_within_50(p))

        synergy_raw = other_cnt + ph_cnt
        # 스펙: 타과 의원 + 약국 50m 내 3개 이상 → +12 의도, 섹션 상한 10
        if synergy_raw >= 3:
            score += min(10.0, 12.0)
        detail["other_dept_clinics_50m"] = other_cnt
        detail["pharmacies_50m"] = ph_cnt
        detail["synergy_cluster_size"] = synergy_raw
        detail["synergy_points_capped"] = min(10.0, 12.0) if synergy_raw >= 3 else 0.0

        score = max(-2.0, min(10.0, score))
        return score, detail


def _normalize_dept(s: str) -> str:
    s = str(s or "").strip()
    if s == "정신과":
        return "정신건강의학과"
    return s


__all__ = [
    "BluedotLocationAnalyzer",
    "LocationAnalyzerInput",
]
