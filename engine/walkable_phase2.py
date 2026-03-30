# -*- coding: utf-8 -*-
"""
Phase 2: PostGIS + pgRouting 기반 도보 폴리곤 분석 파이프라인.

핵심 함수:
1) get_walking_polygon(lat, lon, minutes)
2) filter_data_by_polygon(polygon, raw_data)
3) calculate_persona_score(filtered_data, clinic_type)
4) analyze_location(...)  # end-to-end 실행 + fallback
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.geo_walkable import walkable_polygon_stub

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

try:
    from shapely.geometry import Point, shape
except Exception:  # pragma: no cover
    Point = None
    shape = None


@dataclass
class Phase2Config:
    db_host: str = "127.0.0.1"
    db_port: int = 5432
    db_name: str = "gis_db"
    db_user: str = "postgres"
    db_password: str = "postgres"
    meters_per_minute: float = 70.0
    fallback_radius_m: float = 500.0
    # False면 DB 연결 시도 없이 반경 폴백만 (Fly 등 PostGIS 없는 배포)
    use_pgr_network: bool = True


class WalkingPolygonError(RuntimeError):
    pass


def _to_km(minutes: float, meters_per_minute: float = 70.0) -> float:
    mm = max(1.0, min(60.0, float(minutes)))
    return (mm * meters_per_minute) / 1000.0


def get_walking_polygon(
    lat: float,
    lon: float,
    minutes: float,
    config: Optional[Phase2Config] = None,
) -> Dict[str, Any]:
    """
    pgRouting(pgr_drivingDistance) + PostGIS로 도보권 폴리곤(GeoJSON) 생성.
    실패 시 예외를 던지며, 상위 파이프라인에서 500m fallback 처리.
    """
    cfg = config or Phase2Config()
    if psycopg2 is None:
        raise WalkingPolygonError("psycopg2가 설치되지 않았습니다.")

    dist_km = _to_km(minutes, cfg.meters_per_minute)
    sql = """
    WITH start_node AS (
        SELECT id
        FROM ways_vertices_pgr
        ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 1
    ),
    dd AS (
        SELECT node
        FROM pgr_drivingDistance(
            'SELECT id, source, target, cost, reverse_cost FROM ways',
            (SELECT id FROM start_node),
            %s,
            directed := false
        )
    ),
    nodes AS (
        SELECT v.the_geom AS geom
        FROM dd
        JOIN ways_vertices_pgr v ON v.id = dd.node
    ),
    hull AS (
        SELECT ST_ConcaveHull(ST_Collect(geom), 0.80) AS geom FROM nodes
    )
    SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)) FROM hull
    """
    try:
        conn = psycopg2.connect(
            host=cfg.db_host,
            port=cfg.db_port,
            dbname=cfg.db_name,
            user=cfg.db_user,
            password=cfg.db_password,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (float(lon), float(lat), float(dist_km)))
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        raise WalkingPolygonError(f"DB 연결/쿼리 실패: {e}") from e

    if not row or not row[0]:
        raise WalkingPolygonError("도보 폴리곤을 생성하지 못했습니다.")

    import json

    try:
        geo = json.loads(row[0])
    except Exception as e:
        raise WalkingPolygonError(f"GeoJSON 파싱 실패: {e}") from e

    return {
        "engine_version": "walkable_pgr_v2",
        "type": geo.get("type", "Polygon"),
        "coordinates": geo.get("coordinates"),
        "properties": {
            "walk_minutes": float(minutes),
            "distance_km": round(dist_km, 4),
            "method": "pgr_drivingDistance + concave_hull",
        },
    }


def _point_from_row(row: Dict[str, Any]) -> Optional["Point"]:
    if Point is None:
        return None
    lat_keys = ("lat", "latitude", "center_lat", "YPos")
    lon_keys = ("lng", "lon", "longitude", "center_lng", "XPos")
    lat = next((row.get(k) for k in lat_keys if row.get(k) is not None), None)
    lon = next((row.get(k) for k in lon_keys if row.get(k) is not None), None)
    if lat is None or lon is None:
        return None
    try:
        return Point(float(lon), float(lat))  # GeoJSON 기준 [lon, lat]
    except Exception:
        return None


def filter_data_by_polygon(
    polygon: Dict[str, Any],
    raw_data: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    도보 폴리곤 내부(intersects) 데이터만 반환.
    Shapely 미설치 시 전체 데이터를 그대로 반환(보수적 fallback).
    """
    rows = list(raw_data or [])
    if not rows:
        return []
    if shape is None or Point is None:
        return rows
    try:
        poly = shape({"type": polygon.get("type", "Polygon"), "coordinates": polygon.get("coordinates")})
    except Exception:
        return rows

    out: List[Dict[str, Any]] = []
    for r in rows:
        pt = _point_from_row(r)
        if pt is None:
            continue
        if poly.intersects(pt):
            out.append(r)
    return out


def _safe_num(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _avg(rows: List[Dict[str, Any]], key: str, default: float = 0.0) -> float:
    vals = [_safe_num(r.get(key), None) for r in rows]
    vals = [v for v in vals if v is not None]
    if not vals:
        return default
    return sum(vals) / len(vals)


PERSONA_WEIGHTS: Dict[str, Dict[str, float]] = {
    "korean_medicine": {
        "elderly_ratio": 0.55,   # 60대 이상
        "population": 0.20,
        "income": 0.15,
        "women_20_39_ratio": 0.10,
    },
    "dermatology": {
        "elderly_ratio": 0.10,
        "population": 0.20,
        "income": 0.40,
        "women_20_39_ratio": 0.30,
    },
    "dentistry": {
        "elderly_ratio": 0.20,
        "population": 0.35,
        "income": 0.25,
        "women_20_39_ratio": 0.20,
    },
}


def calculate_persona_score(
    filtered_data: List[Dict[str, Any]],
    clinic_type: str,
) -> Dict[str, Any]:
    """
    폴리곤 내부 집계값으로 진료과목별 100점 만점 페르소나 점수 산출.
    입력 컬럼 예:
    - 총인구 (명)
    - 고령층_비중 (0~1)
    - women_20_39_ratio (0~1) 또는 female_2030_ratio
    - avg_income 또는 income_index
    """
    if not filtered_data:
        return {
            "score": 0.0,
            "clinic_type": clinic_type,
            "reason": "폴리곤 내부 데이터가 없습니다.",
            "metrics": {},
            "weights": PERSONA_WEIGHTS.get(clinic_type, PERSONA_WEIGHTS["korean_medicine"]),
        }

    pop_avg = _avg(filtered_data, "총인구 (명)", 0.0)
    elderly = _avg(filtered_data, "고령층_비중", 0.0)
    women2030 = _avg(filtered_data, "women_20_39_ratio", None)
    if women2030 is None:
        women2030 = _avg(filtered_data, "female_2030_ratio", 0.0)
    income = _avg(filtered_data, "avg_income", None)
    if income is None:
        income = _avg(filtered_data, "income_index", 0.0)

    # 스케일링(0~100)
    pop_score = max(0.0, min(100.0, (pop_avg / 60000.0) * 100.0))
    elderly_score = max(0.0, min(100.0, elderly * 100.0))
    women_score = max(0.0, min(100.0, women2030 * 100.0))
    # income은 원화 또는 index 혼재 가능 -> 0~100 클램프
    if income > 1000:  # 원화 단위로 들어온 경우(예: 월평균 소득)
        income_score = max(0.0, min(100.0, income / 100000.0))
    else:
        income_score = max(0.0, min(100.0, income))

    w = PERSONA_WEIGHTS.get(clinic_type, PERSONA_WEIGHTS["korean_medicine"])
    final = (
        elderly_score * w["elderly_ratio"]
        + pop_score * w["population"]
        + income_score * w["income"]
        + women_score * w["women_20_39_ratio"]
    )
    final = round(max(0.0, min(100.0, final)), 2)

    return {
        "score": final,
        "clinic_type": clinic_type,
        "weights": w,
        "metrics": {
            "population_score": round(pop_score, 2),
            "elderly_ratio_score": round(elderly_score, 2),
            "income_score": round(income_score, 2),
            "women_20_39_score": round(women_score, 2),
            "sample_size": len(filtered_data),
        },
    }


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _fallback_radius_filter(
    lat: float,
    lon: float,
    raw_data: Iterable[Dict[str, Any]],
    radius_m: float = 500.0,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in raw_data or []:
        p = _point_from_row(r)
        if p is None:
            continue
        if _haversine_m(lat, lon, p.y, p.x) <= radius_m:
            out.append(r)
    return out


def analyze_location(
    lat: float,
    lon: float,
    minutes: float,
    raw_data: Iterable[Dict[str, Any]],
    clinic_type: str,
    config: Optional[Phase2Config] = None,
) -> Dict[str, Any]:
    """
    메인 파이프라인:
    1) 도보 폴리곤 생성
    2) 폴리곤 내부 필터
    3) 페르소나 점수 산출
    실패 시 500m 반경 fallback.
    """
    cfg = config or Phase2Config()
    raw_rows = list(raw_data or [])
    used_fallback = False
    warn: Optional[str] = None

    if not cfg.use_pgr_network:
        used_fallback = True
        warn = (
            "PostGIS+pgRouting 미연결 — 반경 근사만 사용합니다. "
            "실도보 폴리곤은 OSM 도로망 DB를 띄운 뒤 POSTGIS_HOST 등을 설정하세요."
        )
        polygon = walkable_polygon_stub(lat, lon, minutes)
        filtered = _fallback_radius_filter(lat, lon, raw_rows, cfg.fallback_radius_m)
        persona = calculate_persona_score(filtered, clinic_type)
        return {
            "status": "success",
            "used_fallback": False,
            "postgis_skipped": True,
            "warning": warn,
            "walk_polygon": polygon,
            "filtered_count": len(filtered),
            "filtered_rows": filtered,
            "persona": persona,
        }

    try:
        polygon = get_walking_polygon(lat, lon, minutes, cfg)
        filtered = filter_data_by_polygon(polygon, raw_rows)
        if not filtered:
            used_fallback = True
            warn = "도보 폴리곤 내부 데이터가 없어 500m 반경으로 fallback했습니다."
            polygon = walkable_polygon_stub(lat, lon, cfg.fallback_radius_m / cfg.meters_per_minute)
            filtered = _fallback_radius_filter(lat, lon, raw_rows, cfg.fallback_radius_m)
    except Exception as e:
        used_fallback = True
        warn = f"도보 폴리곤 생성 실패로 500m 반경 fallback: {e}"
        polygon = walkable_polygon_stub(lat, lon, cfg.fallback_radius_m / cfg.meters_per_minute)
        filtered = _fallback_radius_filter(lat, lon, raw_rows, cfg.fallback_radius_m)

    persona = calculate_persona_score(filtered, clinic_type)
    return {
        "status": "success",
        "used_fallback": used_fallback,
        "warning": warn,
        "walk_polygon": polygon,
        "filtered_count": len(filtered),
        "filtered_rows": filtered,
        "persona": persona,
    }
