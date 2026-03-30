# -*- coding: utf-8 -*-
"""
도보 유효 범위 폴리곤 — V1은 원형 근사(GeoJSON Polygon).
카카오 모빌리티 / TMAP 도보 isochrone 연동 시 교체.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List

# 도보 분당 거리 (m) — 도심 평균
DEFAULT_METERS_PER_MINUTE = 70.0


def walkable_polygon_stub(lat: float, lng: float, minutes: float = 10.0) -> Dict[str, Any]:
    minutes = max(3.0, min(45.0, float(minutes)))
    radius_m = minutes * DEFAULT_METERS_PER_MINUTE
    n = 16
    coords: List[List[float]] = []
    # 소반경 근사: 위도 1도 ≈ 111.32km, 경도는 cos(위도) 보정
    lat_rad = math.radians(lat)
    for i in range(n):
        ang = 2 * math.pi * i / n
        d_north = radius_m * math.cos(ang)
        d_east = radius_m * math.sin(ang)
        dlat = d_north / 111320.0
        dlng = d_east / (111320.0 * max(0.2, math.cos(lat_rad)))
        coords.append([lng + dlng, lat + dlat])  # GeoJSON: [lng, lat]
    coords.append(coords[0])

    return {
        "engine_version": "walkable_stub_v1",
        "type": "Polygon",
        "coordinates": [coords],
        "properties": {
            "walk_minutes": minutes,
            "radius_meters_approx": radius_m,
            "method": "circle_approximation",
            "note": "8차선·하천 단절 필터는 V2에서 도보 네트워크 API로 적용 예정",
        },
        "data_source": "synthetic_circle",
        "data_source_target": "KAKAO_OR_TMAP_WALKING_ISOCHRONE",
    }
