# -*- coding: utf-8 -*-
"""
마스터 CSV에서 좌표 기준 최근접 행정동 컨텍스트 추출 (CFO·타겟팅 API 공용).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from engine.geo_utils import haversine_km


def _find_col(df: pd.DataFrame, keywords: list) -> Optional[str]:
    for c in df.columns:
        c_clean = str(c).lower().replace(" ", "")
        if any(k in c_clean for k in keywords):
            return c
    return None


def resolve_nearest_master_context(
    df_master: pd.DataFrame,
    lat: float,
    lng: float,
    radius_km: float = 3.0,
) -> Optional[Dict[str, Any]]:
    """
    반환: region_name, row(dict), activity_index, estimated_rent_per_pyeong, estimated_spending_index, young_ratio
    """
    if df_master is None or df_master.empty:
        return None

    df = df_master.copy()
    col_pop = _find_col(df, ["총인구", "인구수", "pop"])
    col_name = _find_col(df, ["행정구역", "행정동", "읍면동", "동이름"])
    col_lat = _find_col(df, ["center_lat", "위도", "lat", "y좌표", "ypos"])
    col_lng = _find_col(df, ["center_lng", "경도", "lng", "lon", "x좌표", "xpos"])
    col_young = _find_col(df, ["젊은", "2030", "청년"])

    if not col_lat or not col_lng:
        return None

    df["총인구 (명)"] = pd.to_numeric(df[col_pop], errors="coerce").fillna(0) if col_pop else 0
    df["행정구역(동읍면)별"] = df[col_name].astype(str) if col_name else "미상"
    df["젊은층_비중"] = pd.to_numeric(df[col_young], errors="coerce").fillna(0).clip(0, 1) if col_young else 0.25
    df["고령층_비중"] = (1.0 - df["젊은층_비중"]).clip(0, 1)
    df["center_lat"] = pd.to_numeric(df[col_lat], errors="coerce").fillna(999.0)
    df["center_lng"] = pd.to_numeric(df[col_lng], errors="coerce").fillna(999.0)
    df["subway_count"] = pd.to_numeric(df.get("subway_count", 0), errors="coerce").fillna(0)
    df["anchor_cnt"] = pd.to_numeric(df.get("anchor_cnt", 0), errors="coerce").fillna(0)
    df["academy_cnt"] = pd.to_numeric(df.get("academy_cnt", 0), errors="coerce").fillna(0)
    df["bus_stop_count"] = pd.to_numeric(df.get("bus_stop_count", 0), errors="coerce").fillna(0)
    df["pharmacy_cnt"] = pd.to_numeric(df.get("pharmacy_cnt", 0), errors="coerce").fillna(0)
    df["fitness_cnt"] = pd.to_numeric(df.get("fitness_cnt", 0), errors="coerce").fillna(0)

    df = df.drop_duplicates(subset=["행정구역(동읍면)별"])
    df["distance_km"] = df.apply(
        lambda r: haversine_km(lat, lng, float(r["center_lat"]), float(r["center_lng"])),
        axis=1,
    )
    limit = max(float(radius_km) * 1.5, 1.5)
    df = df[df["distance_km"] <= limit]
    if df.empty:
        return None

    best = df.sort_values("distance_km").iloc[0]
    row = best.to_dict()
    subway = int(row.get("subway_count", 0) or 0)
    anchor = int(row.get("anchor_cnt", 0) or 0)
    activity_index = float(anchor + subway * 3)
    young = float(row.get("젊은층_비중", 0.25) or 0.25)

    estimated_rent_per_pyeong = 50000.0 + (activity_index * 8000.0)
    estimated_spending = 30000.0 + (activity_index * 1500.0) + (young * 20000.0)

    return {
        "region_name": str(row.get("행정구역(동읍면)별", "")),
        "distance_km": float(row.get("distance_km", 0)),
        "row": row,
        "activity_index": activity_index,
        "estimated_rent_per_pyeong": estimated_rent_per_pyeong,
        "estimated_spending_index": estimated_spending,
        "young_ratio": young,
    }
