# -*- coding: utf-8 -*-
import math


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    try:
        r = 6371.0
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
        return r * 2 * math.asin(math.sqrt(a))
    except Exception:
        return 999.0


def offset_lat_lng(lat: float, lng: float, distance_m: float, bearing_deg: float) -> tuple:
    """대략적 이동 (미터, 방위각 도)."""
    r_earth = 6371000.0
    br = math.radians(bearing_deg)
    lat1 = math.radians(lat)
    lng1 = math.radians(lng)
    lat2 = math.asin(math.sin(lat1) * math.cos(distance_m / r_earth) + math.cos(lat1) * math.sin(distance_m / r_earth) * math.cos(br))
    lng2 = lng1 + math.atan2(
        math.sin(br) * math.sin(distance_m / r_earth) * math.cos(lat1),
        math.cos(distance_m / r_earth) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lng2)
