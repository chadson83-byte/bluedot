# -*- coding: utf-8 -*-
"""
1단계 권역 행정동 라벨(마스터 CSV) → 법정동코드·시도·시군구 추출.
카카오 coord2region/coord2address가 비거나 429일 때 트렌드·상권활성도 폴백용.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

from engine.bjdong_mapper import lookup_code10_from_kakao_address_name


def legaldong10_from_admin_region_label(label: str) -> Optional[str]:
    """예: '부산광역시 해운대구 중동' → 법정동 10자리."""
    s = re.sub(r"\s+", " ", str(label or "").strip())
    if len(s) < 4:
        return None
    return lookup_code10_from_kakao_address_name(s)


def sido_sigungu_tuple_from_admin_region_label(label: str) -> Optional[Tuple[str, str]]:
    """
    마스터 행정구역 문자열에서 시도·시군구 토큰 추출 (상권활성도 ES1013 집계 키용).
    예: '부산광역시 해운대구 중동' → ('부산광역시','해운대구')
    """
    parts = re.split(r"\s+", str(label or "").strip())
    if len(parts) < 2:
        return None
    sido: Optional[str] = None
    for p in parts:
        if any(
            p.endswith(suf)
            for suf in ("특별시", "광역시", "특별자치시", "특별자치도")
        ):
            sido = p
            break
        if p.endswith("도") and len(p) >= 2 and "특별" not in p:
            sido = p
            break
    sigungu: Optional[str] = None
    for p in reversed(parts):
        if len(p) >= 2 and (p.endswith("구") or p.endswith("군")):
            sigungu = p
            break
    if sido and sigungu:
        return (sido, sigungu)
    return None
