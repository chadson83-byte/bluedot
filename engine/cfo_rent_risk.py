# -*- coding: utf-8 -*-
"""
임대료 상승·젠트리피케이션 리스크 — 활력 지수 기반 추정.
KOSIS 상업용부동산 임대동향 연동 시 `data_source` 갱신.
"""
from __future__ import annotations

from typing import Any, Dict


def estimate_rent_risk(
    activity_index: float,
    estimated_rent_per_pyeong: float,
    *,
    young_ratio: float = 0.25,
) -> Dict[str, Any]:
    """
    activity_index: 지하철·앵커 등으로부터 산출된 상권 활력(기존 main 로직과 동일 스케일).
    """
    ai = int(abs(float(activity_index)))
    y = max(0.0, min(1.0, float(young_ratio)))
    # 최근 1년 임대료 상승률 추정 5~22%
    yoy_pct = round(5.0 + (ai % 17) * 0.4 + y * 8.0, 1)

    if estimated_rent_per_pyeong >= 120_000 or yoy_pct >= 18:
        level = "high"
        label = "초기 보증금·월세 리스크 높음"
        hint = "KOSIS 상업용부동산 임대동향 및 실거래 확인을 강력 권장합니다."
    elif estimated_rent_per_pyeong >= 90_000 or yoy_pct >= 12:
        level = "medium"
        label = "임대 상승 압력 있음"
        hint = "계약 갱신 조건·인상률 상한을 협상에 반영하세요."
    else:
        level = "low"
        label = "상대적으로 안정적"
        hint = "다만 입지별 편차가 크므로 현장 임장이 필요합니다."

    return {
        "engine_version": "rent_risk_v1",
        "estimated_rent_yoy_pct": yoy_pct,
        "risk_level": level,
        "risk_label_ko": label,
        "cfo_hint": hint,
        "data_source": "activity_index_proxy",
        "data_source_target": "KOSIS_COMMERCIAL_RENT_SURVEY",
    }
