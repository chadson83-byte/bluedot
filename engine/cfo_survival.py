# -*- coding: utf-8 -*-
"""
상권 생존율/폐업률 추정 — V1은 좌표·과목 기반 결정론적 시뮬레이션.
지방행정 인허가 개업/폐업 실데이터 연동 시 `data_source` 교체.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict


def _seed_from_coords(lat: float, lng: float, dept: str) -> int:
    h = hashlib.sha256(f"{lat:.5f},{lng:.5f},{dept}".encode()).hexdigest()
    return int(h[:8], 16)


def estimate_survival_metrics(lat: float, lng: float, dept: str) -> Dict[str, Any]:
    """
    반환: 폐업률(연), 추정 평균 생존 연수, 안전 등급, 코멘트.
    실제 행정 데이터 없을 때 지역·과목 해시로 안정적인 데모 수치 생성.
    """
    s = _seed_from_coords(lat, lng, dept)
    # 연 폐업률 2% ~ 8% 범위
    closure_rate_pct = round(2.0 + (s % 6000) / 1000.0, 1)
    # 평균 생존 6~11년
    avg_survival_years = round(6.0 + (s % 50) / 10.0, 1)

    if closure_rate_pct < 4.0 and avg_survival_years >= 8.0:
        grade = "A"
        safety = "매우 안전"
    elif closure_rate_pct < 5.5:
        grade = "B"
        safety = "양호"
    elif closure_rate_pct < 7.0:
        grade = "C"
        safety = "보통"
    else:
        grade = "D"
        safety = "주의"

    comment = (
        f"이 동네 추정 연간 폐업률은 약 {closure_rate_pct}% 미만 수준이며, "
        f"동종 업종 평균 생존 기간은 약 {avg_survival_years}년으로 {safety} 구간으로 분류됩니다. "
        f"(실제 개폐업은 지방행정 인허가·4대보험 연계 데이터로 검증 예정)"
    )

    return {
        "engine_version": "survival_v1_synthetic",
        "dept": dept,
        "closure_rate_annual_pct": closure_rate_pct,
        "avg_survival_years_est": avg_survival_years,
        "safety_grade": grade,
        "safety_label_ko": safety,
        "comment": comment,
        "data_source": "synthetic_deterministic",
        "data_source_target": "LOCAL_GOV_PERMIT_OPEN_CLOSE",
    }
