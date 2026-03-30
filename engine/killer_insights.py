# -*- coding: utf-8 -*-
"""
킬러 기능: 경쟁 병원 노후도, 리뷰, 주차·인프라, 타임매트릭 강화.
원칙: 실데이터가 없으면 수치를 만들지 않고 '미지원' 상태를 명확히 반환한다.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def enrich_hospital_killer_fields(h: Dict[str, Any], dept: str) -> None:
    """병원 dict에 in-place로 킬러 필드 추가."""
    raw_estb = str(h.get("established_date_raw") or "").strip()
    years_since_opening = None
    first_opening_year = None
    if raw_estb and raw_estb.isdigit() and len(raw_estb) >= 4:
        try:
            year = int(raw_estb[:4])
            first_opening_year = year
            years_since_opening = max(0, datetime.now().year - year)
        except Exception:
            years_since_opening = None
            first_opening_year = None

    h["years_since_opening"] = years_since_opening
    h["first_opening_year"] = first_opening_year
    h["years_since_opening_est"] = None
    h["first_opening_year_est"] = None
    h["permit_data_source"] = "LOCAL_GOV_PERMIT_FIRST_DATE_PENDING"
    h["review_avg_stub"] = None
    h["review_data_source"] = "KAKAO_NAVER_PLACE_PENDING"
    h["review_opportunity_hint"] = (
        "카카오맵/네이버 플레이스 리뷰 실데이터 연동 전까지는 경쟁 리뷰 점수를 표시하지 않습니다."
    )
    if years_since_opening is not None and first_opening_year is not None:
        h["facility_age_label"] = f"최초 개설연도 {first_opening_year}년 (약 {years_since_opening}년차)"
    else:
        h["facility_age_label"] = "최초 인허가일 미연동"


def summarize_competitor_age(hospitals: List[Dict[str, Any]]) -> str:
    """경쟁 병원 노후도 한 줄 요약."""
    if not hospitals:
        return "반경 내 동일 과목 경쟁 기관이 충분히 포착되지 않았습니다. (심평원 API·반경 조정)"
    known = [h.get("years_since_opening") for h in hospitals[:8] if h.get("years_since_opening") is not None]
    if known:
        old_cnt = sum(1 for a in known if int(a) >= 15)
        avg_age = sum(int(a) for a in known) / len(known)
        return (
            f"실측 가능한 {len(known)}곳 기준 평균 {avg_age:.0f}년차이며, "
            f"15년 이상 {old_cnt}곳입니다. (출처: 인허가 최초 개업일)"
        )
    return (
        "경쟁 병원 노후도는 현재 미지원입니다. "
        "지방행정 인허가 데이터의 '최초 인허가일' 연동 후에만 연차/순위를 제공합니다."
    )


def summarize_review_opportunity(hospitals: List[Dict[str, Any]]) -> str:
    return (
        "경쟁사 리뷰 평점 분석은 현재 미지원입니다. "
        "카카오맵/네이버 플레이스의 정책 준수 수집 파이프라인 연동 후 실제 평점만 표시합니다."
    )


def parking_and_infra_insight(lat: float, lng: float, row: Dict[str, Any]) -> Dict[str, Any]:
    """공영주차장 API + 마스터 약국 수 기반(사실 데이터만 표시)."""
    pharmacy_cnt = int(row.get("pharmacy_cnt", 0) or 0)
    parking_line = (
        "공영주차장 거리·도보시간 데이터는 현재 미지원입니다. "
        "전국 공영주차장 API 좌표 매칭 연동 후 실제 거리(분)로 표기합니다."
    )
    pharma_line = (
        f"동일 상권 약국 수: {pharmacy_cnt}곳 (출처: BLUEDOT 마스터 pharmacy_cnt)"
        if pharmacy_cnt > 0
        else "동일 상권 약국 수 데이터가 0으로 집계되었습니다. (출처: BLUEDOT 마스터 pharmacy_cnt)"
    )
    return {
        "parking_summary": parking_line,
        "pharmacy_infra_summary": pharma_line,
        "data_sources": {
            "parking": "DATA_GO_KR_PUBLIC_PARKING_API_PENDING",
            "pharmacy": "BLUEDOT_MASTER_V6_pharmacy_cnt",
        },
    }


def build_node_killer_insights(
    hospitals: List[Dict[str, Any]],
    row: Dict[str, Any],
    lat: float,
    lng: float,
) -> Dict[str, Any]:
    return {
        "competitor_age_narrative": summarize_competitor_age(hospitals),
        "review_opportunity_narrative": summarize_review_opportunity(hospitals),
        "parking_infra": parking_and_infra_insight(lat, lng, row),
        "engine_version": "killer_insights_v2_truth_first",
    }


def enhance_time_matrix_killer(
    labels: List[str],
    values: List[int],
    is_office: bool,
) -> Dict[str, Any]:
    """실데이터 연동 전에는 특정 요일/시간 수치를 단정하지 않는다."""
    return {
        "peak_day": None,
        "peak_time_suggestion": None,
        "killer_narrative": (
            "시간대별 유동인구 실데이터(SKT Data Hub / 서울시 생활인구) 미연동 상태입니다. "
            "현재는 특정 요일·시간 추천을 제공하지 않습니다."
        ),
        "data_source": "SKT_SEOUL_LIVING_POP_PENDING",
    }
