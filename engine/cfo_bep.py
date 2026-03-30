# -*- coding: utf-8 -*-
"""
직원 수 기반 손익분기(BEP) — 개원 직후 생존 견적.
국민연금 사업장 직원수 연동은 V2에서 API로 대체.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

# 과목별 기본 객단가(원/회) — main.py DEPT_DEFAULT_TICKET_KRW와 정합
DEPT_DEFAULT_TICKET_KRW: Dict[str, int] = {
    "치과": 78000,
    "피부과": 110000,
    "안과": 65000,
    "정형외과": 55000,
    "소아과": 42000,
    "내과": 36000,
    "이비인후과": 38000,
    "산부인과": 52000,
    "정신건강의학과": 48000,
    "한의원": 55000,
    "약국": 12000,
    "동물병원": 85000,
}

# 월 의사 1인 인건비 추정(원) — 대표 원장 급여+4대
DEFAULT_DOCTOR_MONTHLY_KRW = 12_000_000
# 직원 1인 월 평균(원) — 간호·코디 등
DEFAULT_STAFF_MONTHLY_KRW = 2_800_000
# 기타 고정비(원) — 통신·세무·보험 등
DEFAULT_OVERHEAD_MONTHLY_KRW = 2_000_000


def simulate_staff_bep(
    dept: str,
    estimated_rent_per_pyeong: float,
    estimated_spending_index: float,
    *,
    doctors: int = 1,
    staff: int = 4,
    clinic_pyeong: float = 35.0,
    variable_cost_ratio: float = 0.12,
    doctor_monthly_krw: Optional[int] = None,
    staff_monthly_krw: Optional[int] = None,
    overhead_monthly_krw: Optional[int] = None,
) -> Dict[str, Any]:
    """
    월 고정비(임대 + 인건비 + 고정 경비) / (객단가 × 기여이익률) ≈ 필요 월간 환자 수.
    variable_cost_ratio: 재료·판촉 등 변동비 비율(매출 대비).
    """
    doctors = max(1, int(doctors))
    staff = max(0, int(staff))
    clinic_pyeong = max(10.0, float(clinic_pyeong))
    variable_cost_ratio = float(min(0.45, max(0.05, variable_cost_ratio)))

    monthly_rent = float(estimated_rent_per_pyeong) * clinic_pyeong

    doc_pay = doctor_monthly_krw if doctor_monthly_krw is not None else DEFAULT_DOCTOR_MONTHLY_KRW
    st_pay = staff_monthly_krw if staff_monthly_krw is not None else DEFAULT_STAFF_MONTHLY_KRW
    ovh = overhead_monthly_krw if overhead_monthly_krw is not None else DEFAULT_OVERHEAD_MONTHLY_KRW

    labor_monthly = doctors * doc_pay + staff * st_pay
    total_fixed = monthly_rent + labor_monthly + ovh

    base_ticket = float(DEPT_DEFAULT_TICKET_KRW.get(dept, 50_000))
    activity_adj = 0.88 + min(0.35, max(0.0, (float(estimated_spending_index) - 28_000.0) / 120_000.0))
    ticket_krw = max(15_000.0, base_ticket * activity_adj)

    contribution_margin = ticket_krw * (1.0 - variable_cost_ratio)
    min_monthly = int(math.ceil(total_fixed / contribution_margin)) if contribution_margin > 0 else 0
    workdays = 26
    daily = round(min_monthly / workdays, 1) if workdays else 0.0

    headline = (
        f"의사 {doctors}명, 직원 {staff}명, 평수 약 {clinic_pyeong:.0f}평 기준으로 "
        f"손익분기를 맞추려면 월 약 {min_monthly:,}명(영업일 기준 일평균 약 {daily}명)의 유료 진료가 필요합니다."
    )

    return {
        "engine_version": "cfo_bep_v1",
        "dept": dept,
        "assumptions": {
            "doctors": doctors,
            "staff": staff,
            "clinic_pyeong": clinic_pyeong,
            "variable_cost_ratio": variable_cost_ratio,
            "doctor_monthly_krw": doc_pay,
            "staff_monthly_krw_each": st_pay,
            "overhead_monthly_krw": ovh,
            "data_tier": "V7_estimated",
            "note": "국민연금 사업장 가입자 수·실임대는 V8에서 API/실거래로 정밀화 예정",
        },
        "monthly_rent_krw": int(monthly_rent),
        "monthly_labor_krw": int(labor_monthly),
        "monthly_fixed_total_krw": int(total_fixed),
        "estimated_ticket_krw": int(ticket_krw),
        "contribution_margin_per_visit_krw": int(contribution_margin),
        "breakeven_monthly_patients": min_monthly,
        "breakeven_daily_patients": daily,
        "headline": headline,
    }
