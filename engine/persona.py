# -*- coding: utf-8 -*-
"""
과목별 페르소나 적합도 — 주거·유동·소비 지수 기반 스코어.
SGIS 주간인구 + 국세청 소득 연동 시 가중치 조정.
"""
from __future__ import annotations

from typing import Any, Dict


def score_personas(
    dept: str,
    row: Dict[str, Any],
    estimated_spending_index: float,
) -> Dict[str, Any]:
    pop = float(row.get("총인구 (명)", 0) or 0)
    young = float(row.get("젊은층_비중", 0.25) or 0.25)
    elderly = float(row.get("고령층_비중", 0.35) or 0.35)
    subway = int(row.get("subway_count", 0) or 0)
    anchor = int(row.get("anchor_cnt", 0) or 0)
    academy = int(row.get("academy_cnt", 0) or 0)

    # 0~100 스코어
    office_worker = min(100.0, 35.0 + subway * 8.0 + anchor * 3.0 + young * 40.0)
    family_kids = min(100.0, 30.0 + academy * 5.0 + (1.0 - young) * 25.0 + pop / 2000.0)
    elderly_care = min(100.0, 25.0 + elderly * 55.0 + pop / 2500.0)

    # 과목별 가중
    weights = {
        "피부과": {"office": 1.35, "family": 0.85, "elderly": 0.7},
        "치과": {"office": 1.2, "family": 1.1, "elderly": 0.9},
        "소아과": {"office": 0.7, "family": 1.45, "elderly": 0.6},
        "한의원": {"office": 0.95, "family": 1.05, "elderly": 1.15},
        "내과": {"office": 0.85, "family": 0.95, "elderly": 1.25},
    }
    w = weights.get(dept, {"office": 1.0, "family": 1.0, "elderly": 1.0})

    spend_factor = min(1.25, max(0.85, estimated_spending_index / 45000.0))
    s_office = round(office_worker * w["office"] * spend_factor, 1)
    s_family = round(family_kids * w["family"], 1)
    s_elderly = round(elderly_care * w["elderly"], 1)

    best = max(
        [("고소득·직장인 유동", s_office), ("영유아·가족", s_family), ("고령·거주", s_elderly)],
        key=lambda x: x[1],
    )

    narrative = (
        f"{dept} 기준으로는 「{best[0]}」 페르소나 적합도가 상대적으로 높습니다(지수 {best[1]}). "
        "SGIS 주간인구·시군구 평균 소득을 반영하면 정밀도가 올라갑니다."
    )

    return {
        "engine_version": "persona_v1",
        "dept": dept,
        "scores": {
            "office_worker_affinity": s_office,
            "family_children_affinity": s_family,
            "elderly_residential_affinity": s_elderly,
        },
        "leading_persona": best[0],
        "narrative": narrative,
        "data_source": "master_demographics_proxy",
        "data_source_target": "SGIS_WEEKDAY_POP + NTS_INCOME",
    }
