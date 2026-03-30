# -*- coding: utf-8 -*-
"""BLUEDOT 분석 엔진 (AI CFO, 생존율, 도보 폴리곤, 페르소나)."""
from engine.cfo_bep import simulate_staff_bep
from engine.cfo_survival import estimate_survival_metrics
from engine.cfo_rent_risk import estimate_rent_risk
from engine.geo_walkable import walkable_polygon_stub
from engine.walkable_phase2 import (
    Phase2Config,
    get_walking_polygon,
    filter_data_by_polygon,
    calculate_persona_score,
    analyze_location,
)
from engine.persona import score_personas
from engine.master_context import resolve_nearest_master_context

__all__ = [
    "simulate_staff_bep",
    "estimate_survival_metrics",
    "estimate_rent_risk",
    "walkable_polygon_stub",
    "Phase2Config",
    "get_walking_polygon",
    "filter_data_by_polygon",
    "calculate_persona_score",
    "analyze_location",
    "score_personas",
    "resolve_nearest_master_context",
]
