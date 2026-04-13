# -*- coding: utf-8 -*-
"""
한의원 등 경쟁 기관 매출: 공개 신호(HIRA 병원기본목록 + 상호) 기반 아키타입·컨셉별 추정 구간.
실매출·원장 세무 데이터가 아님 — 확장 시 getDetail·인허가·ykiho 병합으로 정밀화.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _key_variants(k: str) -> Tuple[str, ...]:
    return tuple(dict.fromkeys((k, k.lower(), k.upper())))


def _int_from_item(item: dict, *keys: str) -> Optional[int]:
    for k in keys:
        for kk in _key_variants(k):
            if kk not in item:
                continue
            try:
                v = item.get(kk)
                if v is None or v == "":
                    continue
                return int(float(v))
            except (TypeError, ValueError):
                continue
    return None


def _yn_from_item(item: dict, *keys: str) -> bool:
    for k in keys:
        for kk in _key_variants(k):
            if kk not in item:
                continue
            s = str(item.get(kk)).strip().upper()
            if s in ("Y", "1", "YES", "TRUE", "예"):
                return True
    return False


def extract_hira_basis_facility_signals(item: Optional[dict]) -> Dict[str, Any]:
    """getHospBasisList item에서 병상·야간·주말 등 가능한 키를 넓게 스캔."""
    if not item or not isinstance(item, dict):
        return {}
    it = item
    beds = _int_from_item(
        it,
        "sickbedCnt",
        "sickBedCnt",
        "sickbedcnt",
        "totBedCnt",
        "totbedCnt",
        "bedCnt",
        "bedcnt",
        "hospBedCnt",
    )
    sig: Dict[str, Any] = {}
    if beds is not None and beds >= 0:
        sig["sickbed_count"] = beds
    sig["night_treatment_yn"] = _yn_from_item(
        it, "trmtNghtYn", "TrmtNghtYn", "rcvNghtYn", "RcvNghtYn", "trmtNghtYn"
    )
    sig["sat_treatment_yn"] = _yn_from_item(it, "trmtSatYn", "TrmtSatYn", "rcvSatYn", "RcvSatYn")
    sig["sun_treatment_yn"] = _yn_from_item(it, "trmtSunYn", "TrmtSunYn", "rcvSunYn", "RcvSunYn")
    sig["icu_possible_yn"] = _yn_from_item(it, "intnCarePsblYn", "IntnCarePsblYn", "intncarePsblYn")
    tot_staff = _int_from_item(it, "totEmpCnt", "totempCnt", "empCnt", "staffCnt", "totStaf")
    if tot_staff is not None and tot_staff >= 0:
        sig["total_employee_reported"] = tot_staff
    cl_cd = str(it.get("clCd") or it.get("clcd") or "").strip()
    if cl_cd:
        sig["cl_cd"] = cl_cd
    ykind = str(it.get("yadmKindNm") or it.get("ykindNm") or it.get("kindNm") or "").strip()
    if ykind:
        sig["yadm_kind_name"] = ykind
    return sig


def _name_signals_han(raw_name: str) -> Dict[str, bool]:
    n = str(raw_name or "")
    return {
        "inpatient_kw": any(
            k in n for k in ("입원", "한방병원", "침복합", "요양", "낮병동")
        ),
        "hospital_kw": any(k in n for k in ("한방병원", "병원")),
        "deputy_kw": "부원장" in n,
        "chuna_kw": "도수" in n or "추나" in n,
    }


def _effective_doctor_count_for_revenue(doctor_count: int, raw_name: str) -> int:
    """상호상 부원장인데 drTotCnt=1인 경우 등 — 매출 상한 프록시만 보정."""
    dc = max(1, int(doctor_count))
    if "부원장" in str(raw_name or "") and dc < 2:
        return 2
    return dc


def _classify_han_archetype(
    *,
    doctor_count: int,
    staff_count: int,
    raw_name: str,
    signals: Dict[str, Any],
) -> Tuple[str, str]:
    """
    Returns (archetype_id, archetype_label_ko).
    """
    nm = _name_signals_han(raw_name)
    beds = int(signals.get("sickbed_count") or 0)
    dc = max(1, int(doctor_count))
    sc = max(0, int(staff_count or 0))

    if beds >= 30 or (nm["hospital_kw"] and beds >= 10):
        return "hospital_grade", "병원급·다병상(신고·상호 기준)"
    if beds >= 8 or nm["inpatient_kw"] or (beds >= 1 and nm["hospital_kw"]):
        return "inpatient_clinic", "입원·병상 운영 클리닉"
    if dc >= 3 or sc >= 10:
        return "large_clinic", "다의원·대규모 직원"
    if dc >= 2 or sc >= 5 or nm["deputy_kw"]:
        return "dual_clinic", "공동·부원장·중간 규모"
    return "solo_clinic", "1인·소형 외래 중심"


def _schedule_mult_from_signals(signals: Dict[str, Any]) -> float:
    m = 1.0
    if signals.get("night_treatment_yn"):
        m *= 1.07
    if signals.get("sat_treatment_yn"):
        m *= 1.035
    if signals.get("sun_treatment_yn"):
        m *= 1.035
    return min(1.18, m)


def _tier_base_monthly_krw(archetype_id: str, sickbed_count: int) -> int:
    """아키타입별 월 매출 중앙값 프록시(원) — 보수~중간대."""
    beds = max(0, int(sickbed_count))
    if archetype_id == "solo_clinic":
        return 38_000_000
    if archetype_id == "dual_clinic":
        return 56_000_000
    if archetype_id == "large_clinic":
        return 74_000_000
    if archetype_id == "inpatient_clinic":
        return 88_000_000 + min(55_000_000, beds * 3_200_000)
    if archetype_id == "hospital_grade":
        return 125_000_000 + min(110_000_000, beds * 2_800_000)
    return 42_000_000


def _year_bonus(established_years: int) -> float:
    y = max(0, int(established_years))
    return 1.0 + min(0.35, y * 0.02)


def _fmt_monthly_band(low: int, high: int) -> str:
    lo = low // 10_000
    hi = high // 10_000
    return f"월 약 {lo:,}~{hi:,}만"


def _fmt_annual_band(low: int, high: int) -> str:
    """연 환산(단순 ×12) — 세무·회계 정의와 다를 수 있음."""
    lo_eok = (low * 12) / 100_000_000.0
    hi_eok = (high * 12) / 100_000_000.0
    return f"연 환산 약 {lo_eok:.1f}~{hi_eok:.1f}억"


def build_han_revenue_bundle(
    *,
    doctor_count: int,
    staff_count: int,
    established_years: int,
    raw_name: str,
    hira_item: Optional[dict],
    extra_mult: float,
) -> Dict[str, Any]:
    """
    한의원 전용: 관측 적합 1개(중앙) + 동일 입지 컨셉 3종 구간.
    """
    signals = extract_hira_basis_facility_signals(hira_item)
    arche_id, arche_label = _classify_han_archetype(
        doctor_count=doctor_count,
        staff_count=staff_count,
        raw_name=raw_name,
        signals=signals,
    )
    edc = _effective_doctor_count_for_revenue(doctor_count, raw_name)
    sch_m = _schedule_mult_from_signals(signals)
    yb = _year_bonus(established_years)
    em = float(extra_mult)
    if em < 0.82:
        em = 0.82
    if em > 1.32:
        em = 1.32

    beds = int(signals.get("sickbed_count") or 0)
    base = _tier_base_monthly_krw(arche_id, beds)
    # 의사 수: 1인 초과 시 체증 완화
    doc_scale = 1.0 + 0.22 * max(0, edc - 1) + 0.06 * max(0, min(6, staff_count or 0) - 2)
    doc_scale = min(2.15, doc_scale)

    mid = int(base * doc_scale * yb * sch_m * em)
    # 관측 아키타입에 맞는 밴드
    spread = 0.58 if arche_id == "solo_clinic" else 0.52 if arche_id == "hospital_grade" else 0.55
    low = max(12_000_000, int(mid * (1.0 - spread)))
    high = min(280_000_000, int(mid * (1.0 + spread * 1.15)))

    # --- 컨셉별 (동일 좌표에서 가능한 운영 모델) ---
    concepts: List[Dict[str, Any]] = []

    def _concept_row(cid: str, label: str, tier: str, hypothetical_beds: int) -> None:
        b0 = _tier_base_monthly_krw(tier, hypothetical_beds)
        ds = 1.0 + 0.18 * max(0, edc - 1)
        ds = min(2.0, ds)
        c_mid = int(b0 * ds * yb * sch_m * em)
        c_sp = 0.56
        c_lo = max(10_000_000, int(c_mid * (1.0 - c_sp)))
        c_hi = min(300_000_000, int(c_mid * (1.0 + c_sp * 1.1)))
        concepts.append(
            {
                "concept_id": cid,
                "label_ko": label,
                "monthly_band_ko": _fmt_monthly_band(c_lo, c_hi),
                "annual_band_ko": _fmt_annual_band(c_lo, c_hi),
                "monthly_mid_man": c_mid // 10_000,
            }
        )

    _concept_row(
        "outpatient_solo",
        "소형 외래·1~2인(입원 최소)",
        "solo_clinic",
        0,
    )
    _concept_row(
        "outpatient_team",
        "중형 클리닉·다직원·비급여 강화형",
        "large_clinic" if (staff_count or 0) >= 4 else "dual_clinic",
        0,
    )
    _concept_row(
        "inpatient_full",
        "입원·야간·주말 가동(병상·인력 투입)",
        "inpatient_clinic" if beds < 8 else "hospital_grade",
        max(beds, 12),
    )

    rev_str = f"월 추정 {mid // 10_000:,}만 (구간 {_fmt_monthly_band(low, high)})"
    rev_man = mid // 10_000
    detail_tail = (
        f" | [{arche_label}] 의사신고 {doctor_count}명·직원 {staff_count or 0}명·"
        f"병상신고 {beds if beds else '—'} · {_fmt_annual_band(low, high)}"
    )

    return {
        "estimated_revenue": rev_str,
        "estimated_revenue_man": rev_man,
        "detail_label_suffix": detail_tail,
        "revenue_scenarios": concepts,
        "revenue_archetype": {"id": arche_id, "label_ko": arche_label},
        "revenue_fit_monthly_man": {"low": low // 10_000, "mid": rev_man, "high": high // 10_000},
        "revenue_estimate_meta": {
            "model": "han_scenario_v1",
            "archetype_id": arche_id,
            "signals_used": signals,
            "name_keyword_hits": _name_signals_han(raw_name),
            "effective_doctors_revenue": edc,
            "schedule_mult": round(sch_m, 4),
            "year_bonus": round(yb, 4),
            "extra_mult_applied": round(em, 4),
            "disclaimer_ko": (
                "심평원 기본목록·상호 기반 추정입니다. 실매출·비급여 비중·원내 입원실은 "
                "상세 API·인허가·현장 확인으로 보정하세요."
            ),
        },
    }


