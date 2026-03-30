# -*- coding: utf-8 -*-
"""
data/car2024.xlsx 기반 자동차보험 진료건수 순위 인사이트.
시·군·구/시도 포맷을 모두 지원하며, 파일 원형이 통계표 형태(헤더가 2행 등)여도 최대한 안전하게 파싱한다.
"""
from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_XLSX = os.path.join(_BASE, "data", "car2024.xlsx")
DEFAULT_TAAS_XLSX = os.path.join(_BASE, "data", "TAAS_전국_사고건수_최종마스터.xlsx")


def extract_sigungu_key(admin_region: str) -> str:
    """
    마스터 행정구역 문자열에서 시·군·구 단위 키 추출.
    예: '강원 강릉시 교동' -> '강원 강릉시'
        '서울특별시 강남구 역삼1동' -> '서울특별시 강남구'
    """
    s = str(admin_region).strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) >= 2:
        sido = str(parts[0]).strip()
        sgg = str(parts[1]).strip()
        # '부산해운대구' 같이 시도명이 붙은 표기 정리
        if sgg.startswith(sido) and len(sgg) > len(sido):
            sgg = sgg[len(sido):]
        return f"{sido} {sgg}".strip()
    return s


def extract_sido_key(admin_region: str) -> str:
    s = str(admin_region).strip()
    if not s:
        return ""
    return s.split()[0]


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", "", str(s).strip())


def _short_sido(s: str) -> str:
    x = str(s).strip()
    for suf in ("특별자치도", "특별자치시", "광역시", "특별시", "자치시", "자치도", "도"):
        x = x.replace(suf, "")
    return x.strip()


def _is_count_col(name: str) -> bool:
    n = str(name).lower()
    return any(
        k in n
        for k in ("건수", "진료", "합계", "count", "cnt", "건", "보험")
    )


def _detect_columns(df) -> Tuple[Optional[str], Optional[str]]:
    """지역명 컬럼 + 건수 컬럼 자동 탐지."""
    import pandas as pd

    cols = list(df.columns)
    region_col = None
    value_col = None

    for c in cols:
        cs = str(c)
        if any(k in cs for k in ("시군구", "지역", "구역", "시도", "자치구", "명칭")):
            region_col = c
            break
    if region_col is None and cols:
        # 첫 번째 문자열 성격 컬럼
        for c in cols:
            if df[c].dtype == object or str(df[c].dtype) == "object":
                region_col = c
                break
    if region_col is None and cols:
        region_col = cols[0]

    numeric_candidates: List[str] = []
    for c in cols:
        if c == region_col:
            continue
        if _is_count_col(c):
            numeric_candidates.append(c)
    if numeric_candidates:
        # 가장 큰 합을 가진 숫자 컬럼을 건수로 간주
        best, best_sum = None, -1.0
        for c in numeric_candidates:
            s = pd.to_numeric(df[c], errors="coerce").fillna(0).sum()
            if s > best_sum:
                best_sum, best = s, c
        value_col = best
    if value_col is None:
        for c in cols:
            if c == region_col:
                continue
            s = pd.to_numeric(df[c], errors="coerce").fillna(0)
            if s.sum() > 0:
                value_col = c
                break

    return region_col, value_col


def _merge_region_columns(df):
    """시도 + 시군구 두 컬럼이 있으면 하나로 합침."""
    df_cols = list(df.columns)
    sido = next((c for c in df_cols if "시도" in str(c) and "시군" not in str(c)), None)
    sgg = next((c for c in df_cols if "시군구" in str(c) or "자치구" in str(c)), None)
    if sido is not None and sgg is not None:
        df = df.copy()
        df["_region_merged"] = (
            df[sido].astype(str).str.strip() + " " + df[sgg].astype(str).str.strip()
        )
        df["_region_merged"] = df["_region_merged"].str.replace(r"\s+", " ", regex=True).str.strip()
        return df, "_region_merged"
    return df, None


def _load_prepared_dataframe(path: str):
    import pandas as pd

    if not os.path.isfile(path):
        return None, "파일 없음"

    # 0) TAAS 최종마스터 포맷(지역명, 사고건수) 우선 처리
    try:
        taas = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
        cols = [str(c).strip() for c in taas.columns]
        if "지역명" in cols and "사고건수" in cols:
            t = taas[["지역명", "사고건수"]].copy()
            t["지역명"] = t["지역명"].astype(str).str.strip()
            t["사고건수"] = pd.to_numeric(t["사고건수"], errors="coerce").fillna(0)
            # 형식: 부산_해운대구 또는 부산_부산해운대구
            split = t["지역명"].str.split("_", n=1, expand=True)
            if split.shape[1] == 2:
                t["sido"] = split[0].astype(str).str.strip()
                t["sigungu_raw"] = split[1].astype(str).str.strip()
                t["sigungu"] = t.apply(
                    lambda r: r["sigungu_raw"][len(r["sido"]):].strip()
                    if str(r["sigungu_raw"]).startswith(str(r["sido"])) and len(str(r["sigungu_raw"])) > len(str(r["sido"]))
                    else r["sigungu_raw"],
                    axis=1,
                )
                t["region"] = t["sido"] + " " + t["sigungu"]
            else:
                t["region"] = t["지역명"]
            out = t[["region", "사고건수"]].rename(columns={"사고건수": "cnt"})
            out["region"] = out["region"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            out = out[out["region"].str.len() > 0]
            # 동일 지역 중복행은 합산 대신 최대값 사용(원본 중복 입력으로 인한 과대계상 방지)
            out = out.groupby("region", as_index=False)["cnt"].max()
            if len(out) > 0:
                return out, None
    except Exception:
        pass

    # 1) 일반 포맷 시도
    try:
        df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
        df, merged = _merge_region_columns(df)
        region_col, value_col = _detect_columns(df)
        if merged:
            region_col = merged
        if region_col is not None and value_col is not None:
            out = df[[region_col, value_col]].copy()
            out.columns = ["region", "cnt"]
            out["cnt"] = pd.to_numeric(out["cnt"], errors="coerce").fillna(0)
            out["region"] = out["region"].astype(str).str.strip()
            out = out[out["region"].str.len() > 0]
            out = out.groupby("region", as_index=False)["cnt"].sum()
            if len(out) > 0:
                return out, None
    except Exception:
        pass

    # 2) 통계표 포맷(제목행 + 헤더행) 대응: 질문자 파일 포맷
    try:
        raw = pd.read_excel(path, sheet_name=0, header=None, engine="openpyxl")
    except Exception as e:
        return None, str(e)

    header_idx = None
    for i in range(min(20, len(raw))):
        row_vals = [str(v).strip() for v in raw.iloc[i].tolist()]
        if "구분" in row_vals and any("명세서건수" in x for x in row_vals):
            header_idx = i
            break
    if header_idx is None:
        return None, "자동차보험 통계표 헤더(구분/명세서건수)를 찾지 못했습니다."

    t = raw.iloc[header_idx:].copy()
    t.columns = [str(c).strip() for c in t.iloc[0].tolist()]
    t = t.iloc[1:].copy()
    need_cols = ["구분", "입원외래구분", "명세서건수"]
    if any(c not in t.columns for c in need_cols):
        return None, "필수 컬럼(구분/입원외래구분/명세서건수)이 없습니다."

    t["구분"] = t["구분"].astype(str).str.strip()
    t["입원외래구분"] = t["입원외래구분"].astype(str).str.strip()
    t["명세서건수"] = pd.to_numeric(t["명세서건수"], errors="coerce").fillna(0)
    t = t[(t["입원외래구분"] == "계") & (t["구분"] != "전체")]
    out = t[["구분", "명세서건수"]].rename(columns={"구분": "region", "명세서건수": "cnt"})
    out = out.groupby("region", as_index=False)["cnt"].sum()
    out["region"] = out["region"].astype(str).str.strip()
    out = out[out["region"].str.len() > 0]
    if len(out) == 0:
        return None, "시도 단위 자동차보험 건수 데이터가 비어 있습니다."
    return out, None


@lru_cache(maxsize=4)
def _cached_table(path: str, mtime: float):
    return _load_prepared_dataframe(path)


def get_car_table(path: Optional[str] = None):
    p = path
    if p is None:
        p = DEFAULT_TAAS_XLSX if os.path.isfile(DEFAULT_TAAS_XLSX) else DEFAULT_XLSX
    if not os.path.isfile(p):
        return None, "파일 없음"
    mtime = os.path.getmtime(p)
    return _cached_table(p, mtime)


def _match_region_row(sigungu_key: str, regions: List[str]) -> Optional[str]:
    """엑셀 지역명과 마스터 시군구 키 매칭."""
    sk = _norm_key(sigungu_key)
    if not sk:
        return None
    best = None
    best_score = -1
    sk_s = _short_sido(sk)
    for r in regions:
        nr = _norm_key(r)
        nr_s = _short_sido(nr)
        if nr == sk:
            return r
        if nr_s == sk_s:
            return r
        if sk.endswith(nr) or nr.endswith(sk):
            score = min(len(sk), len(nr))
            if score > best_score:
                best_score, best = score, r
        elif sk_s.endswith(nr_s) or nr_s.endswith(sk_s):
            score = min(len(sk_s), len(nr_s))
            if score > best_score:
                best_score, best = score, r
        elif nr in sk or sk in nr:
            score = min(len(sk), len(nr)) // 2
            if score > best_score:
                best_score, best = score, r
    # 마지막 토큰만 일치 (예: 엑셀 '강릉시' vs 마스터 '강원 강릉시')
    parts = sigungu_key.split()
    if len(parts) >= 2:
        short = parts[-1]
        for r in regions:
            if _norm_key(short) == _norm_key(r) or _norm_key(r).endswith(_norm_key(short)):
                return r
    return best


def build_car_insurance_insight_for_region(
    admin_region_name: str,
    xlsx_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    리포트용: 시·군·구 단위 자동차보험 진료건수 순위 및 전국 대비 문구.
    """
    path = xlsx_path or (DEFAULT_TAAS_XLSX if os.path.isfile(DEFAULT_TAAS_XLSX) else DEFAULT_XLSX)
    sigungu = extract_sigungu_key(admin_region_name)
    sido = extract_sido_key(admin_region_name)

    df, err = get_car_table(path)
    if df is None:
        return {
            "ok": False,
            "narrative": (
                f"자동차보험 진료 데이터(`data/car2024.xlsx`)를 불러올 수 없습니다. "
                f"({err or '파일 없음'}) 프로젝트 `data` 폴더에 파일을 두면 전국 대비 순위가 표시됩니다."
            ),
            "sigungu_key": sigungu,
            "message": err or "car2024.xlsx 없음",
            "source_file": os.path.basename(path),
        }

    regions = df["region"].tolist()
    matched = _match_region_row(sigungu, regions)
    granularity = "시·군·구"
    if not matched:
        # 시도 단위 포맷 폴백
        matched = _match_region_row(sido, regions)
        granularity = "시도"
    elif len(str(matched).split()) <= 1:
        # 매칭 결과가 '강원', '서울' 등 단일 토큰이면 시도 단위로 간주
        granularity = "시도"
    if not matched:
        return {
            "ok": False,
            "narrative": (
                f"해당 상권 시·군·구(**{sigungu}**)는 `car2024.xlsx`의 지역명과 매칭되지 않았습니다. "
                "엑셀 **지역명(시군구 또는 시도)** 열을 마스터와 동일한 표기로 맞추면 "
                "전국 대비 자동차보험 진료건수 순위가 표시됩니다."
            ),
            "sigungu_key": sigungu,
            "sido_key": sido,
            "source_file": os.path.basename(path),
        }

    sub = df[df["region"] == matched].iloc[0]
    city_cnt = float(sub["cnt"])
    ranked = df.sort_values("cnt", ascending=False).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    row = ranked[ranked["region"] == matched].iloc[0]
    rank = int(row["rank"])
    n_regions = len(ranked)
    nationwide = float(ranked["cnt"].sum())
    share_pct = (city_cnt / nationwide * 100.0) if nationwide > 0 else 0.0

    unit_label = "시·군·구" if granularity == "시·군·구" else "시도"
    narrative = (
        f"이 지역이 속한 **{matched}** 기준 자동차보험 진료건수는 **{city_cnt:,.0f}건**으로, "
        f"전국 {unit_label} 합계 대비 약 **{share_pct:.2f}%** 수준이며 "
        f"동일 기준 **{n_regions}개** {unit_label} 중 **{rank}위**입니다. "
        f"(출처: `data/{os.path.basename(path)}`)"
    )

    return {
        "ok": True,
        "narrative": narrative,
        "sigungu_key": sigungu,
        "sido_key": sido,
        "matched_region": matched,
        "granularity": granularity,
        "city_car_insurance_count": city_cnt,
        "nationwide_total_count": nationwide,
        "share_pct": round(share_pct, 4),
        "rank_among_cities": rank,
        "total_cities_ranked": n_regions,
        "source_file": os.path.basename(path),
    }


def clear_cache():
    _cached_table.cache_clear()
