from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import logging
import random
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import pandas as pd
import numpy as np
import math
import os
import jwt
from datetime import datetime
import time
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Tuple

# DB & Auth
import database as db
from auth_config import KAKAO_REST_KEY, GOOGLE_CLIENT_ID, JWT_SECRET, BLUEDOT_TEST_MODE, PORTONE_API_KEY, PORTONE_API_SECRET

# Phase 1 AI CFO & 타겟팅 엔진
from engine.master_context import resolve_nearest_master_context
from engine.cfo_bep import simulate_staff_bep
from engine.cfo_survival import estimate_survival_metrics
from engine.cfo_rent_risk import estimate_rent_risk
from engine.geo_walkable import walkable_polygon_stub
from engine.persona import score_personas
from engine.killer_insights import (
    enrich_hospital_killer_fields,
    build_node_killer_insights,
    enhance_time_matrix_killer,
)
from engine.car_insurance_stats import build_car_insurance_insight_for_region
from engine.micro_site import (
    build_micro_site_payload,
    build_region_candidate_scores,
    collect_anchor_pois,
    dedupe_pick_top,
    enrich_stage2_top_with_rationale,
)
from engine.walkable_phase2 import analyze_location, get_walking_polygon, Phase2Config
from building_analyzer import generate_aging_report

import re
from engine.address_resolver import resolve_jibun_codes
from engine.bjdong_mapper import DEFAULT_BJDONG_TXT

app = FastAPI(title="BLUEDOT Backend API - National Flexible Radius Edition")


@app.get("/api/health")
def api_health():
    """프론트·배포 점검용 — 200이면 API 서버 정상."""
    p2 = _build_phase2_config()
    bjdong_ok = os.path.isfile(DEFAULT_BJDONG_TXT)
    bjdong_sz = os.path.getsize(DEFAULT_BJDONG_TXT) if bjdong_ok else 0
    dg = (os.getenv("DATA_GO_KR_SERVICE_KEY") or os.getenv("BUILDING_HUB_SERVICE_KEY") or "").strip()
    return {
        "ok": True,
        "service": "bluedot",
        "docs": "/docs",
        "features": {
            "postgis_walking_polygon": bool(p2.use_pgr_network),
            "fly": bool(os.getenv("FLY_APP_NAME", "").strip()),
        },
        "building_pipeline": {
            "bjdong_txt_exists": bjdong_ok,
            "bjdong_txt_bytes": bjdong_sz,
            "bjdong_txt_basename": os.path.basename(DEFAULT_BJDONG_TXT),
            "hira_key_from_env": bool(os.getenv("HIRA_API_KEY", "").strip()),
            "data_go_building_key_from_env": bool(dg),
            "kakao_rest_key_from_env": bool(os.getenv("KAKAO_REST_KEY", "").strip()),
            "juso_key_from_env": bool(
                (os.getenv("JUSO_CONFM_KEY") or os.getenv("JUSO_ADDR_LINK_KEY") or "").strip()
            ),
            "hint": (
                "로컬에만 파일/키가 있으면 Vercel 화면은 변하지 않습니다. "
                "Fly 이미지에 법정동 txt가 들어가려면 git 커밋·push 후 fly deploy 하고, "
                "fly secrets 로 HIRA_API_KEY·DATA_GO_KR_SERVICE_KEY·KAKAO_REST_KEY 를 넣으세요. "
                "이 객체가 false/0이면 해당 단계가 서버에서 비어 있는 것입니다."
            ),
        },
    }


@app.on_event("startup")
def _startup_building_pipeline_warn():
    if not os.path.isfile(DEFAULT_BJDONG_TXT):
        logging.warning(
            "법정동코드 파일 없음 (%s) — regex 폴백만으로는 bjdongCd 매칭이 약합니다.",
            DEFAULT_BJDONG_TXT,
        )
    if not (os.getenv("KAKAO_REST_KEY") or "").strip():
        logging.warning(
            "KAKAO_REST_KEY 미설정 — 주소→법정동(b_code) 1순위를 쓰지 않습니다. "
            "Fly 배포 시 fly secrets set KAKAO_REST_KEY=... 권장.",
        )


# DB 초기화
db.init_db()

security = HTTPBearer(auto_error=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 운영 시에는 환경변수 사용 권장: HIRA_API_KEY
HIRA_API_KEY = os.getenv("HIRA_API_KEY", "8ee102c5d025b9a9709736175aa0168bac653098ef0f762e797f727d77dc7da9")


def _include_car_insurance_insight_for_dept(dept: str) -> bool:
    """자동차보험 진료건수 지표는 한의원 리포트에만 사용."""
    return "한의원" in str(dept or "")


def _get_current_user_id(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[int]:
    if not credentials:
        return None
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=["HS256"])
        return payload.get("user_id")
    except Exception:
        return None


def _require_auth_and_use_credit(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> int:
    """분석 API용: 인증 필수, 크레딧 1회 차감 후 user_id 반환. 실패 시 HTTPException."""
    user_id = _get_current_user_id(credentials)
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    ok = db.use_credit(user_id)
    if not ok:
        raise HTTPException(status_code=402, detail="분석 횟수가 없습니다. 결제 후 이용해 주세요.")
    return user_id


# =========================================================
# [0] 🚀 마스터 데이터 로드 (V7 전국/신뢰성 개선 버전 우선)
# =========================================================
_BASE = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV_PATH = os.path.join(_BASE, "bluedot_master_v7.csv")
if not os.path.exists(MASTER_CSV_PATH):
    MASTER_CSV_PATH = os.path.join(_BASE, "bluedot_master_v6.csv")
df_master = None

try:
    if os.path.exists(MASTER_CSV_PATH):
        df_master = pd.read_csv(MASTER_CSV_PATH)
        ver = "V7" if "v7" in MASTER_CSV_PATH.lower() else "V6"
        print(f"[OK] [SYSTEM] 마스터 {ver} 데이터 로드 완료: 총 {len(df_master)}개 행정동 대기중.")
    else:
        print("[WARN] [SYSTEM] bluedot_master_v6.csv 파일이 없습니다. 경로를 확인해주세요.")
except Exception as e:
    print(f"[ERROR] CSV 로드 실패: {e}")

# 🚀 [추가] 바다 튕김 방지를 위한 실제 거리 계산 함수
def haversine_distance(lat1, lon1, lat2, lon2):
    try:
        R = 6371.0
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dLon/2)**2
        return R * 2 * math.asin(math.sqrt(a))
    except:
        return 999.0


def haversine_distance_vectorized(lat0: float, lon0: float, lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    """행정동 N건 거리(km) 벡터 연산 — df.apply 대비 대용량 마스터에서 체감 속도 개선."""
    R = 6371.0
    lat0_r = math.radians(float(lat0))
    lon0_r = math.radians(float(lon0))
    lat2 = np.radians(pd.to_numeric(lat_series, errors="coerce").fillna(999.0).to_numpy(dtype=np.float64))
    lon2 = np.radians(pd.to_numeric(lon_series, errors="coerce").fillna(999.0).to_numpy(dtype=np.float64))
    dlat = lat2 - lat0_r
    dlon = lon2 - lon0_r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat0_r) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    np.clip(a, 0.0, 1.0, out=a)
    dist = R * 2.0 * np.arcsin(np.sqrt(a))
    return pd.Series(dist, index=lat_series.index)


def _docs_per_10k_column(df: pd.DataFrame) -> pd.Series:
    pop = df["pop_in_10k"].astype(float)
    doc = df["dept_doctors"].astype(float)
    return pd.Series(np.where(pop > 0, doc / pop, 5.0), index=df.index)


def _hira_cache_ttl_seconds() -> float:
    try:
        t = float(os.getenv("BLUEDOT_HIRA_CACHE_TTL_SEC", "120"))
    except ValueError:
        t = 120.0
    return max(20.0, min(t, 3600.0))


def _analyze_top_nodes_workers() -> int:
    try:
        n = int(os.getenv("BLUEDOT_ANALYZE_HIRA_WORKERS", "5"))
    except ValueError:
        n = 5
    return max(1, min(8, n))


def _stage2_region_worker_count(n_nodes: int) -> int:
    try:
        w = int(os.getenv("BLUEDOT_STAGE2_REGION_WORKERS", "4"))
    except ValueError:
        w = 4
    w = max(1, min(5, w))
    return min(w, max(1, n_nodes))


def _dept_to_clinic_type(dept: str) -> str:
    mapping = {
        "한의원": "korean_medicine",
        "피부과": "dermatology",
        "치과": "dentistry",
    }
    return mapping.get(str(dept), "korean_medicine")


def _build_phase2_config() -> Phase2Config:
    """
    PostGIS: POSTGIS_HOST가 비어 있고 Fly에서 돌면 도보 네트워크를 쓰지 않음(연결 타임아웃 방지).
    로컬(uvicorn)에서는 기본 127.0.0.1로 기존과 같이 시도.
    """
    host_env = os.getenv("POSTGIS_HOST", "").strip()
    on_fly = bool(os.getenv("FLY_APP_NAME", "").strip())
    if host_env:
        use_pgr = True
        db_host = host_env
    elif on_fly:
        use_pgr = False
        db_host = "127.0.0.1"
    else:
        use_pgr = True
        db_host = "127.0.0.1"
    return Phase2Config(
        db_host=db_host,
        db_port=int(os.getenv("POSTGIS_PORT", "5432")),
        db_name=os.getenv("POSTGIS_DB", "gis_db"),
        db_user=os.getenv("POSTGIS_USER", "postgres"),
        db_password=os.getenv("POSTGIS_PASSWORD", "postgres"),
        meters_per_minute=float(os.getenv("WALK_METERS_PER_MINUTE", "70")),
        fallback_radius_m=float(os.getenv("WALK_FALLBACK_RADIUS_M", "500")),
        use_pgr_network=use_pgr,
    )


def _apply_phase2_walkable_filter(df: pd.DataFrame, lat: float, lng: float, dept: str, walk_minutes: int = 10):
    """
    Phase2 파이프라인 적용:
    - PostGIS+pgRouting 도보권 폴리곤 필터
    - 실패/빈결과 시 500m fallback(엔진 내부)
    """
    if df is None or df.empty:
        return df, {"status": "empty"}
    result = analyze_location(
        lat=lat,
        lon=lng,
        minutes=float(walk_minutes),
        raw_data=df.to_dict("records"),
        clinic_type=_dept_to_clinic_type(dept),
        config=_build_phase2_config(),
    )
    rows = result.get("filtered_rows") or []
    if rows:
        out_df = pd.DataFrame(rows)
    else:
        out_df = df
    return out_df, {
        "used_fallback": bool(result.get("used_fallback")),
        "postgis_skipped": bool(result.get("postgis_skipped")),
        "warning": result.get("warning"),
        "filtered_count": int(result.get("filtered_count") or 0),
        "walk_polygon": result.get("walk_polygon"),
        "persona": result.get("persona"),
    }


def _convert_addr_to_jibun_codes(addr: str, fallback_sigungu_cd: str = "") -> Dict[str, Any]:
    """
    (기존 파싱 로직 폐기) 도로명주소 API(JUSO)로 지번 코드 변환.
    실패 시 빈 값 반환.
    """
    try:
        return resolve_jibun_codes(addr, fallback_sigungu_cd=fallback_sigungu_cd)
    except Exception as e:
        return {"ok": False, "message": f"주소 변환 실패: {e}", "address": addr, "sigungu_cd": fallback_sigungu_cd}


def _competitors_for_building_aging(real_hosps, limit: int = 5) -> list:
    """건축물대장 조회용 경쟁기관 리스트 — 주소→지번·법정동(Kakao/JUSO/정규식)."""
    out = []
    lim = max(1, min(int(limit), 20))
    for h in (real_hosps or [])[:lim]:
        conv = _convert_addr_to_jibun_codes(
            str(h.get("address") or ""),
            fallback_sigungu_cd=str(h.get("sigungu_cd") or ""),
        )
        out.append(
            {
                "name": h.get("name") or h.get("display_name") or "경쟁기관",
                "address": h.get("address"),
                "sigungu_cd": str(conv.get("sigungu_cd") or h.get("sigungu_cd") or ""),
                "bjdong_cd": str(conv.get("bjdong_cd") or ""),
                "bun": str(conv.get("bun") or ""),
                "ji": str(conv.get("ji") or ""),
            }
        )
    return out


# 🚀 [AI 검색] 프롬프트에서 지역명 추출 → 마스터 데이터 행정구역 필터용
# (프롬프트 키워드, CSV 매칭 패턴) — 긴 키워드 우선
_REGION_PROMPT_TO_CSV = [
    ("김해시", "김해"), ("김해", "김해"),
    ("부산광역시", "부산"), ("부산시", "부산"), ("부산", "부산"),
    ("창원시", "창원"), ("창원", "창원"),
    ("진주시", "진주"), ("진주", "진주"),
    ("서울특별시", "서울"), ("서울시", "서울"), ("서울 ", "서울"), ("서울,", "서울"),
    ("인천광역시", "인천"), ("인천시", "인천"), ("인천", "인천"),
    ("대구광역시", "대구"), ("대구시", "대구"), ("대구", "대구"),
    ("대전광역시", "대전"), ("대전시", "대전"), ("대전", "대전"),
    ("광주광역시", "광주"), ("광주시", "광주"), ("광주", "광주"),
    ("울산광역시", "울산"), ("울산시", "울산"), ("울산", "울산"),
    ("수원시", "수원"), ("수원", "수원"),
    ("성남시", "성남"), ("성남", "성남"),
    ("고양시", "고양"), ("고양", "고양"),
    ("일산", "일산"),
    ("용인시", "용인"), ("용인", "용인"),
    ("강릉시", "강릉"), ("강릉", "강릉"),
    ("경남", "경남"), ("경북", "경북"), ("경기", "경기"), ("강원", "강원"),
    ("충남", "충남"), ("충북", "충북"), ("전남", "전남"), ("전북", "전북"),
]

def extract_region_from_prompt(prompt: str) -> tuple:
    """프롬프트에서 지역 키워드 추출. (CSV매칭패턴, 표시명) 반환. 없으면 (None, None)."""
    if not prompt or not isinstance(prompt, str):
        return None, None
    p = prompt.strip()
    for kw, pattern in _REGION_PROMPT_TO_CSV:
        if kw in p:
            return pattern, kw.strip().rstrip(",.")
    return None, None


# =========================================================
# [0.5] 🚀 B2B 컨설팅 확장: 6각 레이더 + BEP (V8 데이터 훅 포함)
# =========================================================
def _norm_10(val: float, ref: float) -> float:
    """0~ref → 0~10 스케일 (상한 클램프)."""
    if ref <= 0:
        return 0.0
    return round(max(0.0, min(10.0, 10.0 * val / ref)), 1)


def build_radar_balance(
    row: dict,
    doc_ratio: float,
    estimated_rent_per_pyeong: float,
    estimated_spending_index: float,
) -> dict:
    """
    6축 상권 밸런스 (10점 만점 정규화).
    V8: KOSIS 실임대료, SGIS 주간인구, TASIS 소비 → 동일 키로 치환 가능.
    """
    pop = float(row.get("총인구 (명)", 0) or 0)
    young = float(row.get("젊은층_비중", 0.25) or 0.25)
    subway = int(row.get("subway_count", 0) or 0)
    bus = int(row.get("bus_stop_count", 0) or 0)
    anchor = int(row.get("anchor_cnt", 0) or 0)
    pharmacy = int(row.get("pharmacy_cnt", 0) or 0)
    academy = int(row.get("academy_cnt", 0) or 0)
    fitness = int(row.get("fitness_cnt", 0) or 0)

    # 1 배후인구력
    r_pop = _norm_10(pop, 65000.0)
    # 2 유동집객력 (지하철·앵커·청년층)
    flow_raw = subway * 2.2 + anchor * 1.8 + young * 22.0
    r_flow = _norm_10(flow_raw, 85.0)
    # 3 경쟁여유도 (전문의 밀도 낮을수록 높음)
    stress = min(10.0, doc_ratio * 1.75)
    r_comp = round(max(0.0, 10.0 - stress), 1)
    # 4 소비·결제력 (추정 지수)
    r_spend = _norm_10(estimated_spending_index, 90000.0)
    # 5 교통접근성
    transit_raw = subway * 3.0 + min(bus, 35) * 0.55
    r_trans = _norm_10(transit_raw, 48.0)
    # 6 시너지 인프라
    syn_raw = pharmacy * 0.35 + academy * 0.22 + fitness * 0.55 + anchor * 1.1
    r_syn = _norm_10(syn_raw, 90.0)

    labels = ["배후인구력", "유동집객력", "경쟁여유도", "소비·결제력", "교통접근성", "시너지인프라"]
    values = [r_pop, r_flow, r_comp, r_spend, r_trans, r_syn]

    return {
        "labels": labels,
        "values": values,
        "meta": {
            "scale": "0-10",
            "data_tier": "V7_estimated",
            "v8_replacement_map": {
                "소비_결제력": "TASIS_시군구_소득_or_FinDX_카드",
                "유동집객력": "SGIS_주간인구지수",
                "배후인구력": "SGIS_거주인구_실측",
                "임대료_㎡": "KOSIS_상업용부동산_임대동향",
            },
        },
    }


# 진료과목별 기본 객단가(원/회) — V8에서 실데이터로 대체
DEPT_DEFAULT_TICKET_KRW = {
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

# 비급여·자유진료 비중이 높은 과 → BEP 코멘트 분기
DEPT_NON_COVERED_FOCUS = frozenset({"피부과", "치과", "안과", "정형외과"})


def build_bep_simulation(dept: str, estimated_rent_per_pyeong: float, estimated_spending_index: float, row: dict) -> dict:
    """
    (월 임대료 추정 + 고정 인건비) / 추정 객단가 ≈ 월 최소 필요 환자 수
    V8: KOSIS ㎡당 임대료, 실제 평수, 인건비 테이블 연동
    """
    clinic_pyeong = 35  # V8: 건물/층 선택값으로 대체
    monthly_rent = float(estimated_rent_per_pyeong) * clinic_pyeong

    # 과목별 월 고정 인건비 추정(원) — 대략적 컨설팅용
    labor_map = {
        "피부과": 24000000,
        "치과": 22000000,
        "한의원": 16000000,
        "소아과": 18000000,
    }
    fixed_labor = float(labor_map.get(dept, 17000000))

    total_fixed = monthly_rent + fixed_labor

    base_ticket = float(DEPT_DEFAULT_TICKET_KRW.get(dept, 50000))
    # 상권 활력 지수로 객단가 스케일 (추정)
    activity_adj = 0.88 + min(0.35, max(0.0, (estimated_spending_index - 28000.0) / 120000.0))
    ticket_krw = max(15000.0, base_ticket * activity_adj)

    min_monthly = int(math.ceil(total_fixed / ticket_krw)) if ticket_krw > 0 else 0
    workdays = 26
    daily = round(min_monthly / workdays, 1) if workdays else 0.0

    if dept in DEPT_NON_COVERED_FOCUS:
        revenue_model = "비급여중심"
        cfo = (
            f"비급여·자유진료 비중이 높은 과목으로, 주간 유동·직장인 집객과 객단가 변동이 손익에 큰 영향을 줍니다. "
            f"추정 기준으로 월 최소 {min_monthly:,}명의 유료 방문(영업일 기준 일평균 약 {daily}명)이 필요합니다."
        )
    else:
        revenue_model = "급여중심"
        cfo = (
            f"급여·배후 주거 인구가 외래·처방 수요를 견인하는 구조입니다. "
            f"손익분기를 위해 월 최소 {min_monthly:,}명의 환자(영업일 기준 일평균 약 {daily}명)를 유치해야 합니다."
        )

    return {
        "revenue_model": revenue_model,
        "assumptions": {
            "clinic_pyeong": clinic_pyeong,
            "fixed_labor_krw": int(fixed_labor),
            "default_ticket_krw": int(base_ticket),
            "data_tier": "V7_estimated",
        },
        "monthly_rent_krw": int(monthly_rent),
        "monthly_fixed_total_krw": int(total_fixed),
        "estimated_ticket_krw": int(ticket_krw),
        "breakeven_monthly_patients": min_monthly,
        "breakeven_daily_patients": daily,
        "cfo_comment": cfo,
        "v8_hooks": {
            "rent_m2_kosis": None,
            "labor_table_id": None,
            "ticket_fin_dx": None,
        },
    }


# =========================================================
# [0.6] Phase 3: Time-Matrix & 리스크 경고 (V8 통신사 유동인구 연동 예정)
# =========================================================
def build_time_matrix(row: dict, dept: str) -> dict:
    """
    요일별(월~일) 유동인구 지수 더미 배열. 상권 성격에 따라 피크 타임 분기.
    V8: 통신사 유동인구 실데이터로 치환.
    """
    subway = int(row.get("subway_count", 0) or 0)
    anchor = int(row.get("anchor_cnt", 0) or 0)
    young = float(row.get("젊은층_비중", 0.25) or 0.25)
    academy = int(row.get("academy_cnt", 0) or 0)

    # 오피스 상권: 지하철+앵커 높음 → 평일 저녁 피크
    # 주거 상권: 학원 많음, 지하철 낮음 → 토요일 오전 피크
    office_score = (subway * 2.5 + anchor * 1.5 + young * 15) / 30.0
    is_office = office_score > 0.5

    # 월~일 기본 패턴 (0~100 지수)
    if is_office:
        base = [72, 85, 88, 90, 92, 55, 38]  # 수목금 저녁 피크, 일요일 저락
    else:
        base = [68, 72, 70, 72, 75, 95, 82]  # 토요일 오전 피크, 일요일도 괜찮음

    # 허위 수치 논란 방지: 랜덤 노이즈 제거(결정론)
    values = [int(b) for b in base]

    labels = ["월", "화", "수", "목", "금", "토", "일"]

    # 진료시간 컨설팅 코멘트
    if is_office:
        hours_consulting = (
            "목·금요일 야간 진료 및 수요일 오후 야간 진료를 적극 추천합니다. "
            "직장인 퇴근 시간대(18~21시) 집객력이 높은 오피스형 상권입니다."
        )
    else:
        hours_consulting = (
            "토요일 오전 진료 및 일요일 오전 진료를 강력 추천합니다. "
            "가족 단위 내원이 많은 주거형 상권으로, 주말 오전 트래픽이 집중됩니다."
        )

    killer_tm = enhance_time_matrix_killer(labels, values, is_office)

    return {
        "labels": labels,
        "values": values,
        "zone_type": "office" if is_office else "residential",
        "hours_consulting": hours_consulting,
        "peak_day": killer_tm.get("peak_day"),
        "peak_time_suggestion": killer_tm.get("peak_time_suggestion"),
        "killer_narrative": killer_tm.get("killer_narrative"),
        "data_source_living_pop": killer_tm.get("data_source"),
        "meta": {"data_tier": "V7_dummy", "v8_hook": "통신사_유동인구_시간대별"},
    }


def build_risk_warnings(
    row: dict,
    dept: str,
    doc_ratio: float,
    estimated_rent_per_pyeong: float,
    status: str,
    f_score: float,
) -> list:
    """젠트리피케이션, 출혈 경쟁, 상권 단절 등 조건부 리스크 텍스트. V8: 카카오 도보거리 API 연동."""
    warnings = []
    activity = int(row.get("anchor_cnt", 0) or 0) + int(row.get("subway_count", 0) or 0) * 3

    # 1. 임대료 과다 (S급지 젠트리피케이션)
    rent_threshold = 120000
    if estimated_rent_per_pyeong >= rent_threshold:
        warnings.append(
            f"⚠️ 임대료가 평당 약 {estimated_rent_per_pyeong:,.0f}원으로 상승해 있어 젠트리피케이션(고임대) 리스크가 있습니다. "
            "KOSIS 상업용부동산 임대동향 실데이터로 재검증을 권장합니다."
        )

    # 2. 출혈 경쟁 (전문의 포화)
    if doc_ratio >= 5.0 or "극도 포화" in status:
        warnings.append(
            f"⚠️ 인구 1만명당 전문의 {doc_ratio:.1f}명으로 극도 포화 상태입니다. "
            "신규 진입 시 가격 경쟁·광고비 부담이 클 수 있습니다."
        )
    elif doc_ratio >= 3.5:
        warnings.append(
            f"⚠️ 경쟁 밀집도가 높은 구역입니다(인구 만명당 {doc_ratio:.1f}명). "
            "차별화 컨셉(전문진료·편의시간) 전략이 필수입니다."
        )

    # 3. 저점수 구역
    if f_score < 6.0:
        warnings.append(
            "⚠️ AI 종합 점수가 6점 미만으로, 상권 적합성이 낮게 평가되었습니다. "
            "현장 임장 및 경쟁사 실태조사를 반드시 수행하세요."
        )

    # 4. 교통/인프라 부족 (상권 단절 시사)
    if activity <= 2 and int(row.get("subway_count", 0) or 0) == 0:
        warnings.append(
            "⚠️ 지하철역·앵커 테넌트가 부재해 유동인구 접근성이 낮을 수 있습니다. "
            "V8 업데이트 시 카카오 도보거리 API로 물리적 단절(8차선 대로 등) 검증이 예정되어 있습니다."
        )

    return warnings


def attach_consulting_extensions(
    node: dict,
    row: dict,
    dept: str,
    doc_ratio: float,
    estimated_rent_per_pyeong: float,
    estimated_spending: float,
    status: str = "",
    f_score: float = 6.0,
) -> None:
    """recommendation dict에 레이더·BEP·Time-Matrix·리스크 인플레이스 병합."""
    node["radar_balance"] = build_radar_balance(row, doc_ratio, estimated_rent_per_pyeong, estimated_spending)
    node["bep_simulation"] = build_bep_simulation(dept, estimated_rent_per_pyeong, estimated_spending, row)
    node["time_matrix"] = build_time_matrix(row, dept)
    node["risk_warnings"] = build_risk_warnings(
        row, dept, doc_ratio, estimated_rent_per_pyeong, status, f_score
    )


# =========================================================
# [1] 기존: 심평원 실시간 API 검증 엔진 (+ 경쟁사 분석 엔진 탑재)
# =========================================================
def _mask_clinic_name(name: str, dept: str, mask_first_only: bool = True) -> str:
    """실제 등록명 첫 글자만 공개. 'OO한의원' → 'O*** 한의원'. 추정/가짜 데이터는 고정 문구."""
    if not name or "경쟁" in str(name) or "AI추정" in str(name):
        return f"주변 {dept} (참고)"
    name = str(name).strip()
    if len(name) <= 1:
        return name
    return name[0] + "*** " + dept


def _build_fact_tags(doctor_count: int, hours_tag: str, dept: str) -> list:
    """365일, 야간, 원장 N인, 특화, 프랜차이즈 등 팩트 기반 태그. V8: 인허가·NPS 연동."""
    tags = []
    if doctor_count >= 3:
        tags.append("365일 운영")
    elif "야간" in hours_tag or "365" in hours_tag:
        tags.append("야간진료")
    tags.append(f"원장 {doctor_count}인")
    # 허위/추정 태그(랜덤)는 표시하지 않음: 실데이터 연동 후에만 제공
    return tags


def _hospital_revenue_and_detail(doctor_count: int, staff_count: int, established_years: int, dept: str) -> tuple:
    """
    매출 추정(휴리스틱, 랜덤 금지).
    - 실데이터가 없는 구간이므로 '추정'임을 전제로, 입력(의사/직원/연차/과목)만으로 결정론적으로 산출.
    """
    # 과목별 월 매출 기여(원) 가정치(보수적)
    base_per_doc_map = {
        "피부과": 70000000,
        "치과": 60000000,
        "정형외과": 55000000,
        "안과": 55000000,
        "한의원": 42000000,
        "내과": 38000000,
        "이비인후과": 40000000,
        "소아과": 36000000,
        "산부인과": 48000000,
        "정신건강의학과": 42000000,
    }
    base_per_doc = float(base_per_doc_map.get(dept, 40000000))
    # 직원 생산성 가정(원/월) — 데이터 없으므로 보수적, 결정론
    staff_weight = 18000000.0
    rev_base = doctor_count * base_per_doc + float(max(0, staff_count)) * staff_weight
    year_bonus = 1.0 + min(0.35, max(0, established_years) * 0.02)  # 18년차까지 +35%
    rev_total = int(rev_base * year_bonus)
    if rev_total >= 100000000:
        rev_str = f"월 추정 {rev_total // 100000000}억"
    else:
        rev_str = f"월 추정 {rev_total // 10000:,}만"
    rev_man = rev_total // 10000  # 차트용 만원 단위
    if established_years >= 10:
        year_label = f"{established_years}년차 터줏대감"
    elif established_years >= 5:
        year_label = f"{established_years}년차 안정"
    else:
        year_label = f"{established_years}년차 신규"
    detail_label = f"(의사 {doctor_count}명, 직원 {staff_count}명) | {year_label} | {rev_str}"
    return rev_str, detail_label, rev_man


def get_dummy_hospitals(lat: float, lng: float, radius: int, dept: str, count: int):
    # 🚨 바다에 수백개씩 찍히지 않도록 마커 개수 안전 제한
    safe_count = min(int(count), 15)
    dummy_list = []
    
    for i in range(safe_count):
        doctor_count = random.randint(1, 4)
        staff_count = random.randint(max(2, doctor_count * 2), doctor_count * 5)
        established_years = random.randint(1, 20)
        
        if doctor_count >= 3:
            hours_tag = "🕒 365일/야간진료 (대형)"
        elif doctor_count == 2:
            hours_tag = "🕒 주 6일/평일야간 (공동개원)"
        else:
            hours_tag = "🕒 일반 진료시간 (1인원장)"
        
        rev_str, detail_label, rev_man = _hospital_revenue_and_detail(doctor_count, staff_count, established_years, dept)
        fact_tags = _build_fact_tags(doctor_count, hours_tag, dept)
        raw_name = f"경쟁 {dept} (AI추정)"
        display_name = _mask_clinic_name(raw_name, dept)

        h = {
            "id": f"dummy_{i}_{random.randint(1000,9999)}",
            "name": raw_name,
            "display_name": display_name,
            "fact_tags": fact_tags,
            "lat": lat + random.uniform(-0.005 * radius, 0.005 * radius),
            "lng": lng + random.uniform(-0.005 * radius, 0.005 * radius),
            "doctors": doctor_count,
            "staff_count": staff_count,
            "established_years": established_years,
            "hours": hours_tag,
            "estimated_revenue": rev_str,
            "estimated_revenue_man": rev_man,
            "detail_label": detail_label,
        }
        enrich_hospital_killer_fields(h, dept)
        dummy_list.append(h)
    return dummy_list


def _hira_fetch_hospitals_once(
    params: dict,
    dept: str,
    lat: float,
    lng: float,
    *,
    data_source: str,
    log_on_error: bool = True,
) -> Optional[Tuple[list, int]]:
    """
    HIRA getHospBasisList 1회 시도. 성공 시 (병원 dict 목록, 전문의 합) / 실패·0건 시 None.
    """
    url = "https://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList"
    response = None
    backoff = (0.2, 0.55, 1.0)
    for attempt in range(len(backoff) + 1):
        try:
            response = requests.get(url, params=params, timeout=14)
            break
        except Exception:
            response = None
            if attempt < len(backoff):
                time.sleep(backoff[attempt])
    if response is None:
        return None
    if response.status_code != 200:
        if log_on_error:
            logging.warning("HIRA HTTP %s (dept=%s)", response.status_code, dept)
        return None
    try:
        data = response.json()
    except Exception:
        return None
    resp = data.get("response", {})
    hdr = resp.get("header") or {}
    if hdr.get("resultCode") != "00":
        if log_on_error:
            logging.warning(
                "HIRA getHospBasisList 비정상: resultCode=%s resultMsg=%s dept=%s clCd=%s",
                hdr.get("resultCode"),
                hdr.get("resultMsg"),
                dept,
                params.get("clCd"),
            )
        return None
    items_raw = resp.get("body", {}).get("items") or {}
    if isinstance(items_raw, str):
        items_raw = {}
    items = items_raw.get("item") if isinstance(items_raw, dict) else []
    if items is None or items == "":
        items = []
    if isinstance(items, dict):
        items = [items]
    if not items:
        return None

    real_hospitals: list = []
    total_doctors_in_radius = 0
    for item in items:
        doctor_count = int(item.get("drTotCnt", 1))
        total_doctors_in_radius += doctor_count

        staff_count = None
        try:
            pn = item.get("pnursCnt")
            if pn is not None and pn != "":
                staff_count = int(float(pn))
        except Exception:
            staff_count = None

        estb = str(item.get("estbDd", "")).strip()
        established_years = 0
        try:
            if estb and estb.isdigit() and len(estb) >= 4:
                established_years = max(0, datetime.now().year - int(estb[:4]))
        except Exception:
            established_years = 0

        if doctor_count >= 3:
            hours_tag = "🕒 365일/야간진료 (대형)"
        elif doctor_count == 2:
            hours_tag = "🕒 주 6일/평일야간 (공동개원)"
        else:
            hours_tag = "🕒 일반 진료시간 (1인원장)"

        rev_str, detail_label, rev_man = _hospital_revenue_and_detail(
            doctor_count, int(staff_count or 0), established_years, dept
        )
        raw_name = item.get("yadmNm", f"경쟁 {dept}")
        display_name = _mask_clinic_name(raw_name, dept)
        fact_tags = _build_fact_tags(doctor_count, hours_tag, dept)

        h = {
            "id": item.get("ykiho", str(random.randint(1000, 9999))),
            "name": raw_name,
            "display_name": display_name,
            "fact_tags": fact_tags,
            "lat": float(item.get("YPos", lat)),
            "lng": float(item.get("XPos", lng)),
            "doctors": doctor_count,
            "staff_count": staff_count,
            "established_years": established_years,
            "hours": hours_tag,
            "estimated_revenue": rev_str,
            "estimated_revenue_man": rev_man,
            "detail_label": detail_label,
            "established_date_raw": str(item.get("estbDd", "")).strip() or None,
            "op_status": "영업중",
            "address": str(item.get("addr", "")).strip() or None,
            "sido_cd": str(item.get("sidoCd", "")).strip() or None,
            "sigungu_cd": str(item.get("sgguCd", "")).strip() or None,
            "data_source": data_source,
        }
        enrich_hospital_killer_fields(h, dept)
        real_hospitals.append(h)
    return real_hospitals, total_doctors_in_radius


def _normalize_dept_for_hira(dept: str) -> str:
    s = str(dept or "").strip()
    if s == "정신과":
        return "정신건강의학과"
    return s


def _hira_is_distinct_facility_clcd(dept: str) -> bool:
    """
    HIRA 종별코드가 양방 의원(31)과 겹치지 않는 진료 체계.
    이때 '반경 내 전체 기관' 병합이나 clCd=31 단독 폴백을 쓰면 소아·내과 등이 섞인다.
    """
    d = _normalize_dept_for_hira(dept)
    return d in ("한의원", "치과")


# 심평원: 동일 좌표·과목에 대한 짧은 호버/중복 호출을 줄여 과호출·429·타임아웃을 완화
_HIRA_CACHE_LOCK = threading.Lock()
_HIRA_CACHE: Dict[Tuple[float, float, int, str], Tuple[float, Tuple[list, int]]] = {}
_HIRA_CACHE_MAX_KEYS = 320


def _hira_cache_key(lat: float, lng: float, radius: int, dept: str) -> Tuple[float, float, int, str]:
    return (round(float(lat), 3), round(float(lng), 3), int(radius), str(dept))


def _copy_hira_tuple(t: Tuple[list, int]) -> Tuple[list, int]:
    lst, tot = t
    return [dict(h) for h in lst], int(tot)


def _hira_cache_get(key: Tuple[float, float, int, str]) -> Optional[Tuple[list, int]]:
    now = time.time()
    with _HIRA_CACHE_LOCK:
        ent = _HIRA_CACHE.get(key)
        if not ent:
            return None
        ts, val = ent
        if now - ts > _hira_cache_ttl_seconds():
            try:
                del _HIRA_CACHE[key]
            except KeyError:
                pass
            return None
        return _copy_hira_tuple(val)


def _hira_cache_set(key: Tuple[float, float, int, str], val: Tuple[list, int]) -> None:
    with _HIRA_CACHE_LOCK:
        _HIRA_CACHE[key] = (time.time(), val)
        while len(_HIRA_CACHE) > _HIRA_CACHE_MAX_KEYS:
            try:
                del _HIRA_CACHE[next(iter(_HIRA_CACHE))]
            except (StopIteration, KeyError):
                break


def _merge_hira_hospital_tuples(
    *parts: Optional[Tuple[list, int]],
) -> Optional[Tuple[list, int]]:
    """여러 HIRA 조회 결과를 ykiho 기준으로 합침. 앞쪽(과목 필터) 우선."""
    seen: set = set()
    merged: list = []
    for t in parts:
        if not t or not t[0]:
            continue
        for h in t[0]:
            hid = str(h.get("id") or "")
            if hid in seen:
                continue
            seen.add(hid)
            merged.append(h)
    if not merged:
        return None
    total = sum(int(h.get("doctors") or 1) for h in merged)
    return merged, total


def fetch_real_hospitals(lat: float, lng: float, radius: int, dept: str):
    """
    심평원 HIRA: 기본은 과목(또는 종별) 필터 조회 + 반경 완화(무필터) 병합.
    한의원(clCd=93)·치과(50)는 '무필터' 결과에 양방 의원·병원이 섞이므로 병합·clCd=31 폴백을 쓰지 않는다.
    그 외 과목은 1차 0건일 때 반경 내 타 기관 병합으로 호버 공백을 줄인다.
    """
    dept = _normalize_dept_for_hira(dept)
    if dept == "동물병원":
        return [], 0

    ck = _hira_cache_key(lat, lng, radius, dept)
    cached = _hira_cache_get(ck)
    if cached is not None:
        return cached

    clCd = "31"
    dgsbjtCd = ""
    if dept == "치과":
        clCd = "50"
    elif dept == "한의원":
        clCd = "93"
    else:
        clCd = "31"
        dept_codes = {
            "내과": "01",
            "피부과": "14",
            "안과": "12",
            "정형외과": "05",
            "소아과": "11",
            "이비인후과": "13",
            "산부인과": "10",
            "정신건강의학과": "03",
        }
        dgsbjtCd = dept_codes.get(dept, "")

    base = {
        "ServiceKey": urllib.parse.unquote(HIRA_API_KEY),
        "xPos": lng,
        "yPos": lat,
        "radius": radius * 1000,
        "numOfRows": 500,
        "_type": "json",
    }
    params_primary = dict(base)
    if clCd != "31":
        params_primary["clCd"] = clCd
    if dgsbjtCd:
        params_primary["dgsbjtCd"] = dgsbjtCd

    primary = None
    relaxed = None
    strict_kind = _hira_is_distinct_facility_clcd(dept)
    if strict_kind:
        primary = _hira_fetch_hospitals_once(
            params_primary,
            dept,
            lat,
            lng,
            data_source="hira",
            log_on_error=True,
        )
    else:
        with ThreadPoolExecutor(max_workers=2) as ex:
            fut_p = ex.submit(
                _hira_fetch_hospitals_once,
                params_primary,
                dept,
                lat,
                lng,
                data_source="hira",
                log_on_error=True,
            )
            fut_r = ex.submit(
                _hira_fetch_hospitals_once,
                dict(base),
                dept,
                lat,
                lng,
                data_source="hira_nearby_all_types",
                log_on_error=False,
            )
            primary = fut_p.result()
            relaxed = fut_r.result()

    if strict_kind:
        merged = primary if (primary and primary[0]) else None
    else:
        merged = _merge_hira_hospital_tuples(primary, relaxed)
    if merged:
        if not strict_kind and not (primary and primary[0]) and (relaxed and relaxed[0]):
            logging.info(
                "HIRA 과목별 0건·병합 시 반경 완화만 사용 (dept=%s, n=%d)",
                dept,
                len(merged[0]),
            )
        _hira_cache_set(ck, merged)
        return merged

    if not strict_kind:
        params_cl31 = dict(base)
        params_cl31["clCd"] = "31"
        got31 = _hira_fetch_hospitals_once(
            params_cl31, dept, lat, lng, data_source="hira_nearby_cl31", log_on_error=False
        )
        if got31:
            logging.info("HIRA clCd=31 단독으로 반경 목록 확보 (dept=%s)", dept)
            _hira_cache_set(ck, got31)
            return got31

    dummies = get_dummy_hospitals(lat, lng, radius, dept, 10)
    for h in dummies:
        h["data_source"] = "estimate_hira_unreachable"
    tot = sum(int(h.get("doctors") or 1) for h in dummies)
    logging.warning(
        "HIRA 사용 불가·빈 결과 — 추정 경쟁 마커 %d건 반환 (실제 심평원 연동 시 교체됨)",
        len(dummies),
    )
    return dummies, tot

def fetch_demographics_and_revenue(radius: int):
    base_pop = random.randint(25000, 60000) * (radius ** 1.5)
    avg_revenue = random.randint(8000, 15000)
    return int(base_pop), avg_revenue

# =========================================================
# [2] 하단 메뉴 클릭 시 거시 상권 분석 (대표님 원본 유지)
# =========================================================
def analyze_node(node_name: str, lat: float, lng: float, dept: str, radius: int):
    real_hospitals, total_doctors = fetch_real_hospitals(lat, lng, radius, dept)
    clinic_count = len(real_hospitals)
    competition_capacity = max(1, total_doctors)
    population, avg_revenue = fetch_demographics_and_revenue(radius)
    
    pop_in_10k = population / 10000
    clinics_per_10k = clinic_count / pop_in_10k if pop_in_10k > 0 else 0
    doctors_per_10k = competition_capacity / pop_in_10k if pop_in_10k > 0 else 0

    is_red_ocean = False
    
    if dept in ["한의원", "치과"]:
        primary_metric = clinics_per_10k
        if primary_metric >= 4.5:
            is_red_ocean = True
            comp_level = "극도 포화"
        elif primary_metric >= 3.0:
            comp_level = "포화"
        elif primary_metric >= 1.5:
            comp_level = "보통"
        else:
            comp_level = "낮음"
        comp_penalty = min(30.0, (primary_metric / 3.0) * 15 * (1 + max(0, doctors_per_10k - primary_metric) * 0.1))
    else:
        primary_metric = doctors_per_10k
        if primary_metric >= 5.0:
            is_red_ocean = True
            comp_level = "극도 포화"
        elif primary_metric >= 3.5:
            comp_level = "포화"
        elif primary_metric >= 2.0:
            comp_level = "보통"
        else:
            comp_level = "낮음"
        comp_penalty = min(30.0, (primary_metric / 3.5) * 15)

    comp_text = f"{comp_level} (기관 {clinic_count}개 / 전문의 {int(competition_capacity)}명)"

    if dept in ["소아과", "치과"]:
        age_score = min(30.0, (random.uniform(0.15, 0.3) / 0.25) * 30)
    elif dept in ["피부과", "정신건강의학과", "산부인과"]:
        age_score = min(30.0, (random.uniform(0.4, 0.6) / 0.5) * 30)
    else:
        age_score = min(30.0, (random.uniform(0.2, 0.4) / 0.3) * 30)

    anchor_score = 15.0 if random.choice([True, False]) else random.uniform(3.0, 8.0)
    revenue_score = min(35.0, (avg_revenue / 15000) * 35)
    risk_penalty = random.uniform(2.0, 15.0)

    total_raw_score = age_score + anchor_score + revenue_score - risk_penalty - comp_penalty
    raw_final = max(4.0, min(9.9, ((total_raw_score + 50) / 100) * 10)) 
    final_score = min(raw_final, random.uniform(5.5, 6.8)) if is_red_ocean else raw_final

    f_age = round(age_score, 1)
    f_rev = round(revenue_score, 1)
    f_anc = round(anchor_score, 1)
    f_risk = round(risk_penalty, 1)
    f_comp = round(comp_penalty, 1)
    f_final = round(final_score, 1)

    if f_final >= 8.5:
        pop_text = "매우 높음 (A등급)"
        insight = "배후 인구가 탄탄하며 타겟 고객 밀집도가 우수한 최상급 상권입니다."
    elif f_final >= 7.0:
        pop_text = "높음 (B+등급)"
        insight = "배후 세대 구매력이 양호합니다. 타겟 마케팅을 통한 점유율 확보가 가능합니다."
    else:
        pop_text = "보통 (C등급)"
        insight = "🚨 강력 경고: 인구 대비 기관/전문의 비율이 초과된 출혈 경쟁 구역입니다." if is_red_ocean else "경쟁 대비 수요 성장이 정체되어 진입에 주의가 필요합니다."

    return {
        "name": node_name,
        "lat": lat,
        "lng": lng,
        "score_val": f_final,
        "score": f"{f_final}/10",
        "comp_text": comp_text,
        "pop_text": f"{population:,}명 ({pop_text})",
        "insight": insight,
        "hospitals": real_hospitals,
        "formula": {
            "age_score": f"{f_age:.1f}",
            "revenue_score": f"{f_rev:.1f}",
            "anchor_score": f"{f_anc:.1f}",
            "risk_penalty": f"{f_risk:.1f}",
            "comp_penalty": f"{f_comp:.1f}",
            "final_score": f"{f_final:.1f}"
        }
    }

# =========================================================
# [2.5] 🔐 로그인 & 마이페이지 API
# =========================================================
class MicroSiteStage2Node(BaseModel):
    lat: float
    lng: float
    name: Optional[str] = None
    rank: Optional[int] = None


class MicroSiteStage2Request(BaseModel):
    """1차 Top5 권역 좌표 → 권역 내 후보 9곳씩 점수화 후 전역 Top5 건물(후보) 입지."""
    department: str = "한의원"
    radius_m: int = 400
    nodes: List[MicroSiteStage2Node]


def _stage2_region_cands(
    node: MicroSiteStage2Node,
    eval_r: int,
    dept: str,
    kakao_key: str,
) -> List[Dict[str, Any]]:
    lat, lng = float(node.lat), float(node.lng)
    r_fetch_km = max(0.5, (eval_r + 320) / 1000.0)
    r_fetch_km = min(r_fetch_km, 3.0)
    hospitals, _ = fetch_real_hospitals(lat, lng, r_fetch_km, dept)
    anchor_r = min(2000, max(eval_r * 2 + 320, 720))
    anchors, _ = collect_anchor_pois(
        kakao_key=kakao_key,
        lat=lat,
        lng=lng,
        radius_m=anchor_r,
    )
    return build_region_candidate_scores(
        center_lat=lat,
        center_lng=lng,
        parent_name=str(node.name or "") or "권역",
        parent_rank=int(node.rank or 0),
        eval_radius_m=eval_r,
        anchors=anchors,
        hospitals=hospitals or [],
        dept=dept,
        df_master=df_master,
        resolve_master_ctx=resolve_nearest_master_context,
    )


class KakaoAuthRequest(BaseModel):
    access_token: str

class GoogleAuthRequest(BaseModel):
    id_token: str = ""
    access_token: str = ""

class TestAuthRequest(BaseModel):
    name: str = "테스트사용자"

@app.post("/api/auth/kakao")
def auth_kakao(req: KakaoAuthRequest):
    """카카오 액세스 토큰으로 로그인."""
    if not KAKAO_REST_KEY:
        raise HTTPException(status_code=503, detail="카카오 로그인이 설정되지 않았습니다. KAKAO_REST_KEY를 설정하세요.")
    try:
        r = requests.get(
            "https://kapi.kakao.com/v2/user/me",
            headers={"Authorization": f"Bearer {req.access_token}"},
            timeout=5
        )
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail="카카오 토큰 검증 실패")
        data = r.json()
        pid = str(data.get("id", ""))
        email = (data.get("kakao_account", {}) or {}).get("email", "")
        name = ((data.get("kakao_account", {}) or {}).get("profile", {}) or {}).get("nickname", "")
        uid = db.get_or_create_user("kakao", pid, email, name)
        token = jwt.encode({"user_id": uid}, JWT_SECRET, algorithm="HS256")
        credits = db.get_user_credits(uid)
        return {"token": token, "user": {"id": uid, "name": name or email or "카카오사용자", "credits": credits}}
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.post("/api/auth/google")
def auth_google(req: GoogleAuthRequest):
    """구글 ID 토큰 또는 액세스 토큰으로 로그인."""
    if not req.id_token and not req.access_token:
        raise HTTPException(status_code=400, detail="id_token 또는 access_token이 필요합니다.")
    if req.id_token and not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=503, detail="구글 로그인이 설정되지 않았습니다. GOOGLE_CLIENT_ID를 설정하세요.")
    try:
        if req.id_token:
            from google.oauth2 import id_token
            from google.auth.transport import requests as ga_requests
            idinfo = id_token.verify_oauth2_token(req.id_token, ga_requests.Request(), GOOGLE_CLIENT_ID)
            pid = str(idinfo.get("sub", ""))
            email = idinfo.get("email", "")
            name = idinfo.get("name", "")
        elif req.access_token:
            r = requests.get("https://www.googleapis.com/oauth2/v3/userinfo",
                headers={"Authorization": f"Bearer {req.access_token}"}, timeout=5)
            if r.status_code != 200:
                raise HTTPException(status_code=401, detail="구글 토큰 검증 실패")
            data = r.json()
            pid = str(data.get("sub", ""))
            email = data.get("email", "")
            name = data.get("name", "")
        else:
            raise HTTPException(status_code=400, detail="id_token 또는 access_token 필요")
        uid = db.get_or_create_user("google", pid, email, name)
        token = jwt.encode({"user_id": uid}, JWT_SECRET, algorithm="HS256")
        credits = db.get_user_credits(uid)
        return {"token": token, "user": {"id": uid, "name": name or email or "구글사용자", "credits": credits}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"구글 토큰 검증 실패: {str(e)}")

@app.post("/api/auth/test")
def auth_test(req: TestAuthRequest):
    """개발용 테스트 로그인 (OAuth 키 없이 사용 가능)."""
    uid = db.get_or_create_user("test", f"test_{req.name}", "", req.name)
    token = jwt.encode({"user_id": uid}, JWT_SECRET, algorithm="HS256")
    credits = db.get_user_credits(uid)
    return {"token": token, "user": {"id": uid, "name": req.name, "credits": credits}}

@app.get("/api/auth/me")
def auth_me(user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        return {"logged_in": False}
    u = db.get_user_by_id(user_id)
    if not u:
        return {"logged_in": False}
    credits = db.get_user_credits(user_id)
    return {"logged_in": True, "user": {"id": user_id, "name": u.get("name") or u.get("email") or "사용자", "credits": credits}}

@app.get("/api/credits")
def get_credits(user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        return {"credits": 0}
    return {"credits": db.get_user_credits(user_id)}

class AddCreditsRequest(BaseModel):
    imp_uid: Optional[str] = None  # 포트원 결제 식별자 (프로덕션 필수)
    plan: Optional[str] = None
    amount: Optional[int] = None
    credits_added: Optional[int] = None

def _verify_portone_and_get_credits(imp_uid: str) -> Tuple[int, int, str]:
    """포트원 결제 검증 후 (amount, credits_added, plan) 반환. 실패 시 HTTPException."""
    try:
        token_req = requests.post(
            "https://api.iamport.kr/users/getToken",
            json={"imp_key": PORTONE_API_KEY, "imp_secret": PORTONE_API_SECRET},
            timeout=10
        )
        token_data = token_req.json()
        if token_data.get("code") != 0:
            raise HTTPException(status_code=401, detail="포트원 인증 토큰 발급 실패")
        access_token = token_data["response"]["access_token"]
        payment_req = requests.get(
            f"https://api.iamport.kr/payments/{imp_uid}",
            headers={"Authorization": access_token},
            timeout=10
        )
        payment_data = payment_req.json()
        if payment_data.get("code") != 0:
            raise HTTPException(status_code=400, detail="결제 내역 조회 실패")
        info = payment_data["response"]
        if info.get("status") != "paid":
            raise HTTPException(status_code=400, detail="결제가 완료되지 않았습니다.")
        amount = int(info.get("amount", 0))
        if amount == 7000:
            return amount, 1, "1"
        if amount == 30000:
            return amount, 5, "5"
        raise HTTPException(status_code=400, detail="결제 금액이 허용된 값과 일치하지 않습니다.")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail="결제 검증 서버 통신 실패")

@app.post("/api/credits/add")
def add_credits_api(req: AddCreditsRequest, user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if req.imp_uid:
        amount, credits_added, plan = _verify_portone_and_get_credits(req.imp_uid)
    elif BLUEDOT_TEST_MODE and req.plan and req.amount is not None and req.credits_added is not None:
        if req.amount not in (7000, 30000):
            raise HTTPException(status_code=400, detail="잘못된 결제 금액입니다.")
        amount, credits_added, plan = req.amount, req.credits_added, req.plan
    else:
        raise HTTPException(status_code=400, detail="결제 검증이 필요합니다. imp_uid를 전달하거나, 테스트 모드에서 plan/amount/credits_added를 사용하세요.")
    db.add_payment(user_id, amount, plan, credits_added)
    return {"credits": db.get_user_credits(user_id)}

class UseCreditRequest(BaseModel):
    pass

@app.post("/api/credits/use")
def use_credit_api(user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    ok = db.use_credit(user_id)
    if not ok:
        raise HTTPException(status_code=400, detail="사용 가능한 분석 횟수가 없습니다.")
    return {"credits": db.get_user_credits(user_id)}

class ReportSaveRequest(BaseModel):
    region_name: str = ""
    dept_name: str = ""
    report_data: dict = {}

@app.post("/api/reports/save")
def save_report(req: ReportSaveRequest, user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    data = req.report_data or {}
    data["region_name"] = req.region_name
    data["dept_name"] = req.dept_name
    rid = db.save_report(user_id, data, req.region_name, req.dept_name)
    return {"id": rid}

@app.get("/api/reports")
def list_reports(user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        return {"reports": []}
    return {"reports": db.get_reports(user_id)}

@app.get("/api/reports/{report_id}")
def get_report(report_id: int, user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    r = db.get_report(user_id, report_id)
    if not r:
        raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다.")
    return r

@app.get("/api/payments")
def list_payments(user_id: Optional[int] = Depends(_get_current_user_id)):
    if not user_id:
        return {"payments": []}
    return {"payments": db.get_payments(user_id)}


@app.get("/api/hospitals-nearby")
def get_hospitals_nearby(lat: float, lng: float, dept: str, radius: int = 1):
    """지도 호버 시 해당 좌표 반경 내 경쟁 의료기관만 반환 (결과 화면에서 사용)."""
    real_hosps, _ = fetch_real_hospitals(lat, lng, radius, dept)
    return {"hospitals": real_hosps}


@app.get("/api/micro-site")
def api_micro_site(lat: float, lng: float, radius_m: int = 400, dept: str = "한의원"):
    """
    미시 입지 MVP: 클릭 지점 반경(m) 내 앵커 프랜차이즈(카카오 키워드) + HIRA 경쟁 + 마스터 CSV 거시 프록시.
    """
    radius_m = int(max(100, min(radius_m, 2000)))
    r_km = max(0.3, min(radius_m / 1000.0, 2.0))
    comps, _ = fetch_real_hospitals(lat, lng, r_km, dept)
    master_ctx = None
    if df_master is not None and not df_master.empty:
        master_ctx = resolve_nearest_master_context(df_master, lat, lng, radius_km=max(2.0, r_km * 3))
    return build_micro_site_payload(
        lat=lat,
        lng=lng,
        radius_m=radius_m,
        dept=dept,
        competitors=comps or [],
        kakao_key=KAKAO_REST_KEY or "",
        master_ctx=master_ctx,
    )


@app.post("/api/micro-site/stage2")
def api_micro_site_stage2(body: MicroSiteStage2Request):
    """
    2단계: 1차에서 뽑은 최대 5개 권역 각각에 대해
    중심+8방 125m 오프셋 후보(9점)를 두고, 권역 단위 카카오 앵커·HIRA 목록을 재사용해 미시 점수 산출 후 전역 상위 5곳 반환.
    """
    if not body.nodes:
        raise HTTPException(status_code=400, detail="nodes가 비어 있습니다. 1단계 분석 결과를 보내 주세요.")
    eval_r = int(max(100, min(int(body.radius_m), 1200)))
    dept = (body.department or "한의원").strip() or "한의원"
    all_cands: List[Dict[str, Any]] = []
    nodes_in = list(body.nodes)[:5]
    kk = KAKAO_REST_KEY or ""
    nw = _stage2_region_worker_count(len(nodes_in))
    if nw <= 1:
        for node in nodes_in:
            all_cands.extend(_stage2_region_cands(node, eval_r, dept, kk))
    else:
        with ThreadPoolExecutor(max_workers=nw) as _st2_pool:
            for chunk in _st2_pool.map(
                lambda n: _stage2_region_cands(n, eval_r, dept, kk),
                nodes_in,
            ):
                all_cands.extend(chunk)
    # 근접 후보도 허용(지도·리스트에서 겹칠 수 있음). 완전 동일 좌표만 소간격으로 배제.
    top5 = dedupe_pick_top(all_cands, top_k=5, min_sep_m=12.0)
    enrich_stage2_top_with_rationale(top5)
    return {
        "status": "success",
        "department": dept,
        "eval_radius_m": eval_r,
        "regions_used": len(nodes_in),
        "candidates_evaluated": len(all_cands),
        "top_buildings": top5,
        "method": "per_region_9_grid_reuse_anchors_hira",
        "disclaimer": "건물 폴리곤이 아닌 '후보 좌표' 기준 추정입니다. 현장 확인이 필요합니다.",
    }


@app.get("/api/building-aging")
def api_building_aging(lat: float, lng: float, dept: str = "한의원", radius_km: float = 1.0, limit: int = 8):
    """
    (비동기 로딩용) 경쟁기관 건물 노후화 리포트.
    - 분석 API를 느리게 만들지 않기 위해 별도 엔드포인트로 분리.
    - 내부적으로 주소 변환(카카오→JUSO→정규식) + 건축물대장 조회(3초 룰) + DB 캐시를 사용.
    """
    real_hosps, _ = fetch_real_hospitals(lat, lng, int(max(1, radius_km)), dept)
    competitors = _competitors_for_building_aging(real_hosps, limit=limit)
    # 전용 엔드포인트: HIRA 이후 건축HUB 다건 호출 허용(프론트 타임아웃 90초와 맞춤)
    report = generate_aging_report(competitors, sleep_sec=0.05, max_total_sec=28.0)
    return {"status": "success", "report": report, "competitor_count": len(real_hosps or [])}


# 상위 N개 노드: HIRA 병렬 처리(건물 노후화는 /api/building-aging 에서만). 워커 수는 BLUEDOT_ANALYZE_HIRA_WORKERS


@app.get("/api/hospitals")
def get_analysis_data(lat: float, lng: float, dept: str, radius: int = 1, walk_minutes: int = 10):
    # 임시: 로그인/크레딧 없이 분석 허용 (프로덕션 재적용 시 Depends(_require_auth_and_use_credit) 복구)
    analyzed_nodes = []
    all_hospitals = []
    
    if df_master is not None and not df_master.empty:
        try:
            df = df_master.copy()
            
            # 🚀 [핀셋 수술 1] 어떤 컬럼명이든 완벽하게 잡아내는 초정밀 매핑기
            def find_col(keywords):
                for c in df.columns:
                    c_clean = str(c).lower().replace(" ", "")
                    if any(k in c_clean for k in keywords): return c
                return None

            col_pop = find_col(['총인구', '인구수', 'pop'])
            col_name = find_col(['행정구역', '행정동', '읍면동', '동이름'])
            col_lat = find_col(['center_lat', '위도', 'lat', 'y좌표', 'ypos'])
            col_lng = find_col(['center_lng', '경도', 'lng', 'lon', 'x좌표', 'xpos'])
            col_young = find_col(['젊은', '2030', '청년'])

            if not col_lat or not col_lng:
                return {"status": "error", "message": f"🚨 CSV 파일에서 위도/경도 컬럼을 찾을 수 없습니다. (현재 컬럼: {', '.join(df.columns[:5])})"}

            df['총인구 (명)'] = pd.to_numeric(df[col_pop], errors='coerce').fillna(0) if col_pop else 0
            df['행정구역(동읍면)별'] = df[col_name].astype(str) if col_name else "이름 미상 동네"
            df['젊은층_비중'] = pd.to_numeric(df[col_young], errors='coerce').fillna(0).clip(0, 1) if col_young else 0
            df['고령층_비중'] = (1.0 - df['젊은층_비중']).clip(0, 1)
            df['center_lat'] = pd.to_numeric(df[col_lat], errors='coerce').fillna(999.0)
            df['center_lng'] = pd.to_numeric(df[col_lng], errors='coerce').fillna(999.0)
            df['hosp_count'] = pd.to_numeric(df.get('hosp_count', 0), errors='coerce').fillna(0)
            df['total_doctors'] = pd.to_numeric(df.get('total_doctors', 0), errors='coerce').fillna(0)
            df['subway_count'] = pd.to_numeric(df.get('subway_count', 0), errors='coerce').fillna(0)
            df['bus_stop_count'] = pd.to_numeric(df.get('bus_stop_count', 0), errors='coerce').fillna(0)

            # 🚀 [추가] V6 마스터 데이터의 상권 인프라 컬럼 로드
            df['anchor_cnt'] = pd.to_numeric(df.get('anchor_cnt', 0), errors='coerce').fillna(0)
            df['pharmacy_cnt'] = pd.to_numeric(df.get('pharmacy_cnt', 0), errors='coerce').fillna(0)
            df['academy_cnt'] = pd.to_numeric(df.get('academy_cnt', 0), errors='coerce').fillna(0)
            df['fitness_cnt'] = pd.to_numeric(df.get('fitness_cnt', 0), errors='coerce').fillna(0)

            # 중복 제거 (청운효자동 도배 차단)
            df = df.drop_duplicates(subset=['행정구역(동읍면)별'])

            # 🚨 [핀셋 수술 2] 철통 지오펜싱: 바다 찍었는데 수영구/부암동 안 나오게 딱 자름!
            df['distance_km'] = haversine_distance_vectorized(lat, lng, df['center_lat'], df['center_lng'])
            search_limit = max(float(radius) * 1.5, 1.5)
            df_filtered = df[(df['distance_km'] <= search_limit)]

            if df_filtered.empty:
                # 억지로 서울 데이터를 긁어오지 않고 확실하게 에러 반환!
                return {"status": "error", "message": f"선택하신 위치 반경 {radius}km 내에 행정동 데이터가 없습니다. 지도를 내륙으로 이동해주세요."}
            
            df = df_filtered
            df, phase2_meta = _apply_phase2_walkable_filter(df, lat, lng, dept, walk_minutes=walk_minutes)
            if df.empty:
                return {"status": "error", "message": "도보권(또는 폴백 반경) 내 분석 가능한 행정동 데이터가 없습니다."}

            # =======================================================
            # 🚀 신뢰성 개선: 진료과목별 docs 컬럼 우선 사용 (docs_치과, docs_소아과 등)
            # =======================================================
            df['pop_in_10k'] = df['총인구 (명)'] / 10000
            dept_docs_col = f'docs_{dept}'
            if dept_docs_col in df.columns:
                df['dept_doctors'] = pd.to_numeric(df[dept_docs_col], errors='coerce').fillna(0)
            elif dept_docs_col + '_x' in df.columns:  # 한의원_x, 한의원_y 병합 케이스
                df['dept_doctors'] = pd.to_numeric(df.get(dept_docs_col + '_x', 0), errors='coerce').fillna(0) + pd.to_numeric(df.get(dept_docs_col + '_y', 0), errors='coerce').fillna(0)
            else:
                df['dept_doctors'] = df['total_doctors']  # 폴백
            df['docs_per_10k'] = _docs_per_10k_column(df)

            if dept == "치과":
                df['age_score'] = (df['젊은층_비중'] / 0.35) * 20.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 15.0
                df['transit_score'] = (df['subway_count'] * 5.0) + (df['anchor_cnt'] * 1.5)
                df['comp_penalty'] = ((df['docs_per_10k'] / 2.0) ** 2) * 5.0
                df['final_raw'] = 30.0 + df['age_score'].clip(upper=25.0) + df['pop_score'].clip(upper=20.0) + df['transit_score'].clip(upper=20.0) - df['comp_penalty'].clip(upper=35.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=3.5, upper=9.8)
                df.loc[df['docs_per_10k'] >= 5.0, 'final_score'] = df['final_score'].clip(upper=6.8)
            elif dept == "소아과":
                df['age_score'] = (df['젊은층_비중'] / 0.35) * 35.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 20.0
                df['transit_score'] = (df['subway_count'] * 2.0) + (df['academy_cnt'] * 0.5)
                df['comp_penalty'] = ((df['docs_per_10k'] / 1.5) ** 2) * 5.0
                df['final_raw'] = 20.0 + df['age_score'].clip(upper=40.0) + df['pop_score'].clip(upper=25.0) + df['transit_score'].clip(upper=10.0) - df['comp_penalty'].clip(upper=35.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=3.0, upper=9.8)
            elif dept in ["내과", "이비인후과"]:
                df['age_score'] = 15.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 35.0
                df['transit_score'] = (df['bus_stop_count'] * 1.0) + (df['pharmacy_cnt'] * 2.0)
                df['comp_penalty'] = ((df['docs_per_10k'] / 2.5) ** 2) * 8.0
                df['final_raw'] = 20.0 + df['age_score'] + df['pop_score'].clip(upper=40.0) + df['transit_score'].clip(upper=15.0) - df['comp_penalty'].clip(upper=40.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=3.5, upper=9.8)
                df.loc[df['docs_per_10k'] >= 6.0, 'final_score'] = df['final_score'].clip(upper=5.5)
            elif dept == "피부과":
                df['age_score'] = (df['젊은층_비중'] / 0.35) * 20.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 5.0
                df['transit_score'] = (df['subway_count'] * 10.0) + (df['anchor_cnt'] * 2.0) + (df['fitness_cnt'] * 1.0)
                df['comp_penalty'] = ((df['docs_per_10k'] / 3.0) ** 2) * 4.0
                df['final_raw'] = 35.0 + df['age_score'].clip(upper=25.0) + df['pop_score'].clip(upper=10.0) + df['transit_score'].clip(upper=35.0) - df['comp_penalty'].clip(upper=25.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=4.0, upper=9.9)
            else: # 한의원 2가지 컨셉 완벽 이식
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 15.0
                df['comp_penalty'] = ((df['docs_per_10k'] / 2.0) ** 2) * 5.0
                df['age_A'] = (df['고령층_비중'] / 0.65) * 25.0
                df['transit_A'] = (df['bus_stop_count'] * 0.8 + df['pharmacy_cnt'] * 1.5)
                df['score_A_raw'] = 30.0 + df['age_A'].clip(upper=30.0) + df['pop_score'].clip(upper=20.0) + df['transit_A'].clip(upper=15.0) - df['comp_penalty'].clip(upper=35.0)
                df['score_A'] = ((df['score_A_raw'] / 100) * 10).clip(lower=3.0, upper=9.8)
                df['age_B'] = (df['젊은층_비중'] / 0.35) * 25.0
                df['transit_B'] = (df['subway_count'] * 5.0 + df['fitness_cnt'] * 2.0)
                df['score_B_raw'] = 30.0 + df['age_B'].clip(upper=30.0) + df['pop_score'].clip(upper=20.0) + df['transit_B'].clip(upper=15.0) - df['comp_penalty'].clip(upper=35.0)
                df['score_B'] = ((df['score_B_raw'] / 100) * 10).clip(lower=3.0, upper=9.8)
                df['final_score'] = df[['score_A', 'score_B']].max(axis=1)
                df['best_type'] = np.where(
                    df['score_A'] >= df['score_B'],
                    "타입A(전통/통증)",
                    "타입B(미용/다이어트)",
                )

            top_5_df = df.sort_values(by='final_score', ascending=False).head(5)

            def _work_hospital_row(row: dict) -> Tuple[dict, list]:
                f_score = float(round(row['final_score'], 1))
                doc_ratio = float(row['docs_per_10k'])
                subway = int(row['subway_count'])
                bus = int(row['bus_stop_count'])
                anchor = int(row.get('anchor_cnt', 0))
                academy = int(row.get('academy_cnt', 0))
                pharmacy = int(row.get('pharmacy_cnt', 0))
                dist = float(row['distance_km'])
                node_lat = float(row['center_lat'])
                node_lng = float(row['center_lng'])
                
                if doc_ratio >= 5.0: status = "🚨 극도 포화"
                elif doc_ratio >= 3.0: status = "⚠️ 경쟁 심화"
                elif doc_ratio >= 1.5: status = "🟢 보통 (안정)"
                else: status = "💎 블루오션"

                color = "#EF4444" if f_score < 6.0 else "#3B82F6" if f_score < 8.0 else "#10B981"
                
                # 🚀 [추가] 상권 인프라를 활용한 임대료/소비력 추정
                activity_index = anchor + subway * 3
                estimated_rent_per_pyeong = 50000 + (activity_index * 8000)
                estimated_spending = 30000 + (activity_index * 1500) + (row['젊은층_비중'] * 20000)

                # 팩트 기반 데이터 텍스트 출력
                if dept == "소아과":
                    insight = f"🧸 [소아과 특화] 검색 반경 {dist:.1f}km 내 상권. 영유아 타겟 배후 세대({int(row['총인구 (명)']):,}명)와 학원/교습소({academy}개)가 밀집되어 시너지가 매우 높습니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                elif dept in ["내과", "이비인후과"]:
                    insight = f"🩺 [{dept} 특화] 검색 반경 {dist:.1f}km 내 상권. 배후 인구({int(row['총인구 (명)']):,}명)와 주변 약국({pharmacy}개)이 분포하여 처방전 수요가 탄탄합니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                elif dept == "피부과":
                    insight = f"✨ [피부과 특화] 검색 반경 {dist:.1f}km 내 상권. 지하철역({subway}개)과 주요 앵커 테넌트({anchor}개)가 밀집해 비급여 타겟 유동인구 노출이 극대화됩니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                elif dept == "치과":
                    insight = f"🦷 [치과 특화] 검색 반경 {dist:.1f}km 내 상권. 지하철역({subway}개) 및 앵커 상권({anchor}개)이 위치하여 직장인 집객에 유리합니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                else:
                    score_a, score_b, best = row.get('score_A', 0), row.get('score_B', 0), row.get('best_type', '타입A')
                    insight = f"🌿 [한의원 컨셉 분석] 검색 반경 {dist:.1f}km. 전통/통증 적합도: {score_a:.1f}점 | 다이어트/미용 적합도: {score_b:.1f}점. 👉 [{best}] 컨셉 개원이 유리합니다."
                    age_val = row['age_A'] if best == "타입A(전통/통증)" else row['age_B']
                    transit_val = row['transit_A'] if best == "타입A(전통/통증)" else row['transit_B']

                _node = {
                    "name": str(row['행정구역(동읍면)별']),
                    "lat": node_lat,
                    "lng": node_lng,
                    "score_val": f_score,
                    "score": f"{f_score}/10",
                    "comp_text": f"{status} (기관 {int(row['hosp_count'])}개)",
                    "pop_text": f"{int(row['총인구 (명)']):,}명 (3040비중 {float(row['젊은층_비중'])*100:.1f}%)",
                    "insight": insight,
                    "color": color,
                    "premium_data": {
                        "rent": estimated_rent_per_pyeong,
                        "spending": estimated_spending
                    },
                    "formula": {
                        "age_score": f"+{min(35.0, age_val):.1f} (타겟연령)",
                        "revenue_score": f"+{min(35.0, row.get('pop_score', 15.0)):.1f} (배후인구)",
                        "anchor_score": f"+{min(25.0, transit_val):.1f} (교통접근성)",
                        "risk_penalty": "20.0 (기본)",
                        "comp_penalty": f"-{float(row['comp_penalty']):.1f}",
                        "final_score": f"{f_score}"
                    },
                    "phase2": {
                        "used_fallback": bool(phase2_meta.get("used_fallback")),
                        "postgis_skipped": bool(phase2_meta.get("postgis_skipped")),
                        "walk_minutes": walk_minutes,
                        "walk_filter_applied": True,
                    },
                }
                real_hosps, _ = fetch_real_hospitals(node_lat, node_lng, 1, dept)
                _node["nearby_hospitals"] = real_hosps
                _node["killer_insights"] = build_node_killer_insights(real_hosps, row, node_lat, node_lng)
                # 건물 노후화는 HIRA·건축HUB 다건이라 메인 분석을 수 분씩 잡아먹음 → /api/building-aging 로 모달에서만 로드
                _node["building_aging_report"] = None
                if _include_car_insurance_insight_for_dept(dept):
                    try:
                        _node["car_insurance_insight"] = build_car_insurance_insight_for_region(
                            str(row["행정구역(동읍면)별"])
                        )
                    except Exception as _car_e:
                        _node["car_insurance_insight"] = {
                            "ok": False,
                            "narrative": f"자동차보험 진료건수 모듈 오류: {_car_e}. `pip install openpyxl` 후 서버를 재시작해 주세요.",
                            "source_file": "data/car2024.xlsx",
                        }
                else:
                    _node["car_insurance_insight"] = None
                attach_consulting_extensions(
                    _node, row, dept, doc_ratio, estimated_rent_per_pyeong, estimated_spending,
                    status=status, f_score=f_score
                )
                return _node, real_hosps

            with ThreadPoolExecutor(max_workers=_analyze_top_nodes_workers()) as _pool_h:
                _pairs_h = list(_pool_h.map(_work_hospital_row, top_5_df.to_dict("records")))
            for _node_h, _rh in _pairs_h:
                all_hospitals.extend(_rh)
                analyzed_nodes.append(_node_h)

            # CSV 데이터가 정상 처리되었으면 즉시 반환!
            ranked_nodes = sorted(analyzed_nodes, key=lambda x: x["score_val"], reverse=True)
            for index, n in enumerate(ranked_nodes): n["rank"] = index + 1
            return {
                "status": "success", "department": dept, "search_radius": radius,
                "hospitals": all_hospitals, "recommendations": ranked_nodes,
                "phase2": phase2_meta,
            }
        except Exception as e:
            print(f"🚨 하단 버튼 연산 중 에러 발생: {e}")
            return {"status": "error", "message": f"서버 연산 에러: {e}"}

    # 🚨 [핀셋 수정 3] 남측/북측 가짜 데이터를 만들던 offset 백업 로직 완전 영구 삭제
    return {"status": "error", "message": "서버에 상권 마스터 데이터(CSV)가 존재하지 않습니다."}

# =========================================================
# [3] 🚀 NEW V3: 전 과목 멀티 알고리즘 AI 검색 엔진 (유연한 반경 지원)
# =========================================================
@app.get("/api/ai-search")
def ai_search(lat: float, lng: float, prompt: str, radius: int = 3, walk_minutes: int = 10):
    # 임시: 로그인/크레딧 없이 분석 허용
    print(f"\n🧠 [AI ENGINE] 사용자 프롬프트: '{prompt}' (기준 좌표: {lat}, {lng} | 탐색 반경: {radius}km)")
    
    dept_name = "한의원" # 기본값
    if "치과" in prompt: dept_name = "치과"
    elif "소아과" in prompt or "소아청소년과" in prompt: dept_name = "소아과"
    elif "이비인후과" in prompt: dept_name = "이비인후과"
    elif "내과" in prompt: dept_name = "내과"
    elif "피부과" in prompt: dept_name = "피부과"

    region_pattern, region_display = extract_region_from_prompt(prompt)
    use_region_filter = region_pattern is not None
    if use_region_filter:
        print(f"📍 [AI ENGINE] 지역 키워드 감지: '{region_display}' → 행정구역 '{region_pattern}' 필터 적용")
    
    want_young = any(k in prompt for k in ["젊은", "청년", "2030", "20대", "30대", "청년층", "MZ"])
    want_low_comp = any(k in prompt for k in ["경쟁 적", "경쟁적", "경쟁 없", "블루오션", "경쟁 적은", "경쟁이 적"])

    analyzed_nodes = []
    all_hospitals = []
    map_center = {"lat": lat, "lng": lng}

    if df_master is not None and not df_master.empty:
        try:
            df = df_master.copy()
            
            def find_col(keywords):
                for c in df.columns:
                    c_clean = str(c).lower().replace(" ", "")
                    if any(k in c_clean for k in keywords): return c
                return None

            col_pop = find_col(['총인구', '인구수', 'pop'])
            col_name = find_col(['행정구역', '행정동', '읍면동', '동이름'])
            col_lat = find_col(['center_lat', '위도', 'lat', 'y좌표', 'ypos'])
            col_lng = find_col(['center_lng', '경도', 'lng', 'lon', 'x좌표', 'xpos'])
            col_young = find_col(['젊은', '2030', '청년'])

            if not col_lat or not col_lng:
                return {"status": "error", "message": f"🚨 CSV 파일에서 위도/경도 컬럼을 찾을 수 없습니다. (현재 컬럼: {', '.join(df.columns[:5])})"}

            df['총인구 (명)'] = pd.to_numeric(df[col_pop], errors='coerce').fillna(0) if col_pop else 0
            df['행정구역(동읍면)별'] = df[col_name].astype(str) if col_name else "이름 미상 동네"
            df['젊은층_비중'] = pd.to_numeric(df[col_young], errors='coerce').fillna(0).clip(0, 1) if col_young else 0
            df['고령층_비중'] = (1.0 - df['젊은층_비중']).clip(0, 1)
            df['center_lat'] = pd.to_numeric(df[col_lat], errors='coerce').fillna(999.0)
            df['center_lng'] = pd.to_numeric(df[col_lng], errors='coerce').fillna(999.0)
            df['hosp_count'] = pd.to_numeric(df.get('hosp_count', 0), errors='coerce').fillna(0)
            df['total_doctors'] = pd.to_numeric(df.get('total_doctors', 0), errors='coerce').fillna(0)
            df['subway_count'] = pd.to_numeric(df.get('subway_count', 0), errors='coerce').fillna(0)
            df['bus_stop_count'] = pd.to_numeric(df.get('bus_stop_count', 0), errors='coerce').fillna(0)

            # 🚀 [추가] V6 마스터 데이터의 상권 인프라 컬럼 로드
            df['anchor_cnt'] = pd.to_numeric(df.get('anchor_cnt', 0), errors='coerce').fillna(0)
            df['pharmacy_cnt'] = pd.to_numeric(df.get('pharmacy_cnt', 0), errors='coerce').fillna(0)
            df['academy_cnt'] = pd.to_numeric(df.get('academy_cnt', 0), errors='coerce').fillna(0)
            df['fitness_cnt'] = pd.to_numeric(df.get('fitness_cnt', 0), errors='coerce').fillna(0)

            df = df.drop_duplicates(subset=['행정구역(동읍면)별'])

            df['distance_km'] = haversine_distance_vectorized(lat, lng, df['center_lat'], df['center_lng'])
            
            if use_region_filter:
                df_filtered = df[df['행정구역(동읍면)별'].astype(str).str.contains(region_pattern, na=False)]
                if df_filtered.empty:
                    return {"status": "error", "message": f"'{region_display}' 지역에 해당하는 행정동 데이터가 없습니다. 다른 지역명을 입력해 주세요."}
                df = df_filtered
                cent_lat = float(df['center_lat'].mean())
                cent_lng = float(df['center_lng'].mean())
                map_center = {"lat": cent_lat, "lng": cent_lng}
            else:
                search_limit = max(float(radius) * 1.2, 1.5)
                df_filtered = df[(df['distance_km'] <= search_limit)]
                if df_filtered.empty:
                    return {"status": "error", "message": f"현재 위치 반경 {radius}km 내에 분석할 행정동 데이터가 없습니다. 반경을 넓히거나 내륙으로 이동해주세요."}
                df = df_filtered

            df, phase2_meta = _apply_phase2_walkable_filter(df, lat, lng, dept_name, walk_minutes=walk_minutes)
            if df.empty:
                return {"status": "error", "message": "도보권(또는 폴백 반경) 내 분석 가능한 행정동 데이터가 없습니다."}

            # =======================================================
            # 🚀 신뢰성 개선: 진료과목별 docs 컬럼 우선 사용
            # =======================================================
            df['pop_in_10k'] = df['총인구 (명)'] / 10000
            dept_docs_col = f'docs_{dept_name}'
            if dept_docs_col in df.columns:
                df['dept_doctors'] = pd.to_numeric(df[dept_docs_col], errors='coerce').fillna(0)
            elif dept_docs_col + '_x' in df.columns:
                df['dept_doctors'] = pd.to_numeric(df.get(dept_docs_col + '_x', 0), errors='coerce').fillna(0) + pd.to_numeric(df.get(dept_docs_col + '_y', 0), errors='coerce').fillna(0)
            else:
                df['dept_doctors'] = df['total_doctors']
            df['docs_per_10k'] = _docs_per_10k_column(df)

            # =======================================================
            # 🚀 [로직 분기] 과목별 완벽하게 찢어진 5가지 맞춤형 스코어링 (원본 그대로 유지)
            # =======================================================
            if dept_name == "치과":
                df['age_score'] = (df['젊은층_비중'] / 0.35) * 20.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 15.0
                df['transit_score'] = (df['subway_count'] * 5.0) + (df['anchor_cnt'] * 1.5)
                df['comp_penalty'] = ((df['docs_per_10k'] / 2.0) ** 2) * 5.0
                
                df['final_raw'] = 30.0 + df['age_score'].clip(upper=25.0) + df['pop_score'].clip(upper=20.0) + df['transit_score'].clip(upper=20.0) - df['comp_penalty'].clip(upper=35.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=3.5, upper=9.8)
                df.loc[df['docs_per_10k'] >= 5.0, 'final_score'] = df['final_score'].clip(upper=6.8)

            elif dept_name == "소아과":
                df['age_score'] = (df['젊은층_비중'] / 0.35) * 35.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 20.0
                df['transit_score'] = (df['subway_count'] * 2.0) + (df['academy_cnt'] * 0.5)
                df['comp_penalty'] = ((df['docs_per_10k'] / 1.5) ** 2) * 5.0
                
                df['final_raw'] = 20.0 + df['age_score'].clip(upper=40.0) + df['pop_score'].clip(upper=25.0) + df['transit_score'].clip(upper=10.0) - df['comp_penalty'].clip(upper=35.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=3.0, upper=9.8)

            elif dept_name in ["내과", "이비인후과"]:
                df['age_score'] = 15.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 35.0
                df['transit_score'] = (df['bus_stop_count'] * 1.0) + (df['pharmacy_cnt'] * 2.0)
                df['comp_penalty'] = ((df['docs_per_10k'] / 2.5) ** 2) * 8.0
                
                df['final_raw'] = 20.0 + df['age_score'] + df['pop_score'].clip(upper=40.0) + df['transit_score'].clip(upper=15.0) - df['comp_penalty'].clip(upper=40.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=3.5, upper=9.8)
                df.loc[df['docs_per_10k'] >= 6.0, 'final_score'] = df['final_score'].clip(upper=5.5)

            elif dept_name == "피부과":
                df['age_score'] = (df['젊은층_비중'] / 0.35) * 20.0
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 5.0
                df['transit_score'] = (df['subway_count'] * 10.0) + (df['anchor_cnt'] * 2.0) + (df['fitness_cnt'] * 1.0)
                df['comp_penalty'] = ((df['docs_per_10k'] / 3.0) ** 2) * 4.0
                
                df['final_raw'] = 35.0 + df['age_score'].clip(upper=25.0) + df['pop_score'].clip(upper=10.0) + df['transit_score'].clip(upper=35.0) - df['comp_penalty'].clip(upper=25.0)
                df['final_score'] = ((df['final_raw'] / 100) * 10).clip(lower=4.0, upper=9.9)

            else:
                df['pop_score'] = (df['총인구 (명)'] / 50000) * 15.0
                comp_mult = 8.0 if want_low_comp else 5.0
                comp_div = 1.5 if want_low_comp else 2.0
                df['comp_penalty'] = ((df['docs_per_10k'] / comp_div) ** 2) * comp_mult

                df['age_A'] = (df['고령층_비중'] / 0.65) * 25.0
                df['transit_A'] = (df['bus_stop_count'] * 0.8 + df['pharmacy_cnt'] * 1.5)
                df['score_A_raw'] = 30.0 + df['age_A'].clip(upper=30.0) + df['pop_score'].clip(upper=20.0) + df['transit_A'].clip(upper=15.0) - df['comp_penalty'].clip(upper=35.0)
                df['score_A'] = ((df['score_A_raw'] / 100) * 10).clip(lower=3.0, upper=9.8)

                age_B_mult = 35.0 if want_young else 25.0
                df['age_B'] = (df['젊은층_비중'] / 0.35) * age_B_mult
                df['transit_B'] = (df['subway_count'] * 5.0 + df['fitness_cnt'] * 2.0)
                df['score_B_raw'] = 30.0 + df['age_B'].clip(upper=35.0 if want_young else 30.0) + df['pop_score'].clip(upper=20.0) + df['transit_B'].clip(upper=15.0) - df['comp_penalty'].clip(upper=35.0)
                df['score_B'] = ((df['score_B_raw'] / 100) * 10).clip(lower=3.0, upper=9.8)

                df['final_score'] = df[['score_A', 'score_B']].max(axis=1)
                df['best_type'] = np.where(
                    df['score_A'] >= df['score_B'],
                    "타입A(전통/통증)",
                    "타입B(미용/다이어트)",
                )

            top_5_df = df.sort_values(by='final_score', ascending=False).head(5)

            def _work_ai_row(row: dict) -> Tuple[dict, list]:
                f_score = float(round(row['final_score'], 1))
                doc_ratio = float(row['docs_per_10k'])
                subway = int(row['subway_count'])
                bus = int(row['bus_stop_count'])
                anchor = int(row.get('anchor_cnt', 0))
                academy = int(row.get('academy_cnt', 0))
                pharmacy = int(row.get('pharmacy_cnt', 0))
                dist = float(row['distance_km'])
                node_lat = float(row['center_lat'])
                node_lng = float(row['center_lng'])
                
                if doc_ratio >= 5.0: status = "🚨 극도 포화"
                elif doc_ratio >= 3.0: status = "⚠️ 경쟁 심화"
                elif doc_ratio >= 1.5: status = "🟢 보통 (안정)"
                else: status = "💎 블루오션"

                color = "#EF4444" if f_score < 6.0 else "#3B82F6" if f_score < 8.0 else "#10B981"
                
                # 🚀 [추가] 상권 인프라를 활용한 임대료/소비력 추정 로직
                activity_index = anchor + subway * 3
                estimated_rent_per_pyeong = 50000 + (activity_index * 8000)
                estimated_spending = 30000 + (activity_index * 1500) + (row['젊은층_비중'] * 20000)

                scope_txt = f"{region_display} 내" if use_region_filter else f"검색 반경 {dist:.1f}km"
                if dept_name == "소아과":
                    insight = f"🧸 [소아과 특화] {scope_txt} 상권. 영유아 타겟 배후 세대({int(row['총인구 (명)']):,}명)와 학원/교습소({academy}개)가 밀집되어 시너지가 매우 높습니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                elif dept_name in ["내과", "이비인후과"]:
                    insight = f"🩺 [{dept_name} 특화] {scope_txt} 상권. 배후 인구({int(row['총인구 (명)']):,}명)와 동네 메인 약국({pharmacy}개)이 분포하여 처방전 수요가 탄탄합니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                elif dept_name == "피부과":
                    insight = f"✨ [피부과 특화] {scope_txt} 상권. 지하철역({subway}개)과 주요 앵커 테넌트({anchor}개)가 밀집해 비급여 타겟 유동인구 노출이 극대화됩니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                elif dept_name == "치과":
                    insight = f"🦷 [치과 특화] {scope_txt} 상권. 지하철역({subway}개) 및 앵커 상권({anchor}개)이 위치하여 직장인 집객에 유리합니다."
                    age_val, transit_val = row['age_score'], row['transit_score']
                else:
                    score_a, score_b, best = row.get('score_A', 0), row.get('score_B', 0), row.get('best_type', '타입A')
                    insight = f"🌿 [한의원 컨셉 분석] {scope_txt}. 전통/통증 적합도: {score_a:.1f}점 | 다이어트/미용 적합도: {score_b:.1f}점. 👉 [{best}] 컨셉 개원이 유리합니다."
                    age_val = row['age_A'] if best == "타입A(전통/통증)" else row['age_B']
                    transit_val = row['transit_A'] if best == "타입A(전통/통증)" else row['transit_B']

                _node = {
                    "name": str(row['행정구역(동읍면)별']),
                    "lat": node_lat,
                    "lng": node_lng,
                    "score_val": f_score,
                    "score": f"{f_score}/10",
                    "comp_text": f"{status} (기관 {int(row['hosp_count'])}개)",
                    "pop_text": f"{int(row['총인구 (명)']):,}명 (3040비중 {float(row['젊은층_비중'])*100:.1f}%)",
                    "insight": insight,
                    "color": color,
                    "premium_data": {
                        "rent": estimated_rent_per_pyeong,
                        "spending": estimated_spending
                    },
                    "formula": {
                        "age_score": f"+{min(35.0, age_val):.1f} (타겟연령)",
                        "revenue_score": f"+{min(35.0, row.get('pop_score', 15.0)):.1f} (배후인구)",
                        "anchor_score": f"+{min(25.0, transit_val):.1f} (교통접근성)",
                        "risk_penalty": "20.0 (기본)",
                        "comp_penalty": f"-{float(row['comp_penalty']):.1f}",
                        "final_score": f"{f_score}"
                    },
                    "phase2": {
                        "used_fallback": bool(phase2_meta.get("used_fallback")),
                        "postgis_skipped": bool(phase2_meta.get("postgis_skipped")),
                        "walk_minutes": walk_minutes,
                        "walk_filter_applied": True,
                    },
                }
                real_hosps, _ = fetch_real_hospitals(node_lat, node_lng, 1, dept_name)
                _node["nearby_hospitals"] = real_hosps
                _node["killer_insights"] = build_node_killer_insights(real_hosps, row, node_lat, node_lng)
                _node["building_aging_report"] = None
                if _include_car_insurance_insight_for_dept(dept_name):
                    try:
                        _node["car_insurance_insight"] = build_car_insurance_insight_for_region(
                            str(row["행정구역(동읍면)별"])
                        )
                    except Exception as _car_e:
                        _node["car_insurance_insight"] = {
                            "ok": False,
                            "narrative": f"자동차보험 진료건수 모듈 오류: {_car_e}. `pip install openpyxl` 후 서버를 재시작해 주세요.",
                            "source_file": "data/car2024.xlsx",
                        }
                else:
                    _node["car_insurance_insight"] = None
                attach_consulting_extensions(
                    _node, row, dept_name, doc_ratio, estimated_rent_per_pyeong, estimated_spending,
                    status=status, f_score=f_score
                )
                return _node, real_hosps

            with ThreadPoolExecutor(max_workers=_analyze_top_nodes_workers()) as _pool_ai:
                _pairs_ai = list(_pool_ai.map(_work_ai_row, top_5_df.to_dict("records")))
            for _node_a, _rha in _pairs_ai:
                all_hospitals.extend(_rha)
                analyzed_nodes.append(_node_a)

        except Exception as e:
            print(f"🚨 [AI ENGINE CRASH] 에러 발생: {e}")
            return {"status": "error", "message": f"서버 연산 에러: {e}"}
    else:
        # 🚨 가짜 백업 로직 완전 삭제
        return {"status": "error", "message": "서버에 상권 마스터 데이터(CSV)가 존재하지 않습니다."}

    # 3. 랭킹 정렬 및 반환
    ranked_nodes = sorted(analyzed_nodes, key=lambda x: x["score_val"], reverse=True)
    recommendations = []
    
    for index, node in enumerate(ranked_nodes):
        node["rank"] = index + 1
        if "color" not in node:
            node["color"] = "#10B981" if node["score_val"] >= 8.5 else "#3B82F6" if node["score_val"] >= 7.0 else "#F59E0B"
        if "score_val" in node:
            del node["score_val"]
        recommendations.append(node)

    return {
        "status": "success",
        "department": f"{dept_name} (맞춤 컨설팅 완료)",
        "search_radius": radius,
        "hospitals": all_hospitals,
        "recommendations": recommendations,
        "phase2": phase2_meta if 'phase2_meta' in locals() else None,
        "map_center": map_center,
        "region_filtered": use_region_filter,
        "region_name": region_display if use_region_filter else None
    }

# =========================================================
# [3.5] Phase 1: AI CFO · 생존율 · 임대 리스크 · 도보 폴리곤 · 페르소나 API
# =========================================================
class BepSimulateRequest(BaseModel):
    lat: float
    lng: float
    dept: str = "한의원"
    radius_km: float = 3.0
    doctors: int = 1
    staff: int = 4
    clinic_pyeong: float = 35.0
    variable_cost_ratio: float = 0.12


class PersonaScoreRequest(BaseModel):
    lat: float
    lng: float
    dept: str = "한의원"
    radius_km: float = 3.0


@app.post("/api/cfo/bep-simulate")
def api_cfo_bep_simulate(req: BepSimulateRequest):
    """직원 수·평수 기반 개원 직후 손익분기(생존 견적). 크레딧 차감 없음(조회용)."""
    if df_master is None or df_master.empty:
        raise HTTPException(status_code=503, detail="마스터 데이터가 없습니다.")
    ctx = resolve_nearest_master_context(df_master, req.lat, req.lng, req.radius_km)
    if not ctx:
        raise HTTPException(
            status_code=404,
            detail="해당 좌표 반경 내 행정동 데이터가 없습니다. 지도를 내륙으로 이동해 주세요.",
        )
    bep = simulate_staff_bep(
        req.dept,
        ctx["estimated_rent_per_pyeong"],
        ctx["estimated_spending_index"],
        doctors=req.doctors,
        staff=req.staff,
        clinic_pyeong=req.clinic_pyeong,
        variable_cost_ratio=req.variable_cost_ratio,
    )
    return {
        "region_name": ctx["region_name"],
        "distance_km": round(ctx["distance_km"], 3),
        "activity_index": ctx["activity_index"],
        "bep": bep,
    }


@app.get("/api/cfo/survival")
def api_cfo_survival(lat: float, lng: float, dept: str = "한의원"):
    """상권 생존율·폐업률 추정(V1 시뮬레이션)."""
    return {"survival": estimate_survival_metrics(lat, lng, dept)}


@app.get("/api/cfo/rent-risk")
def api_cfo_rent_risk(lat: float, lng: float, radius_km: float = 3.0):
    """임대료 상승·젠트리피케이션 리스크."""
    if df_master is None or df_master.empty:
        raise HTTPException(status_code=503, detail="마스터 데이터가 없습니다.")
    ctx = resolve_nearest_master_context(df_master, lat, lng, radius_km)
    if not ctx:
        raise HTTPException(status_code=404, detail="해당 위치의 상권 데이터를 찾을 수 없습니다.")
    risk = estimate_rent_risk(
        ctx["activity_index"],
        ctx["estimated_rent_per_pyeong"],
        young_ratio=ctx["young_ratio"],
    )
    return {
        "region_name": ctx["region_name"],
        "estimated_rent_per_pyeong": ctx["estimated_rent_per_pyeong"],
        "rent_risk": risk,
    }


@app.get("/api/geo/walkable-polygon")
def api_walkable_polygon(lat: float, lng: float, minutes: float = 10.0):
    """도보 유효 범위 GeoJSON. PostGIS 설정 시 pgRouting 폴리곤, 아니면 원형 근사."""
    cfg = _build_phase2_config()
    if cfg.use_pgr_network:
        try:
            poly = get_walking_polygon(lat, lng, minutes, cfg)
            poly["data_source"] = "postgis_pgrouting"
            return poly
        except Exception as e:
            stub = walkable_polygon_stub(lat, lng, minutes)
            stub["fallback"] = True
            stub["fallback_reason"] = str(e)[:240]
            return stub
    return walkable_polygon_stub(lat, lng, minutes)


@app.post("/api/targeting/persona-score")
def api_persona_score(req: PersonaScoreRequest):
    """과목별 페르소나 적합도."""
    if df_master is None or df_master.empty:
        raise HTTPException(status_code=503, detail="마스터 데이터가 없습니다.")
    ctx = resolve_nearest_master_context(df_master, req.lat, req.lng, req.radius_km)
    if not ctx:
        raise HTTPException(status_code=404, detail="해당 위치의 상권 데이터를 찾을 수 없습니다.")
    persona = score_personas(req.dept, ctx["row"], ctx["estimated_spending_index"])
    return {
        "region_name": ctx["region_name"],
        "estimated_spending_index": ctx["estimated_spending_index"],
        "persona": persona,
    }


# =========================================================
# [4] 🚀 NEW: 포트원(아임포트) 결제 위변조 검증 API (보안 핵심)
# =========================================================
class PaymentRequest(BaseModel):
    imp_uid: str

@app.post("/api/verify-payment")
def verify_payment(req: PaymentRequest):
    imp_uid = req.imp_uid
    
    VALID_AMOUNTS = (7000, 30000)  # 1회 7천원, 5회 3만원
    try:
        token_req = requests.post(
            "https://api.iamport.kr/users/getToken",
            json={"imp_key": PORTONE_API_KEY, "imp_secret": PORTONE_API_SECRET},
            timeout=10
        )
        token_data = token_req.json()
        if token_data["code"] != 0:
            raise HTTPException(status_code=401, detail="포트원 인증 토큰 발급 실패")
        access_token = token_data["response"]["access_token"]
        payment_req = requests.get(
            f"https://api.iamport.kr/payments/{imp_uid}",
            headers={"Authorization": access_token},
            timeout=10
        )
        payment_data = payment_req.json()
        
        if payment_data["code"] != 0:
            raise HTTPException(status_code=400, detail="결제 내역 조회 실패")
            
        payment_info = payment_data["response"]
        
        # 3. 결제 금액 위변조 교차 검증 로직
        amount_paid = payment_info["amount"]
        status = payment_info["status"]
        
        if status == "paid" and amount_paid in VALID_AMOUNTS:
            # 💡 [추가 개발 권장] 여기서 데이터베이스(PostgreSQL 등)에 결제 내역을 저장하는 로직을 추가하세요.
            print(f"✅ 결제 검증 성공! (주문번호: {payment_info['merchant_uid']}, 금액: {amount_paid}원)")
            return {"status": "success", "message": "결제가 정상적으로 검증되었습니다."}
        else:
            # 금액이 조작되었거나 결제가 완료되지 않은 상태
            # 💡 [추가 개발 권장] 비정상 결제이므로 포트원 API를 통해 환불(Cancel) 처리를 진행해야 합니다.
            print(f"🚨 결제 위변조 의심! (허용금액: {VALID_AMOUNTS}, 실제결제액: {amount_paid})")
            raise HTTPException(status_code=400, detail="결제 금액이 일치하지 않거나 위변조가 의심됩니다.")
            
    except requests.exceptions.RequestException as e:
        print(f"🚨 포트원 서버 통신 에러: {e}")
        raise HTTPException(status_code=500, detail="포트원 서버 통신 중 오류가 발생했습니다.")


# 로컬에서 브라우저로 http://127.0.0.1:8000/ 열면 index.html 제공 (file:// 대신 사용 권장)
app.mount("/", StaticFiles(directory=_BASE, html=True), name="frontend")