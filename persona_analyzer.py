# -*- coding: utf-8 -*-
"""
BLUEDOT - SGIS 기반 페르소나 분석 모듈

요구사항
1) get_sgis_token: consumer_key/consumer_secret로 accessToken 발급
2) fetch_population_data: SGIS 인구통계 API 호출 후 10세 단위 연령대 집계 반환
3) calculate_persona_score: 과목별 가중치 기반 100점 만점 점수 + 인사이트 반환
4) 예외 처리 + __main__ 테스트(노년층 많은 가상 데이터에서 한의원 >> 소아과)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass
class SGISConfig:
    # 키값은 더미 문자열로 비워둠(사용자가 채워 넣기)
    consumer_key: str = ""
    consumer_secret: str = ""
    timeout_sec: int = 10


class SGISError(RuntimeError):
    pass


def get_sgis_token(config: Optional[SGISConfig] = None) -> str:
    """
    SGIS accessToken 발급.

    문서 기준(OpenAPI3): https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json
    - consumer_key / consumer_secret 필요
    """
    cfg = config or SGISConfig()
    if not cfg.consumer_key or not cfg.consumer_secret:
        raise SGISError("SGIS consumer_key/consumer_secret이 비어 있습니다. 설정 후 호출하세요.")

    url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
    params = {"consumer_key": cfg.consumer_key, "consumer_secret": cfg.consumer_secret}
    try:
        res = requests.get(url, params=params, timeout=cfg.timeout_sec)
    except Exception as e:
        raise SGISError(f"SGIS 토큰 발급 실패(네트워크): {e}") from e

    if res.status_code >= 400:
        raise SGISError(f"SGIS 토큰 발급 실패(HTTP {res.status_code}): {res.text[:200]}")

    try:
        data = res.json()
    except Exception as e:
        raise SGISError(f"SGIS 토큰 발급 실패(JSON 파싱): {e}") from e

    token = (data.get("result") or {}).get("accessToken") or data.get("accessToken")
    if not token:
        msg = data.get("errMsg") or data.get("message") or "accessToken이 응답에 없습니다."
        raise SGISError(f"SGIS 토큰 발급 실패: {msg}")
    return str(token)


def _bucket_age(age: Any) -> Optional[str]:
    """
    SGIS 응답의 연령 값을 10세 단위 버킷으로 매핑.
    반환 키:
      - '0_9', '10s', '20s', '30s', '40s', '50s', '60p'
    """
    if age is None:
        return None
    s = str(age).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return None
    try:
        a = int(digits)
    except Exception:
        return None
    if a <= 9:
        return "0_9"
    if 10 <= a <= 19:
        return "10s"
    if 20 <= a <= 29:
        return "20s"
    if 30 <= a <= 39:
        return "30s"
    if 40 <= a <= 49:
        return "40s"
    if 50 <= a <= 59:
        return "50s"
    return "60p"


def fetch_population_data(
    access_token: str,
    *,
    adm_cd: Optional[str] = None,
    x: Optional[float] = None,
    y: Optional[float] = None,
    year: Optional[str] = None,
    config: Optional[SGISConfig] = None,
) -> Dict[str, Any]:
    """
    SGIS 인구통계 API 호출 후 10세 단위 연령대별 인구수 반환.

    예시 엔드포인트:
      https://sgisapi.kostat.go.kr/OpenAPI3/stats/searchpopulation.json

    입력은 adm_cd(행정동 코드) 또는 좌표(x,y) 중 하나를 우선 사용.
    """
    cfg = config or SGISConfig()
    if not access_token:
        raise SGISError("access_token이 비어 있습니다.")

    url = "https://sgisapi.kostat.go.kr/OpenAPI3/stats/searchpopulation.json"
    params: Dict[str, Any] = {"accessToken": access_token}
    if year:
        params["year"] = year
    if adm_cd:
        params["adm_cd"] = adm_cd
    else:
        if x is None or y is None:
            raise SGISError("adm_cd 또는 (x,y) 좌표 중 하나는 필수입니다.")
        params["x_coor"] = str(x)
        params["y_coor"] = str(y)

    try:
        res = requests.get(url, params=params, timeout=cfg.timeout_sec)
    except Exception as e:
        raise SGISError(f"SGIS 인구통계 호출 실패(네트워크): {e}") from e

    if res.status_code >= 400:
        raise SGISError(f"SGIS 인구통계 호출 실패(HTTP {res.status_code}): {res.text[:200]}")

    try:
        data = res.json()
    except Exception as e:
        raise SGISError(f"SGIS 인구통계 호출 실패(JSON 파싱): {e}") from e

    # 에러 형식
    if data.get("errCd") not in (None, 0, "0"):
        msg = data.get("errMsg") or "SGIS API 오류"
        raise SGISError(f"SGIS 인구통계 오류: {msg}")

    result = data.get("result") or []
    if not isinstance(result, list):
        result = []

    buckets = {"0_9": 0, "10s": 0, "20s": 0, "30s": 0, "40s": 0, "50s": 0, "60p": 0}
    total = 0

    # SGIS 응답 필드가 환경마다 다를 수 있어, 안전하게 후보 키를 순회
    for row in result:
        if not isinstance(row, dict):
            continue
        # 연령 키 후보
        age_val = row.get("age") or row.get("age_cd") or row.get("age_group") or row.get("age_nm")
        # 인구 수 키 후보
        cnt_val = row.get("population") or row.get("pop") or row.get("tot_ppltn") or row.get("value") or row.get("cnt")
        b = _bucket_age(age_val)
        if not b:
            continue
        try:
            cnt = int(float(cnt_val))
        except Exception:
            cnt = 0
        buckets[b] += cnt
        total += cnt

    ratios = {k: (buckets[k] / total if total > 0 else 0.0) for k in buckets.keys()}
    return {"counts": buckets, "ratios": ratios, "total": total, "raw_size": len(result)}


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def calculate_persona_score(pop: Dict[str, Any], dept: str) -> Dict[str, Any]:
    """
    연령대 비율(ratios)을 입력 받아 과목별 페르소나 점수(0~100)와 인사이트 생성.

    dept 예시:
      - '한의원', '소아과', '피부과', '치과'
    """
    ratios = (pop or {}).get("ratios") or {}
    r0 = float(ratios.get("0_9", 0.0))
    r20 = float(ratios.get("20s", 0.0))
    r30 = float(ratios.get("30s", 0.0))
    r40 = float(ratios.get("40s", 0.0))
    r50 = float(ratios.get("50s", 0.0))
    r60 = float(ratios.get("60p", 0.0))

    # 기본 스코어 구성(0~1 비율들을 가중합한 뒤 100 스케일)
    if dept in ("한의원", "korean_medicine"):
        raw = (r50 * 1.6 + r60 * 2.0 + r40 * 0.6)
        score = _clamp(raw * 100.0)
        insight = f"50대 비율 {r50*100:.1f}%, 60대 이상 비율 {r60*100:.1f}%로 한의원 개원에 유리한 상권입니다."
    elif dept in ("소아과", "pediatrics"):
        raw = (r0 * 2.0 + (r30 + r40) * 1.2 + r20 * 0.3)
        score = _clamp(raw * 100.0)
        insight = f"0~9세 비율 {r0*100:.1f}%, 30~40대(부모세대) 비율 {(r30+r40)*100:.1f}%로 소아과 수요가 기대됩니다."
    elif dept in ("피부과", "dermatology", "치과", "dentistry"):
        raw = ((r20 + r30) * 1.7 + r40 * 0.6)
        score = _clamp(raw * 100.0)
        insight = f"20~30대 비율 {(r20+r30)*100:.1f}%로 {('피부과' if dept in ('피부과','dermatology') else '치과')} 비급여·미용 수요 타깃에 유리합니다."
    else:
        raw = (r30 + r40 + r50) * 1.0
        score = _clamp(raw * 100.0)
        insight = f"30~50대 핵심 연령 비율 {(r30+r40+r50)*100:.1f}% 기반으로 일반 외래 수요를 평가할 수 있습니다."

    return {
        "dept": dept,
        "score_100": round(score, 2),
        "insight": insight,
        "ratios": {
            "0_9": r0,
            "20s": r20,
            "30s": r30,
            "40s": r40,
            "50s": r50,
            "60p": r60,
        },
    }


if __name__ == "__main__":
    # 테스트: 노년층이 비정상적으로 많은 가상 데이터
    fake = {
        "counts": {"0_9": 500, "10s": 800, "20s": 900, "30s": 1200, "40s": 1500, "50s": 3000, "60p": 9000},
        "total": 500 + 800 + 900 + 1200 + 1500 + 3000 + 9000,
    }
    fake["ratios"] = {k: fake["counts"][k] / fake["total"] for k in fake["counts"].keys()}

    km = calculate_persona_score(fake, "한의원")
    ped = calculate_persona_score(fake, "소아과")

    print("=== 페르소나 테스트 ===")
    print("한의원:", km["score_100"], km["insight"])
    print("소아과:", ped["score_100"], ped["insight"])

    assert km["score_100"] > ped["score_100"], "테스트 실패: 한의원 점수가 소아과보다 높아야 합니다."
    print("OK: 한의원 점수가 소아과보다 높습니다.")

