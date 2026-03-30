# -*- coding: utf-8 -*-
"""
BLUEDOT - 경쟁 병원 건물 노후화 분석 모듈

공공데이터포털 '건축HUB_건축물대장정보' API를 통해
경쟁 병원이 입점한 건물의 연식/엘리베이터/주차 스펙을 조회하여 요약 리포트를 생성한다.

요구사항:
- serviceKey: 환경변수 DATA_GO_KR_SERVICE_KEY(또는 BUILDING_HUB_SERVICE_KEY), 없으면 레거시 기본값
- URL은 f-string으로 직접 조립(quote 미사용)
- requests.get(url, verify=False) 호출
- _type=json 응답에서 useAprDay, rideUseElvtCnt/emgenUseElvtCnt, indrAutoUtcnt/oudrAutoUtcnt 추출
- 예외 처리(403/트래픽 초과/빈 응답 등)로 앱이 뻗지 않게 처리
- 실행 예시(__main__) 포함
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from engine.building_cache_pg import ensure_cache_table, get_cached, upsert_cached

# 로컬·레거시 호환. 운영은 DATA_GO_KR_SERVICE_KEY(또는 BUILDING_HUB_SERVICE_KEY) 환경변수 권장.
_LEGACY_SERVICE_KEY = "8ee102c5d025b9a9709736175aa0168bac653098ef0f762e797f727d77dc7da9"


def _building_hub_service_key() -> str:
    return (
        (os.getenv("DATA_GO_KR_SERVICE_KEY") or "").strip()
        or (os.getenv("BUILDING_HUB_SERVICE_KEY") or "").strip()
        or _LEGACY_SERVICE_KEY
    )

ENDPOINT = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"


def _pad4(v: Any) -> str:
    s = "" if v is None else str(v).strip()
    if s == "":
        return ""
    s = "".join(ch for ch in s if ch.isdigit())
    return s.zfill(4)[:4]


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _parse_use_apr_day(use_apr_day: Any) -> Optional[datetime]:
    """
    useAprDay: 보통 YYYYMMDD 형태 문자열/숫자.
    """
    if use_apr_day is None:
        return None
    s = str(use_apr_day).strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) < 8:
        return None
    try:
        return datetime.strptime(digits[:8], "%Y%m%d")
    except Exception:
        return None


def fetch_building_info(
    sigunguCd: str,
    bjdongCd: str,
    bun: str,
    ji: str,
    *,
    timeout: int = 3,
) -> Dict[str, Any]:
    """
    건축물대장(표제부) 조회.

    요구사항:
    - serviceKey를 URL에 직접 포함(quote 미사용)
    - verify=False로 호출
    - _type=json
    """
    bun4 = _pad4(bun)
    ji4 = _pad4(ji)

    # 필수 파라미터 검증(빈 값이면 API를 치지 않음)
    if not (sigunguCd and bjdongCd and bun4 and ji4):
        return {
            "ok": False,
            "message": "지번 코드(sigunguCd/bjdongCd/bun/ji)가 부족합니다. (도로명주소→지번 변환 또는 법정동코드 매핑 필요)",
            "raw": None,
        }

    service_key = _building_hub_service_key()
    if not service_key:
        return {"ok": False, "message": "건축HUB API 키 미설정(DATA_GO_KR_SERVICE_KEY)", "raw": None}

    # ⚠️ quote 사용 금지: f-string으로 직접 URL 조립
    url = (
        f"{ENDPOINT}"
        f"?serviceKey={service_key}"
        f"&sigunguCd={sigunguCd}"
        f"&bjdongCd={bjdongCd}"
        f"&bun={bun4}"
        f"&ji={ji4}"
        f"&numOfRows=10&pageNo=1"
        f"&_type=json"
    )

    # 캐시 우선
    ensure_cache_table()
    cached = get_cached(sigunguCd, bjdongCd, bun4, ji4)
    if isinstance(cached, dict) and cached.get("building"):
        return {"ok": True, "message": "cached", "raw": None, "building": cached.get("building")}

    try:
        res = requests.get(url, verify=False, timeout=timeout)
    except Exception as e:
        return {
            "ok": False,
            "message": f"건축물대장 API 네트워크 오류: {type(e).__name__}",
            "raw": None,
        }

    if res.status_code == 403:
        return {
            "ok": False,
            "message": "건축HUB 403 — 공공데이터포털에서 서비스키·일일한도·등록 IP를 확인하세요.",
            "raw": None,
        }
    if res.status_code >= 400:
        return {
            "ok": False,
            "message": f"건축HUB HTTP {res.status_code} — 응답 본문을 공공데이터포털에서 확인하세요.",
            "raw": None,
        }

    try:
        data = res.json()
    except Exception:
        return {"ok": False, "message": "건축HUB 응답이 JSON이 아닙니다.", "raw": None}

    try:
        header = (((data or {}).get("response") or {}).get("header") or {})
        body = (((data or {}).get("response") or {}).get("body") or {})
        result_code = str(header.get("resultCode", "")).strip()
        result_msg = str(header.get("resultMsg", "") or "").strip()
        # 공공데이터포털은 정상 "00" 패턴이 많지만, 다른 코드도 있을 수 있어 메시지 우선
        if result_code and result_code not in ("00", "0"):
            hint = f" ({result_msg})" if result_msg else ""
            return {
                "ok": False,
                "message": f"건축HUB API 오류 코드 {result_code}{hint}",
                "raw": None,
            }

        items = (body.get("items") or {}).get("item")
        if items is None or items == "":
            return {"ok": True, "message": "데이터 없음", "raw": data, "building": None}
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list) or len(items) == 0:
            return {"ok": True, "message": "데이터 없음", "raw": data, "building": None}

        item = items[0]  # 표제부는 보통 1건
        use_apr_day = item.get("useAprDay")
        dt = _parse_use_apr_day(use_apr_day)
        age_years = None
        if dt is not None:
            age_years = max(0, datetime.now().year - dt.year)

        elv = _to_int(item.get("rideUseElvtCnt"), 0) + _to_int(item.get("emgenUseElvtCnt"), 0)
        parking = _to_int(item.get("indrAutoUtcnt"), 0) + _to_int(item.get("oudrAutoUtcnt"), 0)

        out = {
            "ok": True,
            "message": "ok",
            "raw": None,  # raw는 필요 시만
            "building": {
                "useAprDay": str(use_apr_day).strip() if use_apr_day is not None else None,
                "age_years": age_years,
                "elevator_total": elv,
                "parking_total": parking,
            },
        }
        try:
            upsert_cached(sigunguCd, bjdongCd, bun4, ji4, {"building": out["building"]})
        except Exception:
            pass
        return out
    except Exception as e:
        return {"ok": False, "message": f"건축HUB 응답 처리 오류: {type(e).__name__}", "raw": None}


def generate_aging_report(
    competitors: List[Dict[str, Any]],
    *,
    sleep_sec: float = 0.15,
    old_building_years: int = 20,
    low_parking_threshold: int = 5,
    max_total_sec: float = 4.0,
) -> Dict[str, Any]:
    """
    입력: 경쟁 한의원 리스트
      예: [
        {'name':'A한의원','address':'부산... 1378-9','sigungu_cd':'26350','bjdong_cd':'10500','bun':'1378','ji':'0009'},
        ...
      ]
    출력: 요약 리포트(dict)
    """
    total = len(competitors or [])
    results: List[Dict[str, Any]] = []

    ages: List[int] = []
    old_cnt = 0
    no_elevator: List[str] = []
    low_parking: List[str] = []
    data_missing: List[str] = []
    api_unavailable_cnt = 0
    param_missing_cnt = 0

    started = time.time()
    for idx, c in enumerate(competitors or []):
        if max_total_sec and (time.time() - started) > float(max_total_sec):
            break
        name = str(c.get("name") or "이름없음").strip()
        sigungu_cd = str(c.get("sigungu_cd") or "").strip()
        bjdong_cd = str(c.get("bjdong_cd") or "").strip()
        bun = str(c.get("bun") or "").strip()
        ji = str(c.get("ji") or "").strip()

        info = fetch_building_info(sigungu_cd, bjdong_cd, bun, ji, timeout=3)
        building = info.get("building")
        ok = bool(info.get("ok"))

        rec = {
            "name": name,
            "address": c.get("address"),
            "query": {"sigunguCd": sigungu_cd, "bjdongCd": bjdong_cd, "bun": _pad4(bun), "ji": _pad4(ji)},
            "ok": ok,
            "message": info.get("message"),
            "building": building,
        }
        results.append(rec)

        if not ok:
            msg = str(info.get("message") or "")
            if "지번 코드" in msg or "부족" in msg:
                param_missing_cnt += 1
            else:
                api_unavailable_cnt += 1
        if building is None:
            data_missing.append(name)
        else:
            age = building.get("age_years")
            if isinstance(age, int):
                ages.append(age)
                if age >= int(old_building_years):
                    old_cnt += 1
            if int(building.get("elevator_total") or 0) <= 0:
                no_elevator.append(name)
            if int(building.get("parking_total") or 0) < int(low_parking_threshold):
                low_parking.append(name)

        # 서버 과부하 방지
        if sleep_sec and idx < total - 1:
            time.sleep(float(sleep_sec))

    avg_age = round(sum(ages) / len(ages), 1) if ages else None
    old_ratio = round((old_cnt / len(ages)) * 100.0, 1) if ages else None

    if ages and old_ratio is not None and old_ratio >= 50.0:
        insight = (
            f"경쟁 병원 다수가 노후 건물에 입점해 있어(노후 {old_ratio:.1f}%), "
            "신축/리모델링 상가 진입 시 '쾌적함·동선·편의'를 무기로 우위를 점할 수 있습니다."
        )
    elif ages and avg_age is not None:
        insight = (
            f"경쟁 병원 평균 건물 연차는 약 {avg_age}년입니다. "
            "인테리어 품질·대기 경험·주차 편의가 차별화 포인트가 될 수 있습니다."
        )
    else:
        insight = "현재 건물 정보를 불러올 수 없습니다. (API 승인/트래픽/지번코드 확인 필요)"

    return {
        "summary": {
            "competitor_count": total,
            "avg_building_age_years": avg_age,
            "old_building_ratio_pct": old_ratio,
            "old_building_threshold_years": int(old_building_years),
            "api_unavailable_count": api_unavailable_cnt,
            "param_missing_count": param_missing_cnt,
        },
        "lists": {
            "no_elevator": no_elevator,
            "low_parking_under_5": low_parking,
            "data_missing": data_missing,
        },
        "insight": insight,
        "details": results,
    }


if __name__ == "__main__":
    # 더미 실행 예시 (실제 sigungu/bjdong/bun/ji는 맞춰야 결과가 나옵니다)
    demo = [
        {
            "name": "A한의원",
            "address": "부산광역시 해운대구 중동 1378-9",
            "sigungu_cd": "26350",
            "bjdong_cd": "10500",
            "bun": "1378",
            "ji": "0009",
        },
        {
            "name": "B한의원",
            "address": "부산광역시 해운대구 우동 123-4",
            "sigungu_cd": "26350",
            "bjdong_cd": "10600",
            "bun": "0123",
            "ji": "0004",
        },
    ]

    report = generate_aging_report(demo, sleep_sec=0.2)
    print("=== 경쟁 병원 건물 노후화 리포트 ===")
    print(report["summary"])
    print("엘리베이터 없음:", report["lists"]["no_elevator"])
    print("주차 취약(<5):", report["lists"]["low_parking_under_5"])
    print("데이터 없음:", report["lists"]["data_missing"])
    print("인사이트:", report["insight"])
