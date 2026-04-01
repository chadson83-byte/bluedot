# -*- coding: utf-8 -*-
"""
행정안전부 도로명주소(JUSO) API 연동

도로명/혼합 주소 문자열을 입력받아 지번(본번/부번) 및 행정구역코드(admCd)를 추출한다.

API: https://www.juso.go.kr/addrlink/addrLinkApi.do
필수: confmKey(승인키), keyword(검색어), resultType=json
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Any, Dict, Optional

import requests


def _juso_confm_key() -> str:
    return (
        (os.getenv("JUSO_CONFM_KEY") or "").strip()
        or (os.getenv("JUSO_ADDR_LINK_KEY") or "").strip()
        or "devU01TX0FVVEgyMDI2MDMyNTIyMDcwMDExNzc5MTU="
    )

JUSO_ENDPOINT = "https://www.juso.go.kr/addrlink/addrLinkApi.do"


def _pad4(v: Any) -> str:
    s = "" if v is None else str(v).strip()
    s = re.sub(r"\D", "", s)
    return s.zfill(4)[:4] if s else ""


def _clean_keyword(addr: str) -> str:
    s = str(addr or "").strip()
    # 괄호/층 정보는 검색 품질을 떨어뜨리는 경우가 많아 제거
    s = re.sub(r"\([^)]*\)", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s


@lru_cache(maxsize=2048)
def convert_address_with_juso(address: str, *, timeout: int = 8) -> Dict[str, Any]:
    """
    도로명주소 API 호출로 주소를 지번코드로 변환.

    반환:
    - ok: bool
    - message: str
    - adm_cd_10: str|None  (행정구역코드 10자리)
    - sigungu_cd: str|None (앞 5자리)
    - bjdong_cd: str|None  (뒤 5자리)
    - bun: str|None (4자리)
    - ji: str|None (4자리)
    - jibun_addr: str|None
    - road_addr: str|None
    """
    kw = _clean_keyword(address)
    if not kw:
        return {"ok": False, "message": "주소가 비어 있습니다.", "address": address}

    params = {
        "confmKey": _juso_confm_key(),
        "keyword": kw,
        "currentPage": "1",
        "countPerPage": "1",
        "resultType": "json",
    }

    try:
        res = requests.get(JUSO_ENDPOINT, params=params, timeout=timeout)
    except Exception as e:
        return {"ok": False, "message": f"도로명주소 API 호출 실패(네트워크): {e}", "address": address}

    if res.status_code >= 400:
        return {"ok": False, "message": f"도로명주소 API 호출 실패(HTTP {res.status_code})", "address": address}

    try:
        data = res.json()
    except Exception:
        return {"ok": False, "message": "도로명주소 API 응답 JSON 파싱 실패", "address": address}

    try:
        results = (data or {}).get("results") or {}
        common = results.get("common") or {}
        err_cd = str(common.get("errorCode", "")).strip()
        err_msg = str(common.get("errorMessage", "")).strip()
        if err_cd and err_cd != "0":
            return {"ok": False, "message": f"도로명주소 API 오류({err_cd}): {err_msg}", "address": address}
        juso_list = results.get("juso") or []
        if not juso_list:
            return {"ok": False, "message": "주소 검색 결과가 없습니다.", "address": address}
        j = juso_list[0] or {}
        adm = str(j.get("admCd") or "").strip()
        lnbr_mnnm = _pad4(j.get("lnbrMnnm"))
        lnbr_slno = _pad4(j.get("lnbrSlno") or "0")
        if not adm or len(re.sub(r"\D", "", adm)) < 10:
            return {"ok": False, "message": "admCd(행정구역코드)가 응답에 없습니다.", "address": address}
        adm_digits = re.sub(r"\D", "", adm)[:10]
        sigungu_cd = adm_digits[:5]
        bjdong_cd = adm_digits[5:]
        return {
            "ok": True,
            "message": "ok",
            "address": address,
            "keyword_used": kw,
            "adm_cd_10": adm_digits,
            "sigungu_cd": sigungu_cd,
            "bjdong_cd": bjdong_cd,
            "bun": lnbr_mnnm or None,
            "ji": lnbr_slno or None,
            "jibun_addr": str(j.get("jibunAddr") or "").strip() or None,
            "road_addr": str(j.get("roadAddrPart1") or "").strip() or None,
        }
    except Exception as e:
        return {"ok": False, "message": f"도로명주소 API 응답 파싱 오류: {e}", "address": address}

