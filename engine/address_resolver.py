# -*- coding: utf-8 -*-
"""
주소 변환 방어 파이프라인 (Plan B)

우선순위:
1) Kakao Local API (가능하면)
2) 행정안전부 JUSO API
3) 정규식 파싱(최소한 bun/ji라도 추출)  ← 마지막 보루

모든 외부 호출은 timeout=3 원칙을 적용한다.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

import requests

from auth_config import KAKAO_REST_KEY
from engine.juso_converter import convert_address_with_juso


def _pad4(v: Any) -> str:
    s = "" if v is None else str(v).strip()
    s = re.sub(r"\\D", "", s)
    return s.zfill(4)[:4] if s else ""


def _regex_fallback(address: str, fallback_sigungu_cd: str = "") -> Dict[str, Any]:
    # 지번 패턴(번-지)만이라도 추출
    s = str(address or "").strip()
    bun = None
    ji = None
    m = re.search(r"(\\d{1,4})\\s*-\\s*(\\d{1,4})\\s*$", s)
    if m:
        bun = _pad4(m.group(1))
        ji = _pad4(m.group(2))
    else:
        m2 = re.search(r"(\\d{1,4})\\s*$", s)
        if m2:
            bun = _pad4(m2.group(1))
            ji = "0000"
    return {
        "ok": bool(bun and ji),
        "message": "regex_fallback",
        "address": address,
        "sigungu_cd": fallback_sigungu_cd,
        "bjdong_cd": None,
        "bun": bun,
        "ji": ji,
    }


def _kakao_convert(address: str, *, timeout: int = 3) -> Dict[str, Any]:
    if not KAKAO_REST_KEY:
        return {"ok": False, "message": "kakao_key_missing"}
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_REST_KEY}"}
    params = {"query": str(address or "").strip()}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=timeout)
    except Exception as e:
        return {"ok": False, "message": f"kakao_network_error:{e}"}
    if res.status_code >= 400:
        return {"ok": False, "message": f"kakao_http_{res.status_code}"}
    try:
        data = res.json()
    except Exception:
        return {"ok": False, "message": "kakao_json_parse_failed"}
    docs = (data or {}).get("documents") or []
    if not docs:
        return {"ok": False, "message": "kakao_no_results"}
    d = docs[0] or {}
    addr = d.get("address") or {}
    road = d.get("road_address") or {}
    # b_code: 법정동코드 10자리
    b_code = str((addr.get("b_code") or road.get("b_code") or "")).strip()
    b_code_digits = re.sub(r"\\D", "", b_code)
    if len(b_code_digits) >= 10:
        adm10 = b_code_digits[:10]
        sigungu_cd = adm10[:5]
        bjdong_cd = adm10[5:]
    else:
        adm10 = None
        sigungu_cd = None
        bjdong_cd = None
    bun = _pad4(addr.get("main_address_no") or "")
    ji = _pad4(addr.get("sub_address_no") or "0")
    jibun_addr = addr.get("address_name")
    road_addr = road.get("address_name")
    ok = bool(sigungu_cd and bjdong_cd and bun and ji)
    return {
        "ok": ok,
        "message": "ok",
        "address": address,
        "source": "kakao",
        "adm_cd_10": adm10,
        "sigungu_cd": sigungu_cd,
        "bjdong_cd": bjdong_cd,
        "bun": bun or None,
        "ji": ji or None,
        "jibun_addr": jibun_addr,
        "road_addr": road_addr,
    }


def resolve_jibun_codes(address: str, fallback_sigungu_cd: str = "") -> Dict[str, Any]:
    # 1) Kakao
    k = _kakao_convert(address, timeout=3)
    if k.get("ok"):
        return k
    # 2) JUSO
    j = convert_address_with_juso(address, timeout=3)
    if j.get("ok"):
        j["source"] = "juso"
        return j
    # 3) regex
    r = _regex_fallback(address, fallback_sigungu_cd=fallback_sigungu_cd)
    r["source"] = "regex"
    # JUSO/Kakao 실패 사유를 함께 남김
    r["upstream"] = {"kakao": k.get("message"), "juso": j.get("message")}
    return r

