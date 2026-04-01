# -*- coding: utf-8 -*-
"""
심평원 병원정보서비스(HIRA) API 키·응답 진단.

사용:
  cd 프로젝트루트
  $env:HIRA_API_KEY = "키"
  python scripts/diagnose_hira_key.py
  python scripts/diagnose_hira_key.py --app-dept 치과
  python scripts/diagnose_hira_key.py --app-dept 한의원

해석:
  - HTTP 200 + resultCode "00" → 키·호출 정상
  - 기본 진단은 clCd 없이 반경만 조회(넓음). 앱은 과목별 clCd를 붙여 0건일 수 있음 → --app-dept 로 확인

Fly 로그: fly logs --app <앱이름>
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from typing import Any, Dict, Tuple

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(1)

URL = "https://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList"


def _hira_params_for_dept(dept: str) -> Dict[str, str]:
    """main.py fetch_real_hospitals 와 동일 규칙."""
    d = str(dept or "").strip()
    cl_cd = "31"
    dgsbjt = ""
    if d == "치과":
        cl_cd = "50"
    elif d == "한의원":
        cl_cd = "93"
    else:
        cl_cd = "31"
        codes = {
            "내과": "01",
            "피부과": "14",
            "안과": "12",
            "정형외과": "05",
            "소아과": "11",
            "이비인후과": "13",
            "산부인과": "10",
            "정신건강의학과": "03",
        }
        dgsbjt = codes.get(d, "")
    out: Dict[str, str] = {}
    if cl_cd != "31":
        out["clCd"] = cl_cd
    if dgsbjt:
        out["dgsbjtCd"] = dgsbjt
    return out


def _run_probe(params: Dict[str, Any], title: str) -> Tuple[int, str]:
    print("\n" + "=" * 60)
    print(title)
    print("파라미터 키:", sorted(params.keys()))
    try:
        r = requests.get(URL, params=params, timeout=20)
    except requests.RequestException as e:
        print("네트워크 오류:", e)
        return 1, "network"

    print("HTTP 상태:", r.status_code)
    text = r.text or ""
    try:
        data = r.json()
    except json.JSONDecodeError:
        print("JSON 아님, 본문 앞 500자:\n", text[:500])
        return 1, "parse"

    resp = data.get("response") or {}
    hdr = resp.get("header") or {}
    print("resultCode:", hdr.get("resultCode"), "| resultMsg:", hdr.get("resultMsg"))

    body = resp.get("body") or {}
    items = (body.get("items") or {}).get("item")
    n = 0
    if items is None or items == "":
        n = 0
    elif isinstance(items, dict):
        n = 1
    elif isinstance(items, list):
        n = len(items)
    print("items 건수:", n)

    code = str(hdr.get("resultCode", "")).strip()
    ok = r.status_code == 200 and code in ("00", "0")
    return (0 if ok else 1), "ok" if ok else "api"


def main() -> int:
    ap = argparse.ArgumentParser(description="HIRA getHospBasisList 키 진단")
    ap.add_argument("--key", default="", help="미지정 시 환경변수 HIRA_API_KEY")
    ap.add_argument(
        "--app-dept",
        default="",
        help="main.py와 동일 과목 필터로 추가 조회 (예: 치과, 한의원, 피부과)",
    )
    args = ap.parse_args()
    raw = (args.key or os.getenv("HIRA_API_KEY") or "").strip()
    if not raw:
        print("HIRA_API_KEY가 없습니다.", file=sys.stderr)
        return 2

    service_key = urllib.parse.unquote(raw)
    base = {
        "ServiceKey": service_key,
        "xPos": 129.1633,
        "yPos": 35.1631,
        "radius": 1000,
        "numOfRows": 20,
        "pageNo": 1,
        "_type": "json",
    }

    print("요청 URL:", URL)
    print("serviceKey 앞 8자리:", service_key[:8] + "…", "(길이 %d)" % len(service_key))

    ec, _ = _run_probe(dict(base), "[1] 과목 필터 없음 (반경만, 앱의 완화 1단계와 유사)")
    if ec != 0:
        print("\n결론: 키 또는 네트워크 문제 가능. resultMsg·HTTP를 공공데이터포털과 대조.")
        return ec

    dept = str(args.app_dept or "").strip()
    if dept:
        extra = _hira_params_for_dept(dept)
        p2 = dict(base)
        p2.update(extra)
        _run_probe(
            p2,
            f'[2] 앱과 동일 과목 필터 (dept="{dept}", clCd/dgsbjtCd)',
        )
        print(
            "\n해석: [1]은 건수가 나오는데 [2]가 0건이면, 해당 좌표 반경에 그 과목 기관이 없거나 필터가 엄격한 것이며 키 문제는 아닙니다."
        )
    else:
        print(
            "\n결론: 키는 정상입니다. 앱에서만 안 보이면 —"
            "\n  · Vercel이 Fly 백엔드 URL을 쓰는지(BLUEDOT_API_BASE)"
            "\n  · Fly에 HIRA_API_KEY가 로컬과 동일하게 secrets 되었는지"
            "\n  · 브라우저 Network에서 /api/hospitals 응답의 hospitals 길이"
            "\n과목별 0건 여부는: python scripts/diagnose_hira_key.py --app-dept 치과"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
