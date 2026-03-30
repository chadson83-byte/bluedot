# -*- coding: utf-8 -*-
"""
법정동 코드 매퍼

입력:
- HIRA 주소: '부산광역시 해운대구 중동 1378-9' 등
- HIRA 시군구코드(5자리): sgguCd

출력:
- 건축HUB API에 필요한 bjdongCd(법정동코드, 5자리)

데이터 소스:
- data/법정동코드 전체자료.txt (탭 구분)
  컬럼: 법정동코드(10자리), 법정동명, 폐지여부(존재/폐지)
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Dict, Optional, Tuple

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BJDONG_TXT = os.path.join(_BASE, "data", "법정동코드 전체자료.txt")


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


def _norm_dong_token(tok: str) -> str:
    t = _norm_space(tok)
    # 접미 제거(동/읍/면/리)
    for suf in ("동", "읍", "면", "리", "가"):
        if t.endswith(suf) and len(t) > 1:
            t = t[: -len(suf)]
            break
    return t


def extract_legal_dong_from_address(addr: str) -> Optional[str]:
    """
    주소에서 법정동 후보 토큰(중동/우동/역삼동 등)을 추출.
    번지 직전의 동/읍/면/리 토큰을 우선.
    """
    s = _norm_space(addr)
    if not s:
        return None
    parts = s.split(" ")
    # 괄호 우선: "... (전포동)" 같은 케이스가 가장 안정적
    m = re.search(r"\(([가-힣0-9]+?(동|읍|면|리|가))\)", s)
    if m:
        return m.group(1)
    # 뒤에서부터 스캔: 숫자/번지/층 등을 제외하고 동/읍/면/리/가로 끝나는 토큰 찾기
    for p in reversed(parts):
        if any(ch.isdigit() for ch in p):
            continue
        if p.endswith(("동", "읍", "면", "리", "가")):
            return p
    return None


@lru_cache(maxsize=1)
def _load_table(path: str) -> Dict[Tuple[str, str], str]:
    """
    (sigunguCd, dong_norm) -> bjdongCd 매핑 테이블 생성.
    sigunguCd: 법정동코드 10자리의 앞 5자리
    bjdongCd: 법정동코드 10자리의 뒤 5자리
    """
    if not os.path.isfile(path):
        return {}
    mapping: Dict[Tuple[str, str], str] = {}
    # 파일 인코딩이 cp949인 경우가 많아 2단계 시도
    text = None
    for enc in ("utf-8", "cp949"):
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                sample = f.read(4096)
                rest = f.read()
            candidate = sample + rest
            # utf-8로 "성공"했더라도 깨짐(�)이 다수면 cp949로 재시도
            if enc == "utf-8" and candidate.count("�") > 10:
                continue
            text = candidate
            break
        except Exception:
            text = None
    if text is None:
        return {}

    lines = text.splitlines()
    if not lines:
        return {}
    # 헤더 스킵
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        cols = line.split("\t")
        if len(cols) < 3:
            continue
        code10 = re.sub(r"\D", "", cols[0])
        name = _norm_space(cols[1])
        status = _norm_space(cols[2])
        if len(code10) != 10:
            continue
        if status != "존재":
            continue
        sigungu = code10[:5]
        bjdong = code10[5:]

        tokens = name.split(" ")
        if not tokens:
            continue
        dong_token = tokens[-1]
        dong_norm = _norm_dong_token(dong_token)
        if not dong_norm:
            continue
        mapping[(sigungu, dong_norm)] = bjdong
    return mapping


def resolve_bjdong_cd(
    *,
    sigungu_cd: str,
    address: str,
    table_path: str = DEFAULT_BJDONG_TXT,
) -> Optional[str]:
    """
    HIRA 주소 + 시군구코드에서 bjdongCd(5자리) 찾기.
    """
    sgg = re.sub(r"\D", "", str(sigungu_cd or "").strip())
    if len(sgg) != 5:
        return None
    dong = extract_legal_dong_from_address(address)
    if not dong:
        return None
    dong_norm = _norm_dong_token(dong)
    if not dong_norm:
        return None
    table = _load_table(table_path)
    return table.get((sgg, dong_norm))

