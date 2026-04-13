# -*- coding: utf-8 -*-
"""
data/ 아래 오아시스 CSV 경로 결정.
- 공식 파일명 우선(data/ 루트)
- 없으면 data/ES1007AD*.csv 또는 data/ES1007AD|es1007AD/ 아래 동일 접두 파일 중
  용량이 가장 큰 파일(분할본 대비). BD도 동일(ES1007BD|es1007BD).
"""
from __future__ import annotations

import glob
import os
from typing import List, Optional

_EXACT_AD = "ES1007AD00101MM2504_csv.csv"
# 배포·로컬에서 자주 쓰는 단일 파일명(분할본보다 우선)
_PREFERRED_AD_NAMES = ("ES1007AD.csv", "ES1007AD.CSV")
_EXACT_BD = "ES1007BD00101MM2504_csv.csv"
_AD_SUBDIRS = ("ES1007AD", "es1007AD")
_BD_SUBDIRS = ("ES1007BD", "es1007BD")


def _pick_largest(paths: List[str]) -> Optional[str]:
    exist = [p for p in paths if os.path.isfile(p)]
    if not exist:
        return None
    return max(exist, key=lambda p: os.path.getsize(p))


def _preferred_es1007ad_in_dir(dirpath: str) -> Optional[str]:
    """Linux는 확장자 대소문자 구분 → .csv / .CSV 둘 다 허용."""
    if not dirpath or not os.path.isdir(dirpath):
        return None
    for fn in _PREFERRED_AD_NAMES:
        p = os.path.join(dirpath, fn)
        if os.path.isfile(p):
            return p
    return None


def _uniq_paths(paths: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for p in paths:
        key = os.path.normcase(os.path.normpath(os.path.abspath(p)))
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _glob_ad_under(dirpath: str) -> List[str]:
    return glob.glob(os.path.join(dirpath, "ES1007AD*.csv")) + glob.glob(
        os.path.join(dirpath, "ES1007AD*.CSV")
    )


def _glob_bd_under(dirpath: str) -> List[str]:
    return glob.glob(os.path.join(dirpath, "ES1007BD*.csv")) + glob.glob(
        os.path.join(dirpath, "ES1007BD*.CSV")
    )


def resolve_es1007ad_csv(project_root: str) -> Optional[str]:
    d = os.path.join(project_root, "data")
    hit = _preferred_es1007ad_in_dir(d)
    if hit:
        return hit
    exact = os.path.join(d, _EXACT_AD)
    if os.path.isfile(exact):
        return exact
    for name in _AD_SUBDIRS:
        sd = os.path.join(d, name)
        sub_hit = _preferred_es1007ad_in_dir(sd)
        if sub_hit:
            return sub_hit
    cands: List[str] = []
    cands.extend(_glob_ad_under(d))
    for name in _AD_SUBDIRS:
        sd = os.path.join(d, name)
        if os.path.isdir(sd):
            cands.extend(_glob_ad_under(sd))
    return _pick_largest(_uniq_paths(cands))


def resolve_es1007bd_csv(project_root: str) -> Optional[str]:
    d = os.path.join(project_root, "data")
    exact = os.path.join(d, _EXACT_BD)
    if os.path.isfile(exact):
        return exact
    cands: List[str] = []
    cands.extend(_glob_bd_under(d))
    for name in _BD_SUBDIRS:
        sd = os.path.join(d, name)
        if os.path.isdir(sd):
            cands.extend(_glob_bd_under(sd))
    return _pick_largest(_uniq_paths(cands))


def describe_ad_candidates(project_root: str) -> str:
    p = resolve_es1007ad_csv(project_root)
    return p or "(없음: data/ES1007AD.csv, ES1007AD*.csv, 하위폴더 ES1007AD|es1007AD/ 또는 ES1007AD00101MM2504_csv.csv)"


def describe_bd_candidates(project_root: str) -> str:
    p = resolve_es1007bd_csv(project_root)
    return p or "(없음: data/ES1007BD*.csv, data/ES1007BD|es1007BD/ 또는 ES1007BD00101MM2504_csv.csv)"
