# -*- coding: utf-8 -*-
"""로컬 data/ 번들 점검 — 배포 전 터미널에서 실행.

  python scripts/verify_data_bundle.py
"""
from __future__ import annotations

import os
import sys

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


def _resolve_es1013(base: str) -> str | None:
    import glob

    d = os.path.join(base, "data")
    for name in ("ES1013.csv", "ES1013.CSV"):
        p = os.path.join(d, name)
        if os.path.isfile(p):
            return p
    for pat in ("ES1013*.csv", "ES1013*.CSV"):
        c = sorted(glob.glob(os.path.join(d, pat)))
        if c:
            return c[0]
    return None


def main() -> int:
    from engine.oasis_csv_resolve import resolve_es1007ad_csv, resolve_es1007bd_csv

    print("Project:", _BASE)
    ad = resolve_es1007ad_csv(_BASE)
    bd = resolve_es1007bd_csv(_BASE)
    es1013 = _resolve_es1013(_BASE)
    moct = os.path.join(_BASE, "data", "moct_network.sqlite")
    bjd = os.path.join(_BASE, "data", "법정동코드 전체자료.txt")

    def line(label: str, path: str | None) -> None:
        ok = path and os.path.isfile(path)
        sz = os.path.getsize(path) if ok else 0
        print(f"  [{'OK' if ok else '--'}] {label}: {path or '(없음)'}" + (f"  ({sz:,} bytes)" if ok else ""))

    print("Oasis / optional files:")
    line("SNS·유동 (ES1007AD)", ad)
    line("지하철 유동 (ES1007BD)", bd)
    line("상권활성도 (ES1013*)", es1013)
    line("MOCT SQLite", moct if os.path.isfile(moct) else None)
    line("법정동 txt", bjd if os.path.isfile(bjd) else None)

    master_v7 = os.path.join(_BASE, "bluedot_master_v7.csv")
    line("마스터 bluedot_master_v7.csv", master_v7 if os.path.isfile(master_v7) else None)

    if not ad:
        print("\n[필요] 트렌드 유동 카드: 오아시스 ES1007AD CSV를 data/에 두세요 (파이프 구분).")
    if not bd:
        print("\n[필요] 역세권 카드: ES1007BD CSV를 data/에 두세요.")
    if not es1013:
        print("\n[권장] 상권활성도: ES1013*.csv 를 data/에 두세요.")
    return 0 if ad and bd else 1


if __name__ == "__main__":
    raise SystemExit(main())
