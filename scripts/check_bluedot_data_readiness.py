# -*- coding: utf-8 -*-
"""
BLUEDOT 데이터·배포 점검 (로컬 또는 Fly SSH 안에서 실행).

  python scripts/check_bluedot_data_readiness.py
  BLUEDOT_DB_PATH=/data/bluedot.db python scripts/check_bluedot_data_readiness.py

배포 이미지에 이 파일이 없으면(구버전 이미지):
  python database.py  (로컬) / fly ssh -C "python /app/database.py"

Fly SSH 주의: -C 는 셸이 아니라 exec 한 줄이라 "cd /app && ..." 는 실패함.
  fly ssh console -a bluedot-backend-autumn-grass-4638 -C "python /app/database.py"
  fly ssh console -a bluedot-backend-autumn-grass-4638 -C "python /app/scripts/check_bluedot_data_readiness.py"
셸이 필요하면:
  fly ssh console -a bluedot-backend-autumn-grass-4638 -C "sh -lc 'cd /app && python database.py'"
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

_BASE = Path(__file__).resolve().parents[1]
os.chdir(_BASE)
sys.path.insert(0, str(_BASE))


def _count_table(conn: sqlite3.Connection, name: str) -> tuple[str, int | None, str | None]:
    try:
        n = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        return name, int(n), None
    except Exception as e:
        return name, None, str(e)


def main() -> int:
    db_path = (os.environ.get("BLUEDOT_DB_PATH") or "").strip() or str(_BASE / "bluedot.db")
    bjdong = _BASE / "data" / "법정동코드 전체자료.txt"
    es1013 = list(_BASE.glob("data/ES1013*.csv")) + list(_BASE.glob("data/ES1013*.CSV"))

    print("=== BLUEDOT data readiness ===")
    print(f"BLUEDOT_DB_PATH: {db_path}")
    print(f"DB file exists: {os.path.isfile(db_path)}")
    print(f"법정동 파일: {bjdong} -> {bjdong.is_file()} ({bjdong.stat().st_size if bjdong.is_file() else 0} bytes)")
    print(f"ES1013 CSV: {len(es1013)} file(s)" + (f" -> {es1013[0].name}" if es1013 else " (없음)"))

    kakao = (os.environ.get("KAKAO_REST_KEY") or "").strip()
    print(f"KAKAO_REST_KEY: {'설정됨 (' + str(len(kakao)) + ' chars)' if kakao else '비어 있음 (백엔드 시크릿 확인)'}")
    hira = (os.environ.get("HIRA_API_KEY") or "").strip()
    print(f"HIRA_API_KEY: {'설정됨' if hira else '비어 있음(내장 폴백 가능)'}")

    if not os.path.isfile(db_path):
        print("\n[요약] DB 파일이 없습니다. Fly 볼륨 경로·최초 기동·임포트를 확인하세요.")
        return 1

    conn = sqlite3.connect(db_path)
    for name in (
        "sns_floating_population",
        "commercial_vitality_road",
        "subway_station_footfall",
    ):
        t, n, err = _count_table(conn, name)
        if err:
            print(f"  {t}: 테이블 없음 또는 오류 — {err}")
        else:
            flag = "OK" if (n or 0) > 0 else "비어 있음"
            print(f"  {t}: {n:,} rows ({flag})")
    conn.close()

    print("\n[해석]")
    print("  - sns_floating_population=0 → import_sns_floating_csv.py 또는 BLUEDOT_AUTOIMPORT_SNS")
    print("  - commercial_vitality_road=0 → import_commercial_vitality_csv.py")
    print("  - KAKAO_REST_KEY 없음 → 트렌드(B코드)·상권활성도(시군구) 카카오 단계 실패")
    print("  - 로컬과 Fly 수치가 다르면 → 배포 이미지/볼륨 DB가 다르거나 임포트 미실행")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
