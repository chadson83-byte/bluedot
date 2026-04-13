# -*- coding: utf-8 -*-
"""
볼륨 DB에 오아시스 테이블이 비어 있을 때만 CSV 적재(중복 배포 시 테이블 비우지 않음).
- 지하철(ES1007BD): 항상 시도(소용량).
- SNS(ES1007AD): BLUEDOT_AUTOIMPORT_SNS=1 일 때만(대용량·콜드스타트 수십 초~분).
- 상권활성도(ES1013): 테이블 비어 있고 data/ES1013.csv 또는 ES1013*.csv 가 있으면 시도.
- 주요상권(ES1001AY): trade_area_retail_kreb 비어 있고 data/ES1001AY.csv 가 있으면 시도.
- 상가공급(ES1007AC): oasis_retail_supply_ac 비어 있고 data/ES1007AC*.csv 가 있으면 시도.
"""
from __future__ import annotations

import glob
import os
import sqlite3
import subprocess
import sys

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


def _count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except sqlite3.OperationalError:
        return -1


def run(db_path: str | None = None) -> None:
    dbp = (db_path or os.environ.get("BLUEDOT_DB_PATH") or os.path.join(_BASE, "bluedot.db")).strip()
    if not os.path.isdir(os.path.dirname(os.path.abspath(dbp)) or "."):
        try:
            os.makedirs(os.path.dirname(os.path.abspath(dbp)), exist_ok=True)
        except OSError:
            pass
    conn = sqlite3.connect(dbp)
    try:
        n_sub = _count(conn, "subway_station_footfall")
        n_sns = _count(conn, "sns_floating_population")
        n_cv = _count(conn, "commercial_vitality_road")
        n_kreb = _count(conn, "trade_area_retail_kreb")
        n_ac = _count(conn, "oasis_retail_supply_ac")
    finally:
        conn.close()

    from engine.oasis_csv_resolve import resolve_es1007ad_csv, resolve_es1007bd_csv

    py = sys.executable
    if n_sub == 0:
        csv_b = resolve_es1007bd_csv(_BASE)
        if csv_b and os.path.isfile(csv_b):
            subprocess.run(
                [py, os.path.join(_BASE, "scripts", "import_subway_footfall_csv.py"), "--db", dbp, "--csv", csv_b],
                check=False,
            )
    if n_sns == 0 and (os.environ.get("BLUEDOT_AUTOIMPORT_SNS") or "").strip() == "1":
        csv_a = resolve_es1007ad_csv(_BASE)
        if csv_a and os.path.isfile(csv_a):
            subprocess.run(
                [py, os.path.join(_BASE, "scripts", "import_sns_floating_csv.py"), "--db", dbp, "--csv", csv_a],
                check=False,
            )

    if n_cv == 0:
        subprocess.run(
            [py, os.path.join(_BASE, "scripts", "import_commercial_vitality_csv.py"), "--db", dbp],
            check=False,
        )

    ay = os.path.join(_BASE, "data", "ES1001AY.csv")
    if n_kreb <= 0 and os.path.isfile(ay):
        subprocess.run(
            [py, os.path.join(_BASE, "scripts", "import_es1001ay_csv.py"), "--db", dbp, "--csv", ay],
            check=False,
        )

    if n_ac <= 0:
        ac_csv = os.path.join(_BASE, "data", "ES1007AC.csv")
        if not os.path.isfile(ac_csv):
            matches = sorted(glob.glob(os.path.join(_BASE, "data", "ES1007AC*.csv")))
            ac_csv = matches[0] if matches else ""
        if ac_csv and os.path.isfile(ac_csv):
            subprocess.run(
                [py, os.path.join(_BASE, "scripts", "import_es1007ac_csv.py"), "--db", dbp, "--csv", ac_csv],
                check=False,
            )


if __name__ == "__main__":
    run()
