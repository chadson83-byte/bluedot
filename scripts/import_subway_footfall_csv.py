# -*- coding: utf-8 -*-
"""
ES1007BD CSV → SQLite subway_station_footfall
기본: data/ES1007BD00101MM2504_csv.csv (구분자 |, UTF-8)

  python scripts/import_subway_footfall_csv.py
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys

_CHUNK = 500


def main() -> int:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if base not in sys.path:
        sys.path.insert(0, base)
    from engine.oasis_csv_resolve import resolve_es1007bd_csv

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        default="",
        help="미지정 시 data/ES1007BD00101MM2504_csv.csv 또는 ES1007BD*.csv 자동 탐색",
    )
    ap.add_argument(
        "--db",
        default=os.environ.get("BLUEDOT_DB_PATH") or os.path.join(base, "bluedot.db"),
    )
    args = ap.parse_args()
    csv_path = (args.csv or "").strip()
    if not csv_path:
        csv_path = resolve_es1007bd_csv(base) or ""
    if not csv_path or not os.path.isfile(csv_path):
        print("CSV not found. Place data/ES1007BD00101MM2504_csv.csv or data/ES1007BD*.csv", file=sys.stderr)
        return 1
    print("Using:", csv_path, flush=True)

    conn = sqlite3.connect(args.db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS subway_station_footfall (
            data_strd_ym TEXT NOT NULL,
            subway_scn_innb TEXT NOT NULL,
            subway_scn_nm TEXT NOT NULL,
            subway_route_nm TEXT,
            center_lat REAL NOT NULL,
            center_lng REAL NOT NULL,
            totl_fpop REAL NOT NULL,
            male_fpop REAL NOT NULL,
            female_fpop REAL NOT NULL,
            PRIMARY KEY (subway_scn_innb, data_strd_ym)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_subway_fp_ym ON subway_station_footfall(data_strd_ym)")
    conn.execute("DELETE FROM subway_station_footfall")
    conn.commit()

    batch = []
    n = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            try:
                ym = str(row.get("DATA_STRD_YM") or row.get("data_strd_ym") or "").strip()
                sid = str(row.get("SUBWAY_SCN_INNB") or row.get("subway_scn_innb") or "").strip()
                snm = str(row.get("SUBWAY_SCN_NM") or row.get("subway_scn_nm") or "").strip()
                rnm = str(row.get("SUBWAY_ROUTE_NM") or row.get("subway_route_nm") or "").strip()
                la = float(row.get("SUBWAY_SCN_CNTPNT_LA") or row.get("center_lat") or 0)
                lo = float(row.get("SUBWAY_SCN_CNTPNT_LO") or row.get("center_lng") or 0)
                tt = float(row.get("TOTL_FPOP") or row.get("totl_fpop") or 0)
                m = float(row.get("MALE_FPOP") or row.get("male_fpop") or 0)
                fm = float(row.get("FEMALE_FPOP") or row.get("female_fpop") or 0)
            except (TypeError, ValueError):
                continue
            if not ym or not sid or not snm:
                continue
            batch.append((ym, sid, snm, rnm or "", la, lo, tt, m, fm))
            n += 1
            if len(batch) >= _CHUNK:
                conn.executemany(
                    """INSERT OR REPLACE INTO subway_station_footfall
                    (data_strd_ym, subway_scn_innb, subway_scn_nm, subway_route_nm,
                     center_lat, center_lng, totl_fpop, male_fpop, female_fpop)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
                conn.commit()
                batch.clear()
        if batch:
            conn.executemany(
                """INSERT OR REPLACE INTO subway_station_footfall
                (data_strd_ym, subway_scn_innb, subway_scn_nm, subway_route_nm,
                 center_lat, center_lng, totl_fpop, male_fpop, female_fpop)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            conn.commit()
    conn.close()
    print(f"Done. Rows: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
