# -*- coding: utf-8 -*-
"""
한국부동산원 ES1001AY (주요 상권별 상가영업 현황) → SQLite trade_area_retail_kreb.
구분자: 파이프(|), 필드 따옴표.

  python scripts/import_es1001ay_csv.py
  python scripts/import_es1001ay_csv.py --csv path/to/ES1001AY.csv --db path/to/bluedot.db
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
from typing import Optional, Tuple

from shapely import wkt as shp_wkt

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _f(v) -> float:
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _geom_bounds(wkt_s: str) -> Optional[Tuple[float, float, float, float]]:
    s = (wkt_s or "").strip()
    if not s:
        return None
    try:
        g = shp_wkt.loads(s)
        if g is None or g.is_empty:
            return None
        if not g.is_valid:
            g = g.buffer(0)
        b = g.bounds
        return (float(b[0]), float(b[1]), float(b[2]), float(b[3]))
    except Exception:
        return None


def main() -> int:
    csv.field_size_limit(min(2**31 - 1, 50_000_000))
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        default="",
        help="ES1001AY CSV (기본: data/ES1001AY.csv)",
    )
    ap.add_argument(
        "--db",
        default=os.environ.get("BLUEDOT_DB_PATH") or os.path.join(_BASE, "bluedot.db"),
    )
    args = ap.parse_args()
    csv_path = (args.csv or "").strip() or os.path.join(_BASE, "data", "ES1001AY.csv")
    if not os.path.isfile(csv_path):
        print("CSV not found:", csv_path, file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_area_retail_kreb (
            trdar_no TEXT NOT NULL PRIMARY KEY,
            trdar_nm TEXT NOT NULL,
            ctpr_nm TEXT NOT NULL,
            signgu_nm TEXT NOT NULL,
            opbn_rate REAL,
            bnse_rate REAL,
            cus_rate REAL,
            tcbiz_rate REAL,
            min_lng REAL NOT NULL,
            min_lat REAL NOT NULL,
            max_lng REAL NOT NULL,
            max_lat REAL NOT NULL,
            wkt TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ta_kreb_bbox ON trade_area_retail_kreb(min_lat, max_lat, min_lng, max_lng)"
    )
    conn.execute("DELETE FROM trade_area_retail_kreb")
    conn.commit()

    ins = """
        INSERT OR REPLACE INTO trade_area_retail_kreb (
            trdar_no, trdar_nm, ctpr_nm, signgu_nm,
            opbn_rate, bnse_rate, cus_rate, tcbiz_rate,
            min_lng, min_lat, max_lng, max_lat, wkt
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    n = 0
    skip = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter="|", quotechar='"')
        header = next(reader, None)
        if not header:
            print("Empty CSV", file=sys.stderr)
            return 1
        for raw in reader:
            if len(raw) < 9:
                skip += 1
                continue
            trdar_no = (raw[0] or "").strip()
            trdar_nm = (raw[1] or "").strip()
            ctpr_nm = (raw[2] or "").strip()
            signgu_nm = (raw[3] or "").strip()
            wkt_s = (raw[8] or "").strip()
            if not trdar_no or not wkt_s:
                skip += 1
                continue
            bb = _geom_bounds(wkt_s)
            if not bb:
                skip += 1
                continue
            min_lng, min_lat, max_lng, max_lat = bb
            conn.execute(
                ins,
                (
                    trdar_no,
                    trdar_nm,
                    ctpr_nm,
                    signgu_nm,
                    _f(raw[4]),
                    _f(raw[5]),
                    _f(raw[6]),
                    _f(raw[7]),
                    min_lng,
                    min_lat,
                    max_lng,
                    max_lat,
                    wkt_s,
                ),
            )
            n += 1
    conn.commit()
    conn.close()
    print("imported rows:", n, "skipped:", skip)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
