# -*- coding: utf-8 -*-
"""
오아시스 ES1007AC (유동인구 상가공급) CSV → SQLite oasis_retail_supply_ac.

  python scripts/import_es1007ac_csv.py
  python scripts/import_es1007ac_csv.py --csv path/to/ES1007AC.csv --db path/to/bluedot.db
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys


def _norm_header(h: str) -> str:
    return (h or "").strip().lstrip("\ufeff").lower()


def _f(v) -> float:
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def main() -> int:
    csv.field_size_limit(min(2**31 - 1, 50_000_000))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="", help="ES1007AC CSV (기본 data/ES1007AC.csv)")
    ap.add_argument(
        "--db",
        default=os.environ.get("BLUEDOT_DB_PATH") or os.path.join(base, "bluedot.db"),
    )
    args = ap.parse_args()
    csv_path = (args.csv or "").strip() or os.path.join(base, "data", "ES1007AC.csv")
    if not os.path.isfile(csv_path):
        print("CSV not found:", csv_path, file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oasis_retail_supply_ac (
            data_strd_ym TEXT NOT NULL,
            pnu TEXT NOT NULL,
            legaldong_cd TEXT NOT NULL,
            induty_cd TEXT NOT NULL,
            sopsrt_spl_dims REAL NOT NULL,
            clsf_no TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (data_strd_ym, pnu, induty_cd, clsf_no)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ac_ld_ym ON oasis_retail_supply_ac(data_strd_ym, legaldong_cd)"
    )
    conn.execute("DELETE FROM oasis_retail_supply_ac")
    conn.commit()

    ins = """
        INSERT OR REPLACE INTO oasis_retail_supply_ac (
            data_strd_ym, pnu, legaldong_cd, induty_cd, sopsrt_spl_dims, clsf_no
        ) VALUES (?,?,?,?,?,?)
    """
    n = 0
    batch: list = []
    _CHUNK = 3000
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="|", quotechar='"')
        if not reader.fieldnames:
            print("Empty header", file=sys.stderr)
            return 1
        for raw in reader:
            row = {_norm_header(k): (v.strip() if isinstance(v, str) else v) for k, v in raw.items() if k}
            ym = str(row.get("data_strd_ym") or "").strip()
            pnu = str(row.get("pnu") or "").strip()
            ld = str(row.get("legaldong_cd") or "").strip()
            ind = str(row.get("induty_cd") or "").strip()
            clsf = str(row.get("clsf_no") if row.get("clsf_no") is not None else "").strip()
            spl = _f(row.get("sopsrt_spl_dims"))
            if not ym or not pnu or not ld or not ind:
                continue
            batch.append((ym, pnu, ld, ind, spl, clsf))
            n += 1
            if len(batch) >= _CHUNK:
                conn.executemany(ins, batch)
                conn.commit()
                batch.clear()
    if batch:
        conn.executemany(ins, batch)
        conn.commit()
    conn.close()
    print("imported rows:", n)

    if base not in sys.path:
        sys.path.insert(0, base)
    try:
        from engine.retail_supply_ac import invalidate_retail_supply_ac_cache

        invalidate_retail_supply_ac_cache()
    except Exception as e:
        print("cache invalidate:", e, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
