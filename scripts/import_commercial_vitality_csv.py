# -*- coding: utf-8 -*-
"""
상권활성도지수(ES1013 계열) CSV → SQLite commercial_vitality_road.
공공데이터 명세: STRD_YR, CTPR_NM, SIGNGU_NM, RDNMADR, STRT_SMRD_CLSF, … VTLZ_IDEX
구분자는 파이프(|) 또는 쉼표(,) 자동 감지.

  python scripts/import_commercial_vitality_csv.py
  python scripts/import_commercial_vitality_csv.py --csv path/to/ES1013...csv --db path/to/bluedot.db
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys

_CHUNK = 5000


def _norm_header(h: str) -> str:
    return (h or "").strip().lstrip("\ufeff").upper()


def _detect_delimiter(sample: str) -> str:
    pipe = sample.count("|")
    comma = sample.count(",")
    return "|" if pipe > comma else ","


def _f(row: dict, *keys: str) -> float | None:
    for k in keys:
        v = row.get(k)
        if v is None or (isinstance(v, str) and not str(v).strip()):
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def resolve_default_es1013_csv(base: str) -> str | None:
    """data/ES1013.csv(단일 파일) 우선, 없으면 ES1013*.csv / ES1013*.CSV."""
    data_dir = os.path.join(base, "data")
    for name in ("ES1013.csv", "ES1013.CSV"):
        p = os.path.join(data_dir, name)
        if os.path.isfile(p):
            return p
    import glob

    for pat in ("ES1013*.csv", "ES1013*.CSV"):
        cand = sorted(glob.glob(os.path.join(data_dir, pat)))
        if cand:
            return cand[0]
    return None


def main() -> int:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        default="",
        help="ES1013 상권활성도 CSV 경로 (미지정 시 data/ 내 ES1013*.csv 탐색)",
    )
    ap.add_argument(
        "--db",
        default=os.environ.get("BLUEDOT_DB_PATH") or os.path.join(base, "bluedot.db"),
        help="SQLite DB path",
    )
    args = ap.parse_args()
    csv_path = (args.csv or "").strip()
    if not csv_path:
        csv_path = resolve_default_es1013_csv(base) or ""
        if not csv_path:
            print(
                "CSV not found. Place data/ES1013.csv (or ES1013*.csv) under data/ or pass --csv",
                file=sys.stderr,
            )
            return 1
        print("Using:", csv_path)
    if not os.path.isfile(csv_path):
        print("CSV not found:", csv_path, file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS commercial_vitality_road (
            strd_yr TEXT NOT NULL,
            ctpr_nm TEXT NOT NULL,
            signgu_nm TEXT NOT NULL,
            rdnmadr TEXT NOT NULL,
            strt_smrd_clsf TEXT NOT NULL DEFAULT '',
            bsnes_inde_cnt REAL,
            prvyy_bsnes_cnt REAL,
            bsnes_cnt REAL,
            idx_induty_1 REAL,
            idx_induty_2 REAL,
            idx_induty_3 REAL,
            idx_induty_4 REAL,
            idx_induty_wghsm REAL,
            frnchs_idx_induty_1 REAL,
            frnchs_idx_induty_2 REAL,
            frnchs_idx_induty_3 REAL,
            frnchs_idx_induty_4 REAL,
            frnchs_idx_induty_wghsm REAL,
            olnlp_exche_scor REAL,
            olnlp REAL,
            vtlz_idex REAL,
            PRIMARY KEY (strd_yr, ctpr_nm, signgu_nm, rdnmadr, strt_smrd_clsf)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv_sigungu ON commercial_vitality_road(strd_yr, ctpr_nm, signgu_nm)")
    conn.execute("DELETE FROM commercial_vitality_road")
    conn.commit()

    insert_sql = """
        INSERT OR REPLACE INTO commercial_vitality_road (
            strd_yr, ctpr_nm, signgu_nm, rdnmadr, strt_smrd_clsf,
            bsnes_inde_cnt, prvyy_bsnes_cnt, bsnes_cnt,
            idx_induty_1, idx_induty_2, idx_induty_3, idx_induty_4, idx_induty_wghsm,
            frnchs_idx_induty_1, frnchs_idx_induty_2, frnchs_idx_induty_3, frnchs_idx_induty_4, frnchs_idx_induty_wghsm,
            olnlp_exche_scor, olnlp, vtlz_idex
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    batch: list = []
    n = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(65536)
        f.seek(0)
        delim = _detect_delimiter(sample)
        reader = csv.DictReader(f, delimiter=delim)
        if not reader.fieldnames:
            print("Empty CSV header", file=sys.stderr)
            return 1

        for raw in reader:
            row = {(_norm_header(k)): (v.strip() if isinstance(v, str) else v) for k, v in raw.items() if k}
            yr = row.get("STRD_YR") or ""
            if isinstance(yr, str):
                yr = yr.strip()
            cp = (row.get("CTPR_NM") or "").strip() if row.get("CTPR_NM") else ""
            sg = (row.get("SIGNGU_NM") or "").strip() if row.get("SIGNGU_NM") else ""
            rd = (row.get("RDNMADR") or "").strip() if row.get("RDNMADR") else ""
            sm = (row.get("STRT_SMRD_CLSF") or "").strip() if row.get("STRT_SMRD_CLSF") is not None else ""
            if not yr or not cp or not sg or not rd:
                continue
            tup = (
                str(yr),
                cp,
                sg,
                rd,
                sm,
                _f(row, "BSNES_INDE_CNT"),
                _f(row, "PRVYY_BSNES_CNT"),
                _f(row, "BSNES_CNT"),
                _f(row, "IDX_INDUTY_1"),
                _f(row, "IDX_INDUTY_2"),
                _f(row, "IDX_INDUTY_3"),
                _f(row, "IDX_INDUTY_4"),
                _f(row, "IDX_INDUTY_WGHSM"),
                _f(row, "FRNCHS_IDX_INDUTY_1"),
                _f(row, "FRNCHS_IDX_INDUTY_2"),
                _f(row, "FRNCHS_IDX_INDUTY_3"),
                _f(row, "FRNCHS_IDX_INDUTY_4"),
                _f(row, "FRNCHS_IDX_INDUTY_WGHSM"),
                _f(row, "OLNLP_EXCHE_SCOR"),
                _f(row, "OLNLP"),
                _f(row, "VTLZ_IDEX"),
            )
            batch.append(tup)
            n += 1
            if len(batch) >= _CHUNK:
                conn.executemany(insert_sql, batch)
                conn.commit()
                batch.clear()

    if batch:
        conn.executemany(insert_sql, batch)
        conn.commit()
    conn.close()
    print("imported rows:", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
