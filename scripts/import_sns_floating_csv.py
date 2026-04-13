# -*- coding: utf-8 -*-
"""
오아시스 SNS·유동 CSV → SQLite sns_floating_population 적재.
- 구분자: `|` 또는 `,` (첫 데이터 행 기준 자동)
- 컬럼: 기존(data_strd_ym, pnu, legaldong_cd, induty_cd, fpop_scor, clsf_no)
  또는 명세 WW24 형(DATA_STDR_YM/DATA_STRD_YM, PNU, LEGALDONG_CD, INDUTY_CD, SNS_SCOR, FLOP_IND 등)

  python scripts/import_sns_floating_csv.py
  python scripts/import_sns_floating_csv.py --csv path/to/file.csv --db path/to/bluedot.db
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys

_CHUNK = 8000


def _detect_delimiter(sample_line: str) -> str:
    s = sample_line.strip()
    if not s:
        return "|"
    nc = s.count(",")
    np = s.count("|")
    return "|" if np > nc else ","


def _upper_row(row: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in row.items():
        if k is None:
            continue
        key = str(k).strip().upper()
        out[key] = "" if v is None else str(v).strip()
    return out


def _float_cell(u: dict[str, str], *keys: str) -> float | None:
    for k in keys:
        raw = (u.get(k) or "").strip()
        if not raw:
            continue
        try:
            return float(raw.replace(",", ""))
        except ValueError:
            continue
    return None


def _parse_row_to_tuple(row: dict) -> tuple[str, str, str, str, float, int] | None:
    """(ym, pnu, legaldong_cd, induty_cd, fpop_scor, clsf_no) 또는 None."""
    u = _upper_row(row)
    ym = (
        u.get("DATA_STRD_YM")
        or u.get("DATA_STDR_YM")
        or u.get("DATA_STD_YM")
        or u.get("STRD_YM")
        or ""
    ).strip()
    pnu = (u.get("PNU") or "").strip()
    ld = (u.get("LEGALDONG_CD") or u.get("LEGAL_DONG_CD") or "").strip()
    ind = (u.get("INDUTY_CD") or "").strip() or "_"
    cls_raw = u.get("CLSF_NO") or "0"
    try:
        cs = int(float(cls_raw))
    except ValueError:
        cs = 0

    sc_legacy = _float_cell(u, "FPOP_SCOR")
    sc_flop = _float_cell(u, "FLOP_IND", "FLOPIND")
    sc_sns = _float_cell(u, "SNS_SCOR", "SNSSCOR")

    if sc_legacy is not None:
        sc = sc_legacy
    elif sc_flop is not None and sc_sns is not None:
        sc = 0.55 * sc_flop + 0.45 * sc_sns
    elif sc_flop is not None:
        sc = sc_flop
    elif sc_sns is not None:
        sc = sc_sns
    else:
        sc = 0.0

    if not pnu or not ym:
        return None
    return (ym, pnu, ld, ind, float(sc), cs)


def main() -> int:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if base not in sys.path:
        sys.path.insert(0, base)
    from engine.oasis_csv_resolve import resolve_es1007ad_csv

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        default="",
        help="CSV (미지정 시 data/ES1007AD.csv 등 자동 탐색)",
    )
    ap.add_argument(
        "--db",
        default=os.environ.get("BLUEDOT_DB_PATH") or os.path.join(base, "bluedot.db"),
        help="SQLite DB path",
    )
    args = ap.parse_args()
    csv_path = (args.csv or "").strip()
    if not csv_path:
        csv_path = resolve_es1007ad_csv(base) or ""
    if not csv_path or not os.path.isfile(csv_path):
        print(
            "CSV not found. Place ES1007AD data as data/ES1007AD.csv or data/ES1007AD*.csv",
            file=sys.stderr,
        )
        return 1
    print("Using:", csv_path, flush=True)

    delim = "|"
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        for line in f:
            if line.strip():
                delim = _detect_delimiter(line)
                break

    conn = sqlite3.connect(args.db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sns_floating_population (
            data_strd_ym TEXT NOT NULL,
            pnu TEXT NOT NULL,
            legaldong_cd TEXT NOT NULL,
            induty_cd TEXT NOT NULL,
            fpop_scor REAL NOT NULL,
            clsf_no INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (pnu, induty_cd, clsf_no, data_strd_ym)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sns_fp_ldong ON sns_floating_population(legaldong_cd)")
    conn.execute("DELETE FROM sns_floating_population")
    conn.commit()

    batch: list = []
    n = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            tup = _parse_row_to_tuple(row)
            if not tup:
                continue
            batch.append(tup)
            n += 1
            if len(batch) >= _CHUNK:
                conn.executemany(
                    """INSERT OR REPLACE INTO sns_floating_population
                    (data_strd_ym, pnu, legaldong_cd, induty_cd, fpop_scor, clsf_no)
                    VALUES (?,?,?,?,?,?)""",
                    batch,
                )
                conn.commit()
                batch.clear()
                print(f"  ... {n}", flush=True)
        if batch:
            conn.executemany(
                """INSERT OR REPLACE INTO sns_floating_population
                (data_strd_ym, pnu, legaldong_cd, induty_cd, fpop_scor, clsf_no)
                VALUES (?,?,?,?,?,?)""",
                batch,
            )
            conn.commit()
    conn.close()
    print(f"Done. Rows inserted/updated: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
