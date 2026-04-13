# -*- coding: utf-8 -*-
"""
MOCT 노드·링크 Shapefile → data/moct_network.sqlite (또는 BLUEDOT_MOCT_DB).

  python scripts/import_moct_nodelink.py

필요: pyproj, pyshp (pip install pyproj pyshp)
기본 경로: data/MOCT_NODE.shp, data/MOCT_LINK.dbf
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import struct
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

# 프로젝트 루트
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pyproj import CRS, Transformer  # type: ignore
import shapefile  # type: ignore

from engine.moct_network import moct_sqlite_path

_CELL_DEG = 0.025
_LAT0 = 33.0
_LNG0 = 124.5


def _grid_ij(lat: float, lng: float) -> Tuple[int, int]:
    gi = int((float(lat) - _LAT0) / _CELL_DEG)
    gj = int((float(lng) - _LNG0) / _CELL_DEG)
    return gi, gj


def _dbf_field_layout(dbf_path: str) -> Tuple[int, int, int, List[Tuple[str, int, int]]]:
    """(nrec, hlen, rlen, [(name, offset_in_record, length), ...])"""
    with open(dbf_path, "rb") as f:
        hdr = f.read(32)
        nrec = struct.unpack("<I", hdr[4:8])[0]
        hlen = struct.unpack("<H", hdr[8:10])[0]
        rlen = struct.unpack("<H", hdr[10:12])[0]
        f.seek(32)
        fields: List[Tuple[str, int, int]] = []
        pos = 1  # deletion flag
        while True:
            block = f.read(32)
            if not block or block[0] == 0x0D:
                break
            raw = block[:11].split(b"\x00")[0]
            name = raw.decode("latin-1", errors="replace").strip()
            flen = int(block[16])
            fields.append((name, pos, flen))
            pos += flen
    return nrec, hlen, rlen, fields


def _iter_dbf_records(
    dbf_path: str,
    field_names: Optional[Iterable[str]] = None,
) -> Iterable[Dict[str, Any]]:
    nrec, hlen, rlen, fields = _dbf_field_layout(dbf_path)
    wanted = {n.upper() for n in field_names} if field_names else None
    offsets = [(n, o, ln) for n, o, ln in fields if not wanted or n.upper() in wanted]
    with open(dbf_path, "rb") as f:
        f.seek(hlen)
        for _ in range(nrec):
            raw = f.read(rlen)
            if len(raw) < rlen:
                break
            if raw[0:1] == b"*":
                continue
            row: Dict[str, Any] = {}
            for name, off, ln in offsets:
                chunk = raw[off : off + ln]
                if not chunk:
                    row[name] = None
                    continue
                s = chunk.decode("cp949", errors="replace").strip()
                row[name] = s
            yield row


def _parse_node_id(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _parse_road_rank(val: Any) -> int:
    if val is None:
        return 999
    s = str(val).strip()
    if not s:
        return 999
    try:
        return int(s)
    except ValueError:
        return 999


def aggregate_link_metrics(link_dbf: str) -> Tuple[Dict[int, int], Dict[int, int]]:
    deg: Dict[int, int] = defaultdict(int)
    best: Dict[int, int] = {}
    for rec in _iter_dbf_records(link_dbf, ("F_NODE", "T_NODE", "ROAD_RANK")):
        fn = _parse_node_id(rec.get("F_NODE"))
        tn = _parse_node_id(rec.get("T_NODE"))
        rr = _parse_road_rank(rec.get("ROAD_RANK"))
        for nid in (fn, tn):
            if nid is None:
                continue
            deg[nid] += 1
            prev = best.get(nid, 999)
            if rr < prev:
                best[nid] = rr
    return dict(deg), best


def main() -> None:
    ap = argparse.ArgumentParser(description="MOCT 노드·링크 → SQLite")
    ap.add_argument("--node-shp", default=os.path.join(_ROOT, "data", "MOCT_NODE.shp"))
    ap.add_argument("--link-dbf", default=os.path.join(_ROOT, "data", "MOCT_LINK.dbf"))
    ap.add_argument("--out", default="", help="기본: engine.moct_network.moct_sqlite_path()")
    ap.add_argument("--batch", type=int, default=8000)
    args = ap.parse_args()
    out_path = (args.out or "").strip() or moct_sqlite_path()
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    prj_path = os.path.splitext(args.node_shp)[0] + ".prj"
    if not os.path.isfile(prj_path):
        raise SystemExit(f"prj 없음: {prj_path}")
    with open(prj_path, "r", encoding="utf-8", errors="replace") as f:
        wkt = f.read().strip()
    crs_from = CRS.from_wkt(wkt)
    to_wgs = Transformer.from_crs(crs_from, "EPSG:4326", always_xy=True)

    print("링크 집계 중…", args.link_dbf)
    deg, best_rank = aggregate_link_metrics(args.link_dbf)
    print(f"  노드 수(링크 기준): {len(deg):,}")

    if os.path.isfile(out_path):
        os.remove(out_path)
    conn = sqlite3.connect(out_path)
    try:
        conn.execute(
            """
            CREATE TABLE moct_nodes (
                node_id INTEGER PRIMARY KEY,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                grid_i INTEGER NOT NULL,
                grid_j INTEGER NOT NULL,
                link_degree INTEGER,
                best_road_rank INTEGER,
                node_type INTEGER
            )
            """
        )
        conn.execute("CREATE INDEX idx_moct_grid ON moct_nodes(grid_i, grid_j)")

        sf = shapefile.Reader(args.node_shp, encoding="cp949")
        shapes = sf.shapes()
        records = sf.records()
        field_names = [f[0].upper() for f in sf.fields[1:]]
        try:
            i_nid = field_names.index("NODE_ID")
            i_nty = field_names.index("NODE_TYPE")
        except ValueError as e:
            raise SystemExit(f"NODE 필드 없음: {field_names}") from e

        ins = """
            INSERT OR REPLACE INTO moct_nodes
            (node_id, lat, lng, grid_i, grid_j, link_degree, best_road_rank, node_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        batch: List[Tuple[Any, ...]] = []
        n_in = 0
        n_skip = 0

        pt_kind = getattr(shapefile, "POINT", 1)
        for idx, shp in enumerate(shapes):
            if shp.shapeType != pt_kind:
                n_skip += 1
                continue
            pts = shp.points
            if not pts:
                n_skip += 1
                continue
            xm, ym = float(pts[0][0]), float(pts[0][1])
            try:
                lng, lat = to_wgs.transform(xm, ym)
            except Exception:
                n_skip += 1
                continue
            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                n_skip += 1
                continue

            rec = records[idx]
            nid = _parse_node_id(rec[i_nid])
            if nid is None:
                n_skip += 1
                continue
            nty_raw = rec[i_nty] if i_nty < len(rec) else None
            nty: Optional[int] = None
            if nty_raw is not None:
                s = str(nty_raw).strip()
                if s:
                    try:
                        nty = int(s)
                    except ValueError:
                        d = re.sub(r"\D", "", s)
                        if d:
                            try:
                                nty = int(d)
                            except ValueError:
                                nty = None

            d = deg.get(nid, 0)
            br = best_rank.get(nid, 999)
            gi, gj = _grid_ij(lat, lng)
            batch.append((nid, lat, lng, gi, gj, d, br, nty))
            if len(batch) >= args.batch:
                conn.executemany(ins, batch)
                n_in += len(batch)
                batch.clear()
                print(f"  적재 {n_in:,} …")

        if batch:
            conn.executemany(ins, batch)
            n_in += len(batch)
        conn.commit()
        print(f"완료: {out_path} — 행 {n_in:,}, 스킵 {n_skip:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
