# -*- coding: utf-8 -*-
"""
경쟁 병원 입지 요약: PostGIS 도보 네트워크(교차로 프록시) + 법정동 단위 SNS 유동 격자 백분위.
"""
from __future__ import annotations

import bisect
import logging
import os
import re
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from engine.commercial_vitality import lookup_vitality_for_coordinates
from engine.sns_floating import (
    dept_weighted_fpop_score,
    resolve_fpop_for_coordinates,
)
from engine.subway_floating import _proxy_0_100_for_nearest
from engine.walkable_phase2 import Phase2Config
from engine.moct_network import (
    lookup_moct_nearest,
    merge_walk_network_postgis_moct,
    moct_narrative_ko,
)

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None


def _db_path() -> str:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p = (os.environ.get("BLUEDOT_DB_PATH") or "").strip()
    if p:
        return p
    return os.path.join(base, "bluedot.db")


def _postgis_walk_metrics(lat: float, lon: float, cfg: Phase2Config) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "postgis_skipped": True,
        "skip_reason": None,
        "near_intersection_50m": None,
        "strong_junction_35m": None,
        "max_vertex_degree": None,
        "min_vertex_dist_m": None,
    }
    if psycopg2 is None:
        out["skip_reason"] = "psycopg2_missing"
        return out
    if not cfg.use_pgr_network:
        out["skip_reason"] = "postgis_disabled_env"
        return out
    sql = """
    WITH pt AS (
        SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS g
    ),
    near_v AS (
        SELECT
            v.id,
            ST_Distance(v.the_geom::geography, (SELECT g FROM pt)) AS dist_m,
            (
                SELECT COUNT(*)::int FROM ways w
                WHERE w.source = v.id OR w.target = v.id
            ) AS deg
        FROM ways_vertices_pgr v
        WHERE ST_DWithin(v.the_geom::geography, (SELECT g FROM pt), 50)
    )
    SELECT
        COALESCE(BOOL_OR(deg >= 3), false) AS any_junction_50m,
        COALESCE(BOOL_OR(deg >= 4 AND dist_m <= 35), false) AS strong_corner,
        COALESCE(MAX(deg), 0) AS max_deg,
        COALESCE(MIN(dist_m), NULL) AS min_dist
    FROM near_v
    """
    try:
        conn = psycopg2.connect(
            host=cfg.db_host,
            port=cfg.db_port,
            dbname=cfg.db_name,
            user=cfg.db_user,
            password=cfg.db_password,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (float(lon), float(lat)))
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        logging.warning("competitor_analysis PostGIS: %s", e)
        out["skip_reason"] = f"postgis_error:{type(e).__name__}"
        return out

    if not row:
        out["postgis_skipped"] = False
        out["skip_reason"] = "empty_result"
        return out

    any_j, strong_c, max_deg, min_dist = row
    out["postgis_skipped"] = False
    out["skip_reason"] = None
    out["near_intersection_50m"] = bool(any_j)
    out["strong_junction_35m"] = bool(strong_c)
    out["max_vertex_degree"] = int(max_deg or 0)
    out["min_vertex_dist_m"] = float(min_dist) if min_dist is not None else None
    return out


def _stats_from_sns_rows(rows: List[Tuple[str, float]]) -> Dict[str, float]:
    as_, bs, cs, all_ = [], [], [], []
    for induty, sc in rows:
        try:
            v = float(sc)
        except Exception:
            continue
        s = str(induty or "").strip()
        all_.append(v)
        if s.startswith("A"):
            as_.append(v)
        elif s.startswith("B"):
            bs.append(v)
        elif s.startswith("C"):
            cs.append(v)

    def avg(xs: List[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    m = avg(all_)
    return {
        "mean": m,
        "mean_A": avg(as_) if as_ else m,
        "mean_B": avg(bs) if bs else m,
        "mean_C": avg(cs) if cs else m,
        "max": max(all_) if all_ else 0.0,
    }


def _latest_sns_ym(conn: sqlite3.Connection) -> Optional[str]:
    r = conn.execute("SELECT MAX(data_strd_ym) FROM sns_floating_population").fetchone()
    if not r or not r[0]:
        return None
    return str(r[0]).strip()


def _legaldong_for_pnu(conn: sqlite3.Connection, pnu: str, ym: str) -> Optional[str]:
    row = conn.execute(
        "SELECT legaldong_cd FROM sns_floating_population WHERE pnu = ? AND data_strd_ym = ? LIMIT 1",
        (pnu, ym),
    ).fetchone()
    if not row or not row[0]:
        return None
    return re.sub(r"\D", "", str(row[0]))


def _fpop_percentile_within_dong(
    *,
    legaldong_digits: str,
    dept: str,
    competitor_score: float,
    pnu: Optional[str],
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "legaldong_cd": legaldong_digits[:10] if legaldong_digits else None,
        "percentile_within_dong": None,
        "pnu_scores_n": 0,
        "competitor_fpop_weighted": round(competitor_score, 2),
        "used_pnu": bool(pnu),
    }
    ld = re.sub(r"\D", "", str(legaldong_digits or ""))
    if len(ld) < 8:
        return meta
    ld8 = ld[:8]
    path = _db_path()
    if not os.path.isfile(path):
        return meta
    try:
        conn = sqlite3.connect(path)
        try:
            ym = _latest_sns_ym(conn)
            if not ym:
                return meta
            # PNU별 N+1 쿼리는 수천 회 → 단일 스캔으로 그룹화 (Fly·로컬 타임아웃 방지)
            by_pnu: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
            cur = conn.execute(
                """
                SELECT pnu, induty_cd, fpop_scor FROM sns_floating_population
                WHERE data_strd_ym = ?
                  AND (legaldong_cd = ? OR substr(legaldong_cd, 1, 8) = ?)
                """,
                (ym, ld8, ld8),
            )
            for pnu_val, induty, sc in cur:
                try:
                    by_pnu[str(pnu_val)].append((str(induty or ""), float(sc)))
                except (TypeError, ValueError):
                    continue
            scores: List[float] = []
            for rows in by_pnu.values():
                st = _stats_from_sns_rows(rows)
                if not st or st["mean"] <= 0:
                    continue
                scores.append(dept_weighted_fpop_score(st, dept))
            meta["pnu_scores_n"] = len(scores)
            if not scores:
                return meta
            scores.sort()
            pct = 100.0 * bisect.bisect_right(scores, competitor_score) / len(scores)
            meta["percentile_within_dong"] = round(pct, 2)
        finally:
            conn.close()
    except Exception as e:
        logging.warning("competitor_analysis fpop percentile: %s", e)
    return meta


_RADAR_LABELS = ["SNS·유동", "도보·교차", "역세권", "상권활력", "네트워크밀착"]


def _radar_axis_scores(
    walk: Dict[str, Any],
    fpop_0_100: Optional[float],
    sub_hit: Optional[Tuple[float, Dict[str, Any]]],
    vit_0_100: Optional[float],
) -> List[float]:
    """역세권·상권활력은 호출부에서 1회만 계산해 넘김(중복 HTTP/SQLite 방지)."""
    sns_v = float(fpop_0_100) if fpop_0_100 is not None else 35.0

    md = walk.get("max_vertex_degree")
    if walk.get("postgis_skipped") or md is None:
        cross_v = 38.0
    else:
        d = int(md)
        cross_v = max(0.0, min(100.0, float(max(0, d - 1)) * 22.0))

    sub_v = float(sub_hit[0]) if sub_hit else 32.0
    vit_v = float(vit_0_100) if vit_0_100 is not None else 35.0

    mdm = walk.get("min_vertex_dist_m")
    if mdm is None or walk.get("postgis_skipped"):
        net_v = 40.0
    else:
        net_v = max(0.0, min(100.0, 100.0 * max(0.0, 1.0 - float(mdm) / 55.0)))

    return [sns_v, cross_v, sub_v, vit_v, net_v]


def _build_badges(
    walk: Dict[str, Any],
    fpop_meta: Dict[str, Any],
    subway_hit: Optional[Tuple[float, Dict[str, Any]]],
) -> List[str]:
    badges: List[str] = []
    mdm = walk.get("min_vertex_dist_m")
    if not walk.get("postgis_skipped") and mdm is not None and float(mdm) <= 35:
        badges.append("#대로변")
    if walk.get("near_intersection_50m"):
        badges.append("#교차로_인접")
    if walk.get("strong_junction_35m"):
        badges.append("#횡단보도_앞")
    pct = fpop_meta.get("percentile_within_dong")
    if pct is not None:
        if pct >= 99:
            badges.append("#유동인구_Top1%")
        elif pct >= 95:
            badges.append("#유동인구_Top5%")
        elif pct >= 90:
            badges.append("#유동인구_Top10%")
    if subway_hit and float(subway_hit[0]) >= 72:
        badges.append("#역세권_허브")
    br_m = walk.get("moct_best_road_rank")
    dm_m = walk.get("moct_nearest_dist_m")
    if br_m is not None and dm_m is not None:
        try:
            if int(br_m) <= 104 and float(dm_m) <= 280:
                badges.append("#국가도로망_접점")
        except (TypeError, ValueError):
            pass
    if not badges:
        badges.append("#입지_분석")
    return badges


def _narrative_ko(
    *,
    dept: str,
    walk: Dict[str, Any],
    fpop_meta: Dict[str, Any],
    mean_f: Optional[float],
    subway_meta: Optional[Dict[str, Any]],
) -> str:
    parts: List[str] = []
    src = str(walk.get("road_network_source") or "")
    if src == "moct_vehicle":
        parts.append(moct_narrative_ko(walk.get("moct_nearest")))
    elif walk.get("postgis_skipped"):
        parts.append(
            "도보 네트워크(PostGIS) 연결이 없어 교차로·도로 접점은 추정치로 제한됩니다. "
            "로컬에 OSM+pgRouting을 올리면 반경 50m 교차로 프록시가 활성화됩니다."
        )
    else:
        if walk.get("strong_junction_35m"):
            parts.append(
                "도보 그래프 기준 복합 노드(4지 이상 분기)가 가깝습니다. 실제 횡단보도 여부는 현장 확인이 필요하지만, "
                "유동이 합류하기 쉬운 코너형 입지 패턴과 유사합니다."
            )
        elif walk.get("near_intersection_50m"):
            parts.append("반경 50m 내 도보 네트워크 교차·분기 지점이 있어 시선·동선 교차 가능성이 큽니다.")
        if walk.get("min_vertex_dist_m") is not None:
            parts.append(
                f"가장 가까운 보행 네트워크 노드까지 약 {walk['min_vertex_dist_m']:.0f}m입니다."
            )
    if src == "postgis_pedestrian" and walk.get("moct_nearest"):
        mh = walk["moct_nearest"]
        if isinstance(mh, dict):
            parts.append(
                f"차량 도로망(MOCT) 최근접 노드는 약 {float(mh.get('nearest_dist_m') or 0):.0f}m·연결링크 "
                f"{int(mh.get('link_degree') or 0)}개 수준으로, 대로 접근성 보조 근거에 활용했습니다."
            )
    pct = fpop_meta.get("percentile_within_dong")
    ngrid = fpop_meta.get("pnu_scores_n")
    if pct is not None and ngrid:
        parts.append(
            f"같은 법정동(또는 8자리 매칭) 안 SNS·유동 격자 {ngrid}개 대비, "
            f"과목 가중 유동 지수는 상위 약 {pct:.1f}% 구간으로 해석됩니다."
        )
    elif mean_f is not None:
        parts.append(
            f"해당 법정동 평균 SNS·유동 프록시는 약 {mean_f:.1f}/100 수준입니다."
        )
    if subway_meta and subway_meta.get("nearest_nm"):
        parts.append(
            f"지하철 「{subway_meta.get('nearest_nm')}」 약 {subway_meta.get('nearest_dist_m', 0):.0f}m로 역세권 유동과의 궁합을 점검했습니다."
        )
    parts.append(
        f"기존 {dept} 기관이 밀집한 구간은 수요·인지도 측면에서 ‘검증된 상권’일 가능성이 있으나, "
        "그만큼 차별화·운영 전략이 중요합니다."
    )
    return " ".join(parts)


def analyze_competitor_location(
    *,
    competitor_lat: float,
    competitor_lng: float,
    dept: str,
    kakao_rest_key: str,
    phase2_config: Phase2Config,
    candidate_lat: Optional[float] = None,
    candidate_lng: Optional[float] = None,
    pnu: Optional[str] = None,
) -> Dict[str, Any]:
    dept = (dept or "").strip() or "한의원"
    lat, lng = float(competitor_lat), float(competitor_lng)

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_walk = pool.submit(_postgis_walk_metrics, lat, lng, phase2_config)
        fut_fpop = pool.submit(resolve_fpop_for_coordinates, lat, lng, kakao_rest_key)
        walk = fut_walk.result()
        walk = merge_walk_network_postgis_moct(walk, lookup_moct_nearest(lat, lng))
        mean_f, stats, b10, _ = fut_fpop.result()

    f_weighted = dept_weighted_fpop_score(stats, dept) if stats else None
    if f_weighted is None:
        f_weighted = float(mean_f or 0.0)

    ld_digits = ""
    if pnu and (pnu or "").strip():
        path = _db_path()
        if os.path.isfile(path):
            try:
                conn = sqlite3.connect(path)
                try:
                    ym = _latest_sns_ym(conn)
                    if ym:
                        ld = _legaldong_for_pnu(conn, str(pnu).strip(), ym)
                        if ld:
                            ld_digits = ld
                        rows = conn.execute(
                            """
                            SELECT induty_cd, fpop_scor FROM sns_floating_population
                            WHERE pnu = ? AND data_strd_ym = ?
                            """,
                            (str(pnu).strip(), ym),
                        ).fetchall()
                        if rows:
                            st = _stats_from_sns_rows([(str(a), float(b)) for a, b in rows])
                            f_weighted = dept_weighted_fpop_score(st, dept)
                finally:
                    conn.close()
            except Exception as e:
                logging.debug("competitor_analysis pnu lookup: %s", e)
    if not ld_digits and b10:
        ld_digits = re.sub(r"\D", "", str(b10))

    fpop_meta = _fpop_percentile_within_dong(
        legaldong_digits=ld_digits,
        dept=dept,
        competitor_score=float(f_weighted),
        pnu=str(pnu).strip() if pnu else None,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_sub = pool.submit(_proxy_0_100_for_nearest, lat, lng)
        fut_vit = pool.submit(lookup_vitality_for_coordinates, lat, lng, kakao_rest_key)
        sub_hit = fut_sub.result()
        vit_sc, _ = fut_vit.result()
    vit_v = float(vit_sc) if vit_sc is not None else None
    badges = _build_badges(walk, fpop_meta, sub_hit)

    comp_radar = _radar_axis_scores(walk, f_weighted, sub_hit, vit_v)
    cand_radar: Optional[List[float]] = None
    if candidate_lat is not None and candidate_lng is not None:
        cla, cln = float(candidate_lat), float(candidate_lng)
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_w2 = pool.submit(_postgis_walk_metrics, cla, cln, phase2_config)
            fut_fp2 = pool.submit(resolve_fpop_for_coordinates, cla, cln, kakao_rest_key)
            w2 = fut_w2.result()
            w2 = merge_walk_network_postgis_moct(w2, lookup_moct_nearest(cla, cln))
            mean2, stats2, _, _ = fut_fp2.result()
        if stats2:
            fw2 = dept_weighted_fpop_score(stats2, dept)
        else:
            fw2 = float(mean2 or 0.0)
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_s2 = pool.submit(_proxy_0_100_for_nearest, cla, cln)
            fut_v2 = pool.submit(lookup_vitality_for_coordinates, cla, cln, kakao_rest_key)
            sub2 = fut_s2.result()
            vit2_sc, _ = fut_v2.result()
        vit2_v = float(vit2_sc) if vit2_sc is not None else None
        cand_radar = _radar_axis_scores(w2, fw2, sub2, vit2_v)

    narr = _narrative_ko(
        dept=dept,
        walk=walk,
        fpop_meta=fpop_meta,
        mean_f=mean_f,
        subway_meta=sub_hit[1] if sub_hit else None,
    )

    return {
        "badges": badges,
        "narrative_ko": narr,
        "radar": {
            "labels": list(_RADAR_LABELS),
            "competitor": [round(x, 1) for x in comp_radar],
            "candidate": [round(x, 1) for x in cand_radar] if cand_radar else None,
        },
        "walk_network": walk,
        "fpop": {
            "weighted_0_100": round(float(f_weighted), 2),
            "dong_mean_0_100": round(float(mean_f), 2) if mean_f is not None else None,
            "legaldong_b_code": b10,
            "percentile_within_dong": fpop_meta.get("percentile_within_dong"),
            "dong_grid_count": fpop_meta.get("pnu_scores_n"),
        },
        "subway": (
            {
                "proxy_0_100": round(float(sub_hit[0]), 2),
                "nearest_nm": sub_hit[1].get("nearest_nm"),
                "nearest_dist_m": sub_hit[1].get("nearest_dist_m"),
            }
            if sub_hit
            else None
        ),
    }
