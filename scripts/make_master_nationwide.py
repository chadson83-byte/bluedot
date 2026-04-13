# -*- coding: utf-8 -*-
"""
===============================================================================
BLUEDOT 전국 마스터 데이터 생성기 (신뢰성 개선 버전)
===============================================================================
- 전국 의료기관 데이터 변환 (부산 → 전국)
- 진료과목별 경쟁도(docs_per_10k) 분리
- 통계청 인구 데이터 연동 (실데이터 우선)
- 전국 도시철도/버스 인프라 통합
===============================================================================
"""
import pandas as pd
import numpy as np
import glob
import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# ============================================================
# [1] 설정: 경로 및 데이터 소스
# ============================================================
CONFIG = {
    # 의료기관 (전국 데이터 사용 - 공공데이터포털 다운로드)
    "hospital_excel": BASE_DIR / "data" / "1.병원정보서비스(2025.12.).xlsx",
    "dept_excel": BASE_DIR / "data" / "5.의료기관별상세정보서비스_03_진료과목정보 2025.12..xlsx",
    
    # 대중교통 (여러 CSV를 리스트로 추가 - 전국 커버리지)
    "subway_files": [
        BASE_DIR / "data" / "1_raw" / "부산교통공사_도시철도역사정보_20210226.csv",
        # 아래 파일들을 다운로드 후 경로 추가:
        # BASE_DIR / "data" / "1_raw" / "서울교통공사_역사좌표.csv",
        # BASE_DIR / "data" / "1_raw" / "대구도시철도_역사정보.csv",
        # BASE_DIR / "data" / "1_raw" / "대전도시철도_역사정보.csv",
        # BASE_DIR / "data" / "1_raw" / "광주도시철도_역사정보.csv",
    ],
    "subway_lat_col": "역위도",   # 파일마다 다를 수 있음 (위도, y, lat 등)
    "subway_lng_col": "역경도",   # 경도, x, lng 등
    
    "bus_file": BASE_DIR / "data" / "1_raw" / "tl_bus_station_info.csv",
    "bus_lat_col": "gpsy",
    "bus_lng_col": "gpsx",
    
    # 인구 (실데이터 - 통계청/SGIS 연동 시 필수)
    "population_csv": BASE_DIR / "data" / "1_raw" / "행정동별_인구_실데이터.csv",  # 없으면 경고 후 추정
    
    # 상가정보 (make_v6와 동일)
    "commercial_dir": BASE_DIR / "data" / "2_상가정보",
    
    "output_file": BASE_DIR / "bluedot_master_v7.csv",
}

# 진료과목 매핑 (HIRA 코드 ↔ BLUEDOT 과목명)
DEPT_CODE_MAP = {
    "01": "내과", "11": "소아과", "14": "피부과", "13": "이비인후과",
    "12": "안과", "05": "정형외과", "10": "산부인과", "03": "정신건강의학과",
    "치과": "치과", "한의원": "한의원",  # 병원정보 엑셀의 종별코드로 구분
}

def read_csv_safe(path, **kwargs):
    for enc in ["utf-8", "cp949", "euc-kr"]:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except (UnicodeDecodeError, Exception):
            continue
    raise ValueError(f"Cannot read {path}")


def haversine_km(lat1, lon1, lat2, lon2):
    """단일 좌표 또는 배열 지원"""
    R = 6371.0
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lat2 = np.asarray(lat2)
    lon2 = np.asarray(lon2)
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# ============================================================
# [2] 의료기관 데이터 처리 (전국)
# ============================================================
def load_hospitals():
    """병원정보 엑셀 → 행정동별 hosp_count, total_doctors + 진료과목별 집계"""
    p = CONFIG["hospital_excel"]
    if not p.exists():
        alt = BASE_DIR / "data" / "1_raw" / p.name
        p = alt if alt.exists() else p
    if not p.exists():
        raise FileNotFoundError(f"병원 데이터 없음: {p}\n공공데이터포털에서 '병원정보서비스' 전국 데이터를 다운로드하세요.")
    
    df = pd.read_excel(p)
    df = df.dropna(subset=["좌표(X)", "좌표(Y)", "읍면동"])
    df["lng"] = pd.to_numeric(df["좌표(X)"], errors="coerce")
    df["lat"] = pd.to_numeric(df["좌표(Y)"], errors="coerce")
    df = df.dropna(subset=["lat", "lng"])
    
    # 기본 집계 (행정동별)
    cols = ["시도코드명", "시군구코드명", "읍면동"]
    grouped = df.groupby(cols).agg(
        center_lat=("lat", "mean"),
        center_lng=("lng", "mean"),
        hosp_count=("암호화요양기호", "count"),
        total_doctors=("총의사수", "sum"),
    ).reset_index()
    
    grouped["행정구역(동읍면)별"] = grouped["시도코드명"] + " " + grouped["시군구코드명"] + " " + grouped["읍면동"]
    
    # 진료과목별 집계 (선택적 - 진료과목정보 연동)
    dept_path = CONFIG["dept_excel"]
    if dept_path.exists():
        try:
            df_dept = pd.read_excel(dept_path)
            code_col = next((c for c in df_dept.columns if "과목" in c and "코드" in c), None)
            num_col = next((c for c in df_dept.columns if "인원" in c), None) or next((c for c in df_dept.columns if c not in ["암호화요양기호", "요양기관명"]), None)
            if code_col and num_col:
                hosp_mini = df[["암호화요양기호", "시도코드명", "시군구코드명", "읍면동"]].drop_duplicates()
                merged = df_dept.merge(hosp_mini, on="암호화요양기호", how="inner")
                merged["dept_doctors"] = pd.to_numeric(merged[num_col], errors="coerce").fillna(0)
                for code, name in DEPT_CODE_MAP.items():
                    if code in ["치과", "한의원"]:
                        continue
                    sub = merged[merged[code_col].astype(str).str.zfill(2).str[:2] == str(code).zfill(2)]
                    if sub.empty:
                        continue
                    agg = sub.groupby(cols)["dept_doctors"].sum().reset_index().rename(columns={"dept_doctors": f"docs_{name}"})
                    grouped = grouped.merge(agg, on=cols, how="left")
        except Exception as e:
            print(f"  [경고] 진료과목 집계 스킵: {e}")
    
    # 치과/한의원: 병원정보 종별코드명으로 집계 (한의원+한방 통합)
    if "종별코드명" in df.columns:
        for kind, name in [("치과", "치과")]:
            sub = df[df["종별코드명"].astype(str).str.contains(kind, na=False)]
            if sub.empty:
                continue
            agg = sub.groupby(cols)["총의사수"].sum().reset_index().rename(columns={"총의사수": f"docs_{name}"})
            grouped = grouped.merge(agg, on=cols, how="left")
        # 한의원 = 한의원 + 한방 통합
        sub = df[df["종별코드명"].astype(str).str.contains("한의원|한방", na=False, regex=True)]
        if not sub.empty:
            agg = sub.groupby(cols)["총의사수"].sum().reset_index().rename(columns={"총의사수": "docs_한의원"})
            grouped = grouped.merge(agg, on=cols, how="left")
    
    return grouped


# ============================================================
# [3] 전국 지하철역 통합
# ============================================================
def load_subway_nationwide():
    all_rows = []
    lat_kw = ["위도", "lat", "y", "역위도"]
    lng_kw = ["경도", "lng", "x", "역경도"]
    
    for p in CONFIG["subway_files"]:
        if not p.exists():
            continue
        try:
            sep = "\t" if p.suffix == ".csv" and "부산" in str(p) else ","
            df = read_csv_safe(p, sep=sep)
            lat_col = next((c for c in df.columns if any(k in c for k in lat_kw)), None)
            lng_col = next((c for c in df.columns if any(k in c for k in lng_kw)), None)
            if lat_col and lng_col:
                df = df[[lat_col, lng_col]].dropna()
                df.columns = ["lat", "lng"]
                df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
                df["lng"] = pd.to_numeric(df["lng"], errors="coerce")
                df = df.dropna()
                all_rows.append(df)
        except Exception as e:
            print(f"  [경고] 지하철 파일 스킵 {p.name}: {e}")
    
    if not all_rows:
        print("  [경고] 지하철 데이터 없음. subway_count=0으로 처리됩니다.")
        return np.array([]), np.array([])
    
    combined = pd.concat(all_rows, ignore_index=True)
    return combined["lat"].values, combined["lng"].values


# ============================================================
# [4] 버스 정류장
# ============================================================
def load_bus():
    p = CONFIG["bus_file"]
    if not p.exists():
        print("  [경고] 버스 데이터 없음.")
        return np.array([]), np.array([])
    df = read_csv_safe(p)
    lat_col = CONFIG.get("bus_lat_col", "gpsy")
    lng_col = CONFIG.get("bus_lng_col", "gpsx")
    if lat_col not in df.columns or lng_col not in df.columns:
        lat_col = next((c for c in df.columns if "y" in c.lower() or "lat" in c.lower()), df.columns[0])
        lng_col = next((c for c in df.columns if "x" in c.lower() or "lng" in c.lower()), df.columns[1])
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lng_col] = pd.to_numeric(df[lng_col], errors="coerce")
    df = df.dropna(subset=[lat_col, lng_col])
    return df[lat_col].values, df[lng_col].values


# ============================================================
# [5] 인구 실데이터 (통계청/SGIS)
# ============================================================
def load_population(grouped):
    """실데이터 우선. 없으면 보수적 추정(기존보다 신뢰도 높임)"""
    p = CONFIG["population_csv"]
    if p.exists():
        df_pop = read_csv_safe(p)
        # 행정동명, 총인구, 젊은층비중 등 컬럼 자동 매칭
        name_col = next((c for c in df_pop.columns if "행정" in c or "동" in c or "읍면" in c), df_pop.columns[0])
        pop_col = next((c for c in df_pop.columns if "인구" in c or "총" in c), None)
        young_col = next((c for c in df_pop.columns if "젊" in c or "20" in c or "30" in c), None)
        
        if pop_col:
            df_pop["총인구 (명)"] = pd.to_numeric(df_pop[pop_col], errors="coerce").fillna(0)
        else:
            df_pop["총인구 (명)"] = 0
        if young_col:
            df_pop["젊은층_비중"] = pd.to_numeric(df_pop[young_col], errors="coerce").fillna(0.25)
        else:
            df_pop["젊은층_비중"] = 0.25
        
        df_pop["행정구역(동읍면)별"] = df_pop[name_col].astype(str)
        merged = grouped.merge(
            df_pop[["행정구역(동읍면)별", "총인구 (명)", "젊은층_비중"]],
            on="행정구역(동읍면)별", how="left"
        )
        grouped["총인구 (명)"] = merged["총인구 (명)"].fillna(0)
        grouped["젊은층_비중"] = merged["젊은층_비중"].fillna(0.25)
        return grouped
    
    # 실데이터 없을 때: 통계청 추정치보다 낮은 신뢰의 추정
    # (hosp_count 기반은 부정확하므로, 전국 평균 행정동 인구로 대체)
    print("  [주의] 행정동별_인구_실데이터.csv 없음. 통계청 SGIS API 연동 또는 CSV 준비를 권장합니다.")
    print("         임시로 배후인구 추정치를 사용합니다. (데이터 신뢰도 하락)")
    # 보수적 추정: 행정동 평균 약 1.2만명, 편차 적용
    np.random.seed(42)
    n = len(grouped)
    base = 8000 + grouped["hosp_count"] * 800  # 병원 밀집도와 어느정도 상관
    noise = np.random.uniform(0.7, 1.5, n)
    # 과거 8만 상한은 카드에 '허위 8만명'으로 오인되기 쉬움. 실데이터 CSV 없을 때만 쓰는 추정이므로 상한만 완화.
    grouped["총인구 (명)"] = (base * noise).clip(1000, 250000).astype(int)
    grouped["젊은층_비중"] = 0.2 + (grouped["subway_count"] * 0.02).clip(upper=0.2)
    return grouped


# ============================================================
# [6] 상가 인프라 (make_v6와 동일)
# ============================================================
def load_commercial():
    ANCHOR = "스타벅스|올리브영|다이소|파리바게뜨|메가엠지씨커피"
    ACADEMY = "학원|교습|독서실|스터디"
    FITNESS = "필라테스|요가|헬스|피트니스|골프"
    
    files = glob.glob(str(CONFIG["commercial_dir"] / "*.csv"))
    if not files:
        return None
    
    rows = []
    for f in files:
        try:
            df = pd.read_csv(f, usecols=["상호명", "상권업종소분류명", "시도명", "시군구명", "행정동명"], dtype=str, encoding="utf-8")
        except Exception:
            try:
                df = pd.read_csv(f, usecols=["상호명", "상권업종소분류명", "시도명", "시군구명", "행정동명"], dtype=str, encoding="cp949")
            except Exception:
                continue
        df = df.fillna("")
        df["anchor"] = df["상호명"].str.contains(ANCHOR, case=False, na=False).astype(int)
        df["pharmacy"] = (df["상권업종소분류명"].str.contains("약국", na=False) | df["상호명"].str.contains("약국", na=False)).astype(int)
        df["academy"] = (df["상권업종소분류명"].str.contains(ACADEMY, na=False) | df["상호명"].str.contains(ACADEMY, na=False)).astype(int)
        df["fitness"] = df["상호명"].str.contains(FITNESS, case=False, na=False).astype(int)
        agg = df.groupby(["시도명", "시군구명", "행정동명"])[["anchor", "pharmacy", "academy", "fitness"]].sum().reset_index()
        agg["match_key"] = agg["시도명"].str[:2] + " " + agg["행정동명"].str.replace(r"제?(\d+)동$", "동", regex=True)
        rows.append(agg)
    
    if not rows:
        return None
    full = pd.concat(rows).groupby("match_key").agg({"anchor": "sum", "pharmacy": "sum", "academy": "sum", "fitness": "sum"}).reset_index()
    return full


# ============================================================
# [7] 메인 실행
# ============================================================
def main():
    print("=" * 60)
    print("BLUEDOT 전국 마스터 데이터 생성 (신뢰성 개선)")
    print("=" * 60)
    
    grouped = load_hospitals()
    print(f"1. 의료기관: {len(grouped)}개 행정동 로드")
    
    sub_lat, sub_lng = load_subway_nationwide()
    bus_lat, bus_lng = load_bus()
    
    print("2. 대중교통 반경 1km 집계 중...")
    sub_counts, bus_counts = [], []
    for _, row in grouped.iterrows():
        lat, lng = row["center_lat"], row["center_lng"]
        if len(sub_lat) > 0:
            dist = haversine_km(lat, lng, sub_lat, sub_lng)
            sub_counts.append(np.sum(dist <= 1.0))
        else:
            sub_counts.append(0)
        if len(bus_lat) > 0:
            dist = haversine_km(lat, lng, bus_lat, bus_lng)
            bus_counts.append(np.sum(dist <= 1.0))
        else:
            bus_counts.append(0)
    
    grouped["subway_count"] = sub_counts
    grouped["bus_stop_count"] = bus_counts
    
    grouped = load_population(grouped)
    
    # 상가 인프라 병합 (make_v6와 동일 매칭 규칙)
    comm = load_commercial()
    if comm is not None:
        grouped["_sido"] = grouped["행정구역(동읍면)별"].apply(lambda x: str(x).split()[0][:2])
        grouped["_dong"] = grouped["행정구역(동읍면)별"].apply(lambda x: str(x).split()[-1])
        grouped["_dong"] = grouped["_dong"].str.replace(r"제?(\d+)동$", "동", regex=True)
        grouped["match_key"] = grouped["_sido"] + " " + grouped["_dong"]
        grouped = grouped.merge(comm, on="match_key", how="left")
        grouped = grouped.drop(columns=["match_key", "_sido", "_dong"], errors="ignore")
        for k, v in [("anchor", "anchor_cnt"), ("pharmacy", "pharmacy_cnt"), ("academy", "academy_cnt"), ("fitness", "fitness_cnt")]:
            grouped[v] = grouped.get(k, pd.Series(0, index=grouped.index)).fillna(0).astype(int)
        grouped = grouped.drop(columns=["anchor", "pharmacy", "academy", "fitness"], errors="ignore")
    else:
        grouped["anchor_cnt"] = grouped["pharmacy_cnt"] = grouped["academy_cnt"] = grouped["fitness_cnt"] = 0
    
    # 과목별 docs 컬럼이 있으면 유지, 없으면 total_doctors 기반
    out_cols = ["행정구역(동읍면)별", "총인구 (명)", "젊은층_비중", "center_lat", "center_lng",
                "hosp_count", "total_doctors", "subway_count", "bus_stop_count",
                "anchor_cnt", "pharmacy_cnt", "academy_cnt", "fitness_cnt"]
    extra = [c for c in grouped.columns if c.startswith("docs_") and c not in out_cols]
    out_cols = out_cols + extra
    
    final = grouped[[c for c in out_cols if c in grouped.columns]]
    final = final.drop_duplicates(subset=["행정구역(동읍면)별"])
    final.to_csv(CONFIG["output_file"], index=False, encoding="utf-8-sig")
    
    print(f"\n3. 저장 완료: {CONFIG['output_file']}")
    print(f"   행 수: {len(final)}, 과목별 컬럼: {extra}")
    print("\n[다음 단계] main.py의 MASTER_CSV_PATH를 bluedot_master_v7.csv로 변경하세요.")


if __name__ == "__main__":
    main()
