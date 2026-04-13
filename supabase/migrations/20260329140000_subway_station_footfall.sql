-- 오아시스 ES1007BD: 지하철 역사(시나리오) 반경 격자 유동(월간). CSV 컬럼명 기준.
CREATE TABLE IF NOT EXISTS subway_station_footfall (
  data_strd_ym      TEXT NOT NULL,
  subway_scn_innb   TEXT NOT NULL,
  subway_scn_nm     TEXT NOT NULL,
  subway_route_nm   TEXT,
  center_lat        DOUBLE PRECISION NOT NULL,
  center_lng        DOUBLE PRECISION NOT NULL,
  totl_fpop         DOUBLE PRECISION NOT NULL,
  male_fpop         DOUBLE PRECISION NOT NULL,
  female_fpop       DOUBLE PRECISION NOT NULL,
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (subway_scn_innb, data_strd_ym)
);

CREATE INDEX IF NOT EXISTS idx_subway_fp_ym ON subway_station_footfall (data_strd_ym);
CREATE INDEX IF NOT EXISTS idx_subway_fp_lat_lng ON subway_station_footfall (center_lat, center_lng);

COMMENT ON TABLE subway_station_footfall IS '역사 중심 반경 500m 격자 기준 월간 유동(총/남/여). 좌표로 최근접 허브 매칭.';
