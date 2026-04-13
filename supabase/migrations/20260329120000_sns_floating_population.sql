-- 오아시스비즈니스 SNS·유동 지수 (필지/법정동 단위)
-- Supabase(Postgres)용. 건물/필지 테이블이 있을 때만 FK를 활성화하세요.

CREATE TABLE IF NOT EXISTS sns_floating_population (
  data_strd_ym   TEXT NOT NULL,
  pnu            TEXT NOT NULL,
  legaldong_cd   TEXT NOT NULL,
  induty_cd      TEXT NOT NULL,
  fpop_scor      NUMERIC(8, 2) NOT NULL,
  clsf_no        INTEGER NOT NULL DEFAULT 0,
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (pnu, induty_cd, clsf_no, data_strd_ym)
);

CREATE INDEX IF NOT EXISTS idx_sns_fp_ldong ON sns_floating_population (legaldong_cd);
CREATE INDEX IF NOT EXISTS idx_sns_fp_ym ON sns_floating_population (data_strd_ym);
CREATE INDEX IF NOT EXISTS idx_sns_fp_induty ON sns_floating_population (induty_cd);
CREATE INDEX IF NOT EXISTS idx_sns_fp_pnu ON sns_floating_population (pnu);

COMMENT ON TABLE sns_floating_population IS 'SNS 노출·유동 프록시 지수(100점 만점). PNU로 필지 조인, legaldong_cd로 행정동 요약.';

-- 아래는 예시: 실제 프로젝트의 건물 테이블명·PNU 컬럼에 맞게 수정 후 적용
-- ALTER TABLE sns_floating_population
--   ADD CONSTRAINT fk_sns_pnu_building
--   FOREIGN KEY (pnu) REFERENCES buildings (pnu);
