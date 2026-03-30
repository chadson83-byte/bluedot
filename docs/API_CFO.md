# BLUEDOT Phase 1 — AI CFO & 타겟팅 API

백엔드 기준 URL: `http://127.0.0.1:8000` (로컬)

## 1. `POST /api/cfo/bep-simulate`

개원 직후 **손익분기(생존 견적)** — 의사·직원 수·평수 반영.

**요청 JSON**

| 필드 | 타입 | 기본값 | 설명 |
|------|------|--------|------|
| `lat`, `lng` | float | 필수 | 기준 좌표 |
| `dept` | string | `한의원` | 진료과목 |
| `radius_km` | float | `3` | 마스터 매칭 반경(km) |
| `doctors` | int | `1` | 의사 수 |
| `staff` | int | `4` | 직원 수 |
| `clinic_pyeong` | float | `35` | 임대 평수 |
| `variable_cost_ratio` | float | `0.12` | 변동비 비율(매출 대비) |

**응답**: `region_name`, `distance_km`, `activity_index`, `bep` (월 고정비, BEP 월·일 환자수, `headline` 문구 등)

---

## 2. `GET /api/cfo/survival`

**쿼리**: `lat`, `lng`, `dept`

상권 **폐업률·평균 생존 연수** 추정(V1은 결정론적 시뮬레이션).  
실데이터 연동 시 `data_source` 필드가 갱신됩니다.

---

## 3. `GET /api/cfo/rent-risk`

**쿼리**: `lat`, `lng`, `radius_km`(기본 `3`)

**임대료 상승 압력·젠트리피케이션** 리스크 등급.

---

## 4. `GET /api/geo/walkable-polygon`

**쿼리**: `lat`, `lng`, `minutes`(기본 `10`)

도보 범위 **GeoJSON Polygon**(V1은 원형 근사).  
실제 도보 isochrone은 카카오/TMAP 연동 예정.

---

## 5. `POST /api/targeting/persona-score`

**요청 JSON**: `lat`, `lng`, `dept`, `radius_km`(선택)

**페르소나 적합도**(직장인·가족·고령) 점수 및 요약 문장.

---

## 인증

위 엔드포인트는 **크레딧을 차감하지 않습니다**(컨설팅 조회·연동 테스트용).  
프로덕션에서 비로그인 차단이 필요하면 `Depends(_get_current_user_id)` 및 401 정책을 추가하세요.

## 프론트엔드 연동

- `index.html`에서 `window.BLUEDOT_API_BASE`로 백엔드 URL 설정 (기본 `http://127.0.0.1:8000`).
- **정밀 컨설팅 리포트** 모달을 열면 `app.js`의 `renderCfoPhase1Extended()`가 위 API 5종을 **병렬** 호출합니다.
- 리포트에 **저장된** `cfo_phase1` 캐시가 있으면 재요청 없이 즉시 표시합니다(마이페이지 열람 시).

## 데이터 계층

| 엔진 | 현재 | 목표 데이터 소스 |
|------|------|-------------------|
| BEP | 마스터 추정 임대·소비 | KOSIS 임대, 4대보험, 실거래 |
| 생존율 | 시뮬레이션 | 지방행정 인허가 개폐업 |
| 임대 리스크 | 활력 지수 프록시 | KOSIS 상업용 임대동향 |
| 도보 폴리곤 | 원 근사 | 카카오/TMAP 도보 API |
| 페르소나 | 인구·인프라 프록시 | SGIS 주간인구 + 국세청 소득 |
