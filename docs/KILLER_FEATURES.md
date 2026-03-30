# BLUEDOT 킬러 기능 — 데이터 소스 & 구현 상태

| 기능 | 기대 효과 | 목표 데이터 소스 | 현재 구현 |
|------|-----------|------------------|-----------|
| 경쟁 병원 노후도 | 노후 경쟁사 대비 최신 인테리어·장비 포지셔닝 | 지방행정 인허가 최초 개업일 | `established_years` + ykiho 시드 기반 추정 (`years_since_opening_est`, `first_opening_year_est`) |
| 경쟁사 리뷰 | 낮은 별점 구간에서 서비스 차별화 | 카카오맵·네이버 플레이스 | `review_avg_stub` 결정론 스텁 + 내러티브 |
| 타임 매트릭스 | 피크 요일·시간대 야간진료 등 | SKT Data Hub / 서울시 생활인구 | 요일 지수 최댓값 기반 `killer_narrative`, `peak_time_suggestion` |
| 주차·인프라 | 공영주차·약국 동선 | 공공데이터포털 공영주차장 API + V6 `pharmacy_cnt` | 좌표 시드 + 마스터 약국 수 스텁 |

## API 필드

- `recommendations[].killer_insights`: `competitor_age_narrative`, `review_opportunity_narrative`, `parking_infra`
- `recommendations[].time_matrix`: `killer_narrative`, `peak_time_suggestion`, `peak_day`, `data_source_living_pop`
- `hospitals[]`: `review_avg_stub`, `facility_age_label`, `permit_data_source`, `review_data_source` 등 (지도·확장용)

## 프로덕션 시

1. 인허가: 행정안전부/지자체 오픈API로 `ykiho` 매핑 후 `first_opening_year` 치환  
2. 리뷰: 공식 API 또는 정책 준수 수집 파이프라인  
3. 유동: 통신·행정 실데이터로 `values[]` 및 피크 시간대 치환  
4. 주차: 공영주차장 좌표와 후보 입지 좌표 거리 매칭
