# BLUEDOT 프로덕션 배포 전 QA 감사 리포트

**최종 수정 일자:** 2025-03-22  
**수정 상태:** CB-1 ~ CB-5 모두 코드 반영 완료 ✅

### 프로덕션 배포 시 필수 설정
| 환경변수 | 설명 | 프로덕션 |
|---------|------|----------|
| `BLUEDOT_TEST_MODE` | 1이면 imp_uid 없이 결제 API 허용 | **설정하지 않음** 또는 `0` |
| `PORTONE_API_KEY` | 포트원 REST API 키 | 필수 설정 |
| `PORTONE_API_SECRET` | 포트원 REST API 시크릿 | 필수 설정 |
| `BLUEDOT_JWT_SECRET` | JWT 서명용 시크릿 | 강한 값으로 변경 |

로컬 개발 시: `BLUEDOT_TEST_MODE=1` 로 테스트 결제 가능 (imp_uid 불필요).

---

## 1. 🚨 Critical Blockers (배포 전 무조건 수정)

### CB-1. 결제 검증 없이 크레딧 무제한 적립 가능 (치명적) — ✅ 수정 완료
**발생 조건:** 로그인한 사용자가 `/api/credits/add`를 직접 POST 호출  
**문제:** (동일)

**적용 수정:**
- `imp_uid` 전달 시 포트원 API로 결제 검증 후에만 크레딧 적립
- `BLUEDOT_TEST_MODE=1` 환경변수 시에만 plan/amount/credits_added로 테스트 결제 허용
- 프로덕션 배포 시 `BLUEDOT_TEST_MODE` 미설정 또는 0으로 두고, `PORTONE_API_KEY`, `PORTONE_API_SECRET` 설정 필수

### CB-2. 분석 API에 인증/크레딧 검증 없음 (무료 우회) — ✅ 수정 완료
**발생 조건:** 누구나 `/api/hospitals`, `/api/ai-search`를 직접 호출  
**문제:** (동일)

**적용 수정:**
- `_require_auth_and_use_credit` 의존성 추가 → 로그인 필수, 요청 시 크레딧 1회 차감
- 비로그인 시 401, 크레딧 부족 시 402 반환
- 프론트엔드: 분석 버튼 클릭 시 로그인 필수, `authHeaders()`로 Bearer 전송

### CB-3. 프론트엔드 fetch 무한 대기 (Timeout 없음) — ✅ 수정 완료
**발생 조건:** 백엔드 응답 지연 또는 네트워크 끊김  
**문제:** (동일)

**적용 수정:**
- `fetchWithTimeout()` (60초, AbortController) 도입
- `parseJsonSafe()`로 비정상 JSON 파싱 시 명확한 에러 메시지
- `startAnalysis`, `submitAISearch`에 적용

### CB-4. rec.score가 undefined일 때 TypeError — ✅ 수정 완료
**발생 조건:** `openReportModal`에서 `rec.score`가 없는 저장된 리포트 데이터  
**위치:** `app.js`  
**적용 수정:** `rec.score_val ?? (rec.score ? String(rec.score).split('/')[0] : null) ?? '0'`

### CB-5. 젊은층_비중 > 1일 때 고령층 음수 → 레이더/BEP 왜곡 — ✅ 수정 완료
**발생 조건:** CSV에 젊은층_비중이 1 초과인 비정상 데이터  
**적용 수정:** `젊은층_비중.clip(0, 1)`, `고령층_비중 = (1 - 젊은층_비중).clip(0, 1)`

---

## 2. ⚠️ Warnings (신뢰성 저하 잠재 요인)

### W-1. HIRA API 응답이 JSON이 아닐 때 (XML 에러 등)
**위치:** `main.py` 511행 `data = response.json()`  
**문제:** API가 에러 시 XML을 반환하면 `JSONDecodeError` 발생. 현재 `except Exception`으로 잡히지만 사용자에게 "허위 마커 미표시"만 하고, 프론트에는 빈 결과만 전달되어 원인 파악이 어려움.

**권장:** `response.headers.get('content-type')`으로 JSON 여부 확인 후, 비정상 시 로깅 강화 및 클라이언트 에러 메시지 구체화

### W-2. 포트원 verify_payment에 Timeout 없음 — ✅ 수정 완료
**적용:** `verify_payment` 및 `_verify_portone_and_get_credits` 내 requests 호출에 `timeout=10` 추가

### W-3. Chart 인스턴스 미 destroy 시 메모리 누수 가능성
**위치:** `renderRadarAndBep`, `renderTimeMatrix`, `renderCharts`  
**현황:** `radarChart`, `timeMatrixChart` 등은 `destroy()` 후 새로 생성. 다만 `closeReportModal` 시 명시적으로 destroy하지는 않음. 모달만 닫고 다음 리포트 열 때 기존 차트 destroy 후 새로 만들므로 실사용 시 큰 누수 가능성은 낮음.

**권장:** `closeReportModal`에서 `if(radarChart){ radarChart.destroy(); radarChart=null; }` 등 정리 로직 추가

### W-4. setInterval 미 해제 가능성
**위치:** `app.js` 339행 `setInterval(checkExist, 100)`  
**문제:** `customOverlay.a`가 일정 시간 내에 생성되지 않으면 interval이 영구 동작할 수 있음. 카카오맵 DOM 생성이 보통 빠르므로 발생 확률은 낮음.

**권장:** `setTimeout`으로 5초 후 `clearInterval` 강제 실행

---

## 3. 💡 Test Scenarios (직접 테스트 권장)

### 시나리오 1: 결제 우회 시도
1. 로그인 (테스트 계정)
2. `BLUEDOT_TEST_MODE`를 **설정하지 않은 상태**에서 서버 재시작
3. Console: `fetch('.../api/credits/add', { method:'POST', headers:{'Content-Type':'application/json','Authorization':'Bearer '+localStorage.getItem('bluedot_auth_token')}, body: JSON.stringify({plan:'5',amount:30000,credits_added:5}) }).then(r=>r.json()).then(console.log)`  
4. **기대:** 400 "결제 검증이 필요합니다" 또는 imp_uid 필수 메시지로 거부

### 시나리오 2: 분석 API 직접 호출 (비로그인)
1. 로그아웃 상태
2. `fetch('http://127.0.0.1:8000/api/ai-search?lat=35.17&lng=129.07&prompt=김해 한의원&radius=3').then(r=>r.json()).then(console.log)`  
3. **기대:** 401 "로그인이 필요합니다"

### 시나리오 3: 악랄한 지도 클릭 + 빠른 연타
1. 지도에서 **바다 한가운데** (예: 동해 근해) 클릭
2. 분석 반경 1km로 "거시 상권 정밀 분석" 실행
3. **기대:** "선택하신 위치 반경 내에 행정동 데이터가 없습니다" 메시지로 우아하게 처리  
4. 동시에 **리포트 카드 연속 빠르게 클릭** (Rank 1, 2, 3...)  
5. **기대:** 모달이 중복으로 뜨지 않고, 차트가 겹치거나 크래시하지 않음
