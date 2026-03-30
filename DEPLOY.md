# BLUEDOT 배포 가이드 (Vercel + Fly.io)

프론트는 Vercel 정적 호스팅, API는 Fly.io 컨테이너로 동작하도록 구성되어 있습니다.

## 사전 준비

1. [Fly.io](https://fly.io) 계정, [flyctl](https://fly.io/docs/hands-on/install-flyctl/) 설치 및 `fly auth login`
2. [Vercel](https://vercel.com) 계정, GitHub에 이 저장소 연동(권장)

---

## 1) Fly.io — 백엔드(API)

프로젝트 루트(`main.py`, `Dockerfile`, `fly.toml` 있는 폴더)에서:

```bash
fly deploy
```

앱 이름을 `fly launch` 때 `bluedot-backend`가 아니게 정했다면, **`vercel.json`의 `destination` URL**을 `https://<실제앱이름>.fly.dev/api/$1`로 바꾸세요.

### Secrets (민감 정보는 반드시 secrets)

```bash
fly secrets set BLUEDOT_JWT_SECRET="긴_랜덤_문자열"
fly secrets set HIRA_API_KEY="심평원_API_키"
fly secrets set KAKAO_REST_KEY="카카오_REST_API_키"
fly secrets set GOOGLE_CLIENT_ID="구글_클라이언트_ID"
fly secrets set PORTONE_API_KEY="포트원_REST키"
fly secrets set PORTONE_API_SECRET="포트원_시크릿"
fly secrets set DATA_GO_KR_SERVICE_KEY="공공데이터포털_건축HUB_인증키"
```

- 로그인/주소 API를 안 쓰면 일부는 생략 가능하지만, **`BLUEDOT_JWT_SECRET`**은 프로덕션에서 꼭 고유 값으로 설정하세요.
- **`DATA_GO_KR_SERVICE_KEY`**: 경쟁 병원 건물 노후화(건축물대장)용. 미설정 시 코드에 포함된 레거시 키로 동작할 수 있으나, 운영에서는 본인 키로 교체하는 것을 권장합니다.
- **도보 폴리곤(PostGIS)**: Fly는 기본적으로 `FLY_APP_NAME`이 있으면 PostGIS 연결을 시도하지 않고 반경 근사만 씁니다(느린 DB 타임아웃 방지). OSM+pgRouting DB를 띄운 뒤 `POSTGIS_HOST` 등을 secrets로 넣으면 실도보 폴리곤이 켜집니다.

`/api/health` 응답의 `features.postgis_walking_polygon` 으로 현재 모드를 확인할 수 있습니다.
- `BLUEDOT_TEST_MODE`는 `fly.toml`의 `[env]`에서 이미 `0`입니다. 결제 테스트 모드는 켜지 마세요.

### 동작 확인

```text
https://<앱이름>.fly.dev/api/health
```

`{"ok":true,...}` 가 나오면 API는 정상입니다.

### SQLite 참고

지금은 `bluedot.db`(SQLite)를 컨테이너 안에 둡니다. **머신이 재시작되면 DB가 초기화될 수 있습니다.** 영구 저장이 필요하면 Fly Volume 또는 Postgres로 옮기세요.

---

## 2) Vercel — 프론트(정적)

1. Vercel에서 **New Project** → GitHub 저장소 선택
2. Framework Preset: **Other** (또는 정적)
3. Root Directory: 저장소 루트
4. Build Command: 비움, Output Directory: 비움 (또는 기본)

`vercel.json`의 `/api/*` 리라이트는 **짧은 요청·폴백용**입니다. 메인 분석은 브라우저가 **Fly URL로 직접** 호출합니다(`index.html`·`app.js`의 Fly 오리진).  
Fly 앱 URL을 바꿀 때는 **`vercel.json`·`index.html`(인라인)·`app.js`(`BLUEDOT_VERCEL_FLY_ORIGIN`) 세 곳을 같은 주소로 맞추세요.**

### 카카오 지도

[Kakao Developers](https://developers.kakao.com/) → 해당 앱 → **JavaScript 키** → **JavaScript SDK 도메인**에 배포 URL을 추가합니다.

예: `https://your-project.vercel.app`  
(커스텀 도메인을 쓰면 그 도메인도 추가)

---

## 3) 배포 순서 요약

1. `fly deploy` 로 API 올리기 → `/api/health` 확인  
2. `vercel.json`의 Fly URL이 실제 앱과 일치하는지 확인  
3. Vercel에 푸시/재배포 → 프로덕션 URL에서 화면·분석 동작 확인  
4. 카카오 콘솔에 Vercel 도메인 등록  

문제가 있으면 브라우저 **Network** 탭에서 `/api/...` 요청이 502/404인지, `sdk.js`가 막히는지 확인하세요.
