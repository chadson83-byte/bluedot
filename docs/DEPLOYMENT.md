# 배포 가이드 (추천: Vercel + Fly.io)

## 구조

- **프론트(정적)**: Vercel
- **백엔드(FastAPI)**: Fly.io (Docker)
- **DB(PostGIS/pgRouting + 캐시)**: Fly Postgres 또는 외부 Postgres(PostGIS 지원)

---

## 1) 백엔드(Fly.io) 배포

### A. Fly CLI 설치/로그인

- Fly CLI 설치 후 `fly auth login`

### B. 앱 생성

프로젝트 루트에서:

```bash
fly launch
```

- `fly.toml`의 `app = "bluedot-backend"` 는 **앱 이름 중복 시 변경**하세요.

### C. 환경변수 세팅

필수/권장:

- `HIRA_API_KEY`
- `KAKAO_REST_KEY` (주소 변환 1순위, 없으면 JUSO로만 동작)
- `POSTGIS_HOST / POSTGIS_PORT / POSTGIS_DB / POSTGIS_USER / POSTGIS_PASSWORD` (Phase2 + 캐시)

예:

```bash
fly secrets set HIRA_API_KEY="..." KAKAO_REST_KEY="..."
fly secrets set POSTGIS_HOST="..." POSTGIS_PORT="5432" POSTGIS_DB="gis_db" POSTGIS_USER="..." POSTGIS_PASSWORD="..."
```

### D. 배포

```bash
fly deploy
```

정상 확인:
- `https://<YOUR_FLY_APP>.fly.dev/api/health`

---

## 2) 프론트(Vercel) 배포

### A. Vercel에 프로젝트 Import

- 프레임워크: **Other**
- Build Command: 없음
- Output: 루트 정적 파일(`index.html`) 그대로

### B. `vercel.json` rewrite 수정

`vercel.json`의 아래 값을 **실제 Fly 백엔드 도메인**으로 수정:

```json
{
  "source": "/api/(.*)",
  "destination": "https://YOUR_FLY_APP.fly.dev/api/$1"
}
```

### C. 배포

배포 후 프론트에서 `/api/*`는 Vercel이 Fly로 프록시합니다.

---

## 3) 주의사항

- 서버리스(Vercel)로 FastAPI를 올리면 PostGIS/pgRouting 및 장시간 연산에서 제약이 커서 권장하지 않습니다.
- `data/` 폴더에 큰 파일이 많으면 컨테이너 이미지가 커집니다. 운영에서는 데이터는 S3/스토리지로 분리 권장.

