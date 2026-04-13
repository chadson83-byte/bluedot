# Fly.io + SQLite + 오아시스 데이터 — 역할 분담

Supabase처럼 웹 **SQL Editor**는 Fly에 없습니다. DB는 볼륨上的 **`/data/bluedot.db`**(SQLite)이고, 조회·적재는 **SSH + `sqlite3` / `python scripts/...`** 로 합니다.

---

## 1. 레포에 이미 반영된 것 (에이전트/코드 쪽에서 끝난 부분)

다음은 **배포만 되면** 동작하도록 코드·설정에 들어가 있습니다.

| 항목 | 내용 |
|------|------|
| DB 경로 | `fly.toml` `[env]` → `BLUEDOT_DB_PATH=/data/bluedot.db` |
| 영구 디스크 | `[[mounts]]` → `bluedot_data` → 컨테이너 `/data` |
| 앱 작업 디렉터리 | `Dockerfile` → `WORKDIR /app` (코드·스크립트는 `/app` 기준) |
| 테이블 생성 | 서버 기동 시 `database.init_db()` 로 SQLite 스키마 생성 |
| 빈 DB 시 시드 | 기동 스레드에서 `scripts/seed_oasis_if_empty.py` 실행 → 지하철 CSV·(조건부) SNS·상권 CSV가 **이미지 안에 있으면** 자동 적재 시도 |
| 임포트 스크립트 | `/app/scripts/import_*_csv.py` (상대 경로는 항상 `/app`에서 실행한다고 가정) |
| 이미지에 포함되는 data 파일 | `.dockerignore`의 `!data/...` 예외만 빌드 컨텍스트에 포함 (예: 지하철·SNS·`ES1013.csv` 등) |
| 헬스 점검 | `GET /api/health` 의 `oasis_sqlite_rows` 로 SNS / 지하철 / 상권 테이블 **행 수** 확인 |

즉, **Fly 앱 생성·시크릿·배포·CSV가 이미지에 들어가게 git 반영**만 되면, 시드/스키마는 서버가 처리합니다.

---

## 2. 반드시 사용자(Fly 계정)가 할 일

### 2-1. 로컬 CLI

1. [Fly CLI](https://fly.io/docs/hands-on/install-flyctl/) 설치 (`flyctl` 또는 `fly` 명령).
2. `flyctl auth login` (또는 `fly auth login`).

### 2-2. 앱·볼륨 (최초 1회)

- 대시보드에 **Volume `bluedot_data`** 가 없으면(또는 새 리전에 띄울 때):

  ```bash
  flyctl volumes create bluedot_data --region nrt --size 3 -a bluedot-backend-autumn-grass-4638
  ```

  (앱 이름은 `fly.toml` 의 `app = '...'` 와 동일하게.)

- 머신이 2대 이상이면 SQLite 볼륨은 **한 대에만** 붙을 수 있으므로, 운영은 **1대** 권장(`fly.toml` 주석 참고).

### 2-3. 시크릿(환경 변수)

대시보드 **Secrets** 또는:

```bash
flyctl secrets set -a bluedot-backend-autumn-grass-4638 KAKAO_REST_KEY="..."
flyctl secrets set -a bluedot-backend-autumn-grass-4638 HIRA_API_KEY="..."
# 기타 프로젝트에 필요한 키 (DATA_GO_KR, 네이버 등)
```

- **SNS·법정동 매칭·상권 시군구 매칭**에 `KAKAO_REST_KEY` 가 필요합니다.

### 2-4. 배포 (이미지에 CSV 넣기)

- `.dockerignore` 가 허용한 파일만 **이미지에 포함**됩니다.  
  상권 단일 파일명은 예: `data/ES1013.csv` → git 커밋 후:

  ```bash
  flyctl deploy -a bluedot-backend-autumn-grass-4638
  ```

- **이미지에 넣지 않은 대용량 CSV**는 아래 2-6처럼 볼륨에 올린 뒤 SSH에서 임포트해야 합니다.

### 2-5. SNS 대용량 자동 적재(선택)

- `fly.toml` 의 `[env]` 에 다음을 넣고 재배포하면, **볼륨 DB가 비어 있을 때만** SNS CSV 적재를 시도합니다(콜드스타트 수 분 걸릴 수 있음).

  ```toml
  BLUEDOT_AUTOIMPORT_SNS = '1'
  ```

- 넣지 않으면 SNS는 **SSH에서 수동 임포트**(2-6)가 필요합니다.

### 2-6. SSH로 임포트 / SQL 조회 (Supabase 에디터 대체)

**인터랙티브 셸:**

```bash
flyctl ssh console -a bluedot-backend-autumn-grass-4638
```

셸 안에서 (경로는 이 레포 Dockerfile 기준):

```bash
cd /app
# DB는 환경변수와 동일하게 지정
export BLUEDOT_DB_PATH=/data/bluedot.db

# 상권 (data/ES1013.csv 가 이미지의 /app/data 아래에 있을 때)
python scripts/import_commercial_vitality_csv.py --db /data/bluedot.db

# 지하철
python scripts/import_subway_footfall_csv.py --db /data/bluedot.db --csv data/ES1007BD00101MM2504_csv.csv

# SNS (수백만 행·시간 다소 소요)
python scripts/import_sns_floating_csv.py --db /data/bluedot.db --csv data/ES1007AD00101MM2504_csv.csv
```

**한 줄로 실행(Windows PowerShell에서도 동일 인자):**

```bash
flyctl ssh console -a bluedot-backend-autumn-grass-4638 -C "cd /app && python scripts/import_commercial_vitality_csv.py --db /data/bluedot.db"
```

**SQLite CLI** (컨테이너에 `sqlite3` 가 없을 수 있음 → 없으면 `apt` 불가한 slim 이미지이므로 **로컬에서 DB 복사** 또는 **Python 한 줄**로 조회):

```bash
flyctl ssh console -a bluedot-backend-autumn-grass-4638 -C "cd /app && python -c \"import sqlite3; c=sqlite3.connect('/data/bluedot.db'); print(c.execute('select count(*) from commercial_vitality_road').fetchone())\""
```

### 2-7. 이미지에 없는 파일을 볼륨 `/data`에 넣는 방법

- **방법 A (권장)**: 파일을 `data/` 에 두고 `.dockerignore` 에 `!data/파일명` 추가 → git 커밋 → `flyctl deploy`.
- **방법 B**: [fly ssh sftp](https://fly.io/docs/flyctl/ssh-sftp/) 등으로 머신의 `/data` 에 업로드한 뒤, 임포트 시 `--csv /data/업로드한파일.csv` 로 실행.

### 2-8. 적재 후 캐시

- 엔진이 메모리 캐시를 쓰는 경우, 행 수가 헬스에 맞아도 API가 옛 값을 볼 수 있습니다. 이때 **머신 재시작**(대시보드 Machines → Restart 또는 `flyctl machines restart ...`)을 한 번 하세요.

### 2-9. 동작 확인

```bash
curl -s https://bluedot-backend-autumn-grass-4638.fly.dev/api/health
```

JSON 안의 `oasis_sqlite_rows` 에서 `sns_floating_population`, `subway_station_footfall`, `commercial_vitality_road` 의 숫자가 기대과 일치하는지 봅니다.

---

## 3. 요약 표

| 작업 | 누가 / 어디서 |
|------|----------------|
| 스키마·시드 스크립트·헬스 필드 | 레포 코드 (이미 반영) |
| 볼륨·시크릿·deploy | 사용자 + Fly |
| SQL GUI | 없음 → SSH + sqlite3 또는 Python |
| 대용량 CSV를 이미지에 안 넣을 때 | 사용자: SFTP 등으로 `/data` 또는 매 deploy에 예외 추가 |

이 문서는 `.dockerignore` 때문에 **Docker 이미지 안에는 포함되지 않을 수 있습니다**. 로컬·GitHub에서 읽으면 됩니다.
