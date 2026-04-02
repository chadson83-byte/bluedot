# Fly.io: SQLite 영구 저장용 Volume 생성 + Machine 1대 + 배포
# flyctl 설치·로그인 후 프로젝트 루트에서 실행:
#   powershell -ExecutionPolicy Bypass -File scripts\fly_volume_bootstrap.ps1

$ErrorActionPreference = "Stop"
$app = "bluedot-backend-autumn-grass-4638"
$region = "nrt"
$vol = "bluedot_data"

$flyExe = "fly"
if (Get-Command flyctl -ErrorAction SilentlyContinue) { $flyExe = "flyctl" }

Write-Host "==> Volumes (app=$app)"
& $flyExe volumes list -a $app 2>$null
Write-Host "`n==> Creating volume $vol in $region (3GB). 이미 있으면 에러 → 무시 후 deploy."
& $flyExe volumes create $vol --region $region --size 3 -a $app --yes 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "(create skipped or already exists)"
}

Write-Host "`n==> SQLite + 단일 Volume = Machine 1대"
& $flyExe scale count 1 -a $app --yes

Write-Host "`n==> Deploy"
& $flyExe deploy -a $app

Write-Host "`nDone. Health: GET https://${app}.fly.dev/api/health → sqlite.fly_volume_style_path 가 true 이면 /data 마운트 사용 중."
