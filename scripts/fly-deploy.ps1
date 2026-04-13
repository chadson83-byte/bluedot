# Fly.io 원격 빌드·배포 (Dockerfile + fly.toml)
# 사전: https://fly.io/docs/hands-on/install-flyctl/ 에서 flyctl 설치 후 `fly auth login`
# 볼륨·시크릿: fly.toml 주석, https://fly.io/docs/reference/secrets/
$ErrorActionPreference = "Stop"
if (-not (Get-Command fly -ErrorAction SilentlyContinue) -and -not (Get-Command flyctl -ErrorAction SilentlyContinue)) {
    Write-Error "flyctl 이 없습니다. PowerShell에서: iwr https://fly.io/install.ps1 -useb | iex"
}
$fly = if (Get-Command fly -ErrorAction SilentlyContinue) { "fly" } else { "flyctl" }
Set-Location (Split-Path -Parent $PSScriptRoot)
& $fly deploy --remote-only
