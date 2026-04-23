$ErrorActionPreference = "Stop"

$root = "E:\n8n to python"
$runnerName = "n8ntopython-reingest-runner"
$launchLog = Join-Path $root "data\logs\reingest-launch.log"
$progressFile = Join-Path $root "data\logs\reingest-progress.json"

New-Item -ItemType Directory -Force -Path (Join-Path $root "data\logs") | Out-Null

$existing = docker ps -a --filter "name=^/${runnerName}$" --format "{{.ID}}"
if ($existing) {
  docker rm -f $runnerName | Out-Null
}

if (Test-Path $launchLog) {
  Remove-Item -Force $launchLog
}

if (Test-Path $progressFile) {
  Remove-Item -Force $progressFile
}

docker compose -f (Join-Path $root "compose.yaml") run -d --name $runnerName -e PYTHONPATH=/app --workdir /app worker sh -lc "python tools/reingest_all_pdfs.py >> data/logs/reingest-launch.log 2>&1"

Write-Output "started $runnerName"
