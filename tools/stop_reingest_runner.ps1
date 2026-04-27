$ErrorActionPreference = "Stop"

$runnerName = "n8ntopython-reingest-runner"
$existing = docker ps -a --filter "name=^/${runnerName}$" --format "{{.ID}}"
if ($existing) {
  docker rm -f $runnerName | Out-Null
  Write-Output "stopped $runnerName"
} else {
  Write-Output "$runnerName not found"
}
