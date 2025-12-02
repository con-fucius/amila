param(
    [switch]$IncludeStack
)

$repoRoot = $PSScriptRoot
$backendPath = Join-Path $repoRoot "backend"

Set-Location $backendPath

Write-Host "Running fast test suite (excluding full_stack and degraded_stack)" -ForegroundColor Cyan
uv run pytest -m "not full_stack and not degraded_stack" tests

if ($LASTEXITCODE -ne 0) {
    Write-Error "Fast test suite failed."
    exit $LASTEXITCODE
}

if ($IncludeStack) {
    Write-Host "Running full_stack and degraded_stack tests" -ForegroundColor Yellow
    uv run pytest -m "full_stack or degraded_stack" tests
    exit $LASTEXITCODE
}
