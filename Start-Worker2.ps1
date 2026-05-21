param(
    [string]$Host = "192.168.15.50",
    [string]$Port = "5090",
    [string]$ReconnectDelay = "10",
    [string]$ForceStatus = "OK"
)

$pythonCommand = if (Get-Command py -ErrorAction SilentlyContinue) {
    "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    "python"
} else {
    throw "Python nao encontrado no PATH. Instale Python 3 e marque a opcao para adicionar ao PATH."
}

Push-Location $PSScriptRoot
try {
    $env:MASTER_HOST = $Host
    $env:MASTER_PORT = $Port
    $env:WORKER_MODE = "TASKS"
    $env:RECONNECT_DELAY = $ReconnectDelay
    $env:FORCE_STATUS = $ForceStatus
    & $pythonCommand "worker2.py"
} finally {
    Pop-Location
}