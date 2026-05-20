param(
    [string]$Port = "5090",
    [string]$TaskUsers = "Michel"
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
    $env:MASTER_PORT = $Port
    $env:TASK_USERS = $TaskUsers
    & $pythonCommand "master.py"
} finally {
    Pop-Location
}