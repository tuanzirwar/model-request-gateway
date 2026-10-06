$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskRecord = Join-Path $taskRoot '.local/demo-process.json'
if (Test-Path -LiteralPath $taskRecord) {
    $taskData = Get-Content -LiteralPath $taskRecord | ConvertFrom-Json
    $taskProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$($taskData.pid)"
    $taskExpected = Join-Path $taskRoot '.venv\Scripts\python.exe'
    if ($taskProcess -and $taskProcess.ExecutablePath -eq $taskExpected -and $taskProcess.CommandLine -like '*model_gateway.app:create_app*') {
        taskkill /PID $taskData.pid /T /F | Out-Null
        Write-Output '已停止本项目演示进程树'
    }
}
