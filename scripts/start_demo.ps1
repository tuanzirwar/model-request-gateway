param([int]$Port = 0)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $taskRoot
New-Item -ItemType Directory -Force -Path '.local','reports' | Out-Null
$taskPython = Join-Path $taskRoot '.venv\Scripts\python.exe'
$env:PYTHONIOENCODING = 'utf-8'
if (Test-Path -LiteralPath '.local/demo-process.json') {
    $taskExisting = Get-Content -LiteralPath '.local/demo-process.json' | ConvertFrom-Json
    $taskExistingProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$($taskExisting.pid)"
    if ($taskExistingProcess -and $taskExistingProcess.ExecutablePath -eq $taskPython -and $taskExistingProcess.CommandLine -like '*model_gateway.app:create_app*') {
        Write-Output "本项目已有演示进程，请访问 http://127.0.0.1:$($taskExisting.port)/ 或先运行stop_demo.ps1"
        return
    }
}
if ($Port -eq 0) {
    $taskListener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $taskListener.Start()
    $Port = $taskListener.LocalEndpoint.Port
    $taskListener.Stop()
}
& $taskPython scripts/prepare_demo.py --port $Port
if ($LASTEXITCODE) { throw '演示授权初始化失败' }
$taskProcess = Start-Process -FilePath $taskPython -ArgumentList '-m','uvicorn','model_gateway.app:create_app','--factory','--host','127.0.0.1','--port',"$Port",'--log-level','warning' -WorkingDirectory $taskRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $taskRoot '.local/demo.stdout.txt') -RedirectStandardError (Join-Path $taskRoot '.local/demo.stderr.txt') -PassThru
@{pid=$taskProcess.Id;port=$Port;python=$taskPython} | ConvertTo-Json | Set-Content -LiteralPath '.local/demo-process.json'
$taskReady = $false
for ($taskAttempt = 0; $taskAttempt -lt 30; $taskAttempt++) {
    try {
        $taskHealth = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 1
        if ($taskHealth) { $taskReady = $true; break }
    } catch { Start-Sleep -Milliseconds 300 }
    if ($taskProcess.HasExited) { break }
}
if (-not $taskReady) {
    & (Join-Path $PSScriptRoot 'stop_demo.ps1')
    throw '演示服务未就绪，请查看 .local/demo.stderr.txt'
}
Write-Output "网关已启动，PID=$($taskProcess.Id)，地址 http://127.0.0.1:$Port/docs，TUI配置 .local/tui-demo/config.yaml"
