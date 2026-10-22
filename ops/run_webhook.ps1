# Auto-Trader webhook launcher: starts the Cloudflare tunnel, then the webhook
# server in the foreground with its output appended to a dated log file.
#
# This is the pre-service launcher (a scheduled task or a shell window pointed
# at it). The NSSM services in install_services.ps1 superseded it for
# unattended operation; it is kept as the simplest way to run the server
# attached, and as a fallback.
#
# Usage: powershell -ExecutionPolicy Bypass -File ops\run_webhook.ps1

$ErrorActionPreference = 'Stop'

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe   = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$Cloudflared = "C:\Program Files (x86)\cloudflared\cloudflared.exe"
$Tunnel      = "my-tunnel-name"          # cloudflared tunnel name
$PublicUrl   = "https://<your-hostname>/webhook"   # for the log header only

# Load env vars from secrets/.env
$envFile = Join-Path $ProjectRoot 'secrets\.env'
if (Test-Path -LiteralPath $envFile) {
    Get-Content -LiteralPath $envFile -ErrorAction Stop | ForEach-Object {
        $line = $_.Trim()
        if ($line.Length -eq 0 -or $line.StartsWith('#')) { return }
        $idx = $line.IndexOf('=')
        if ($idx -lt 1) { return }
        $name  = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
        if ($name.Length -gt 0) { Set-Item -Path ("Env:" + $name) -Value $value }
    }
}

# Force UTF-8: the Pine passphrase in webhook JSON can include non-ASCII
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

Set-Location -LiteralPath $ProjectRoot

# Dated log file, one per day, append mode so restarts preserve history.
$LogDir  = Join-Path $ProjectRoot 'outputs\logs'
if (-not (Test-Path -LiteralPath $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}
$LogFile = Join-Path $LogDir ('webhook_stdout_' + (Get-Date -Format 'yyyy-MM-dd') + '.log')

$header = @"

========================================================================
[$(Get-Date -Format s)] Auto-Trader Webhook starting (host-pid $PID)
  Project root: $ProjectRoot
  Python:       $PythonExe
  Cloudflared:  $Cloudflared
  Log file:     $LogFile
========================================================================
"@
Add-Content -LiteralPath $LogFile -Value $header -Encoding utf8

# --- Start Cloudflare Tunnel (background) ---
Add-Content -LiteralPath $LogFile -Value ("[{0}] Starting Cloudflare Tunnel..." -f (Get-Date -Format s)) -Encoding utf8
$tunnelProc = Start-Process `
    -FilePath $Cloudflared `
    -ArgumentList "tunnel run $Tunnel" `
    -WindowStyle Hidden `
    -PassThru

Add-Content -LiteralPath $LogFile -Value ("  Tunnel PID: {0}" -f $tunnelProc.Id) -Encoding utf8
Add-Content -LiteralPath $LogFile -Value "  URL: $PublicUrl" -Encoding utf8
Start-Sleep -Seconds 3

# --- Start Webhook Server (foreground) ---
Add-Content -LiteralPath $LogFile -Value ("[{0}] Starting Webhook Server on port 8000..." -f (Get-Date -Format s)) -Encoding utf8

try {
    # Route through cmd /c so the native exe's stderr is redirected cleanly
    # with '2>&1'. PowerShell 5.1's native redirect wraps each stderr line in
    # a NativeCommandError record (five-plus lines of noise per log line), and
    # uvicorn sends all INFO/ERROR to stderr via Python logging. cmd.exe's
    # >> with 2>&1 does a raw byte-level append and produces clean output.
    & cmd /c "`"$PythonExe`" -u main.py webhook --port 8000 >> `"$LogFile`" 2>&1"
} finally {
    Add-Content -LiteralPath $LogFile -Value ("[{0}] Stopping Cloudflare Tunnel..." -f (Get-Date -Format s)) -Encoding utf8
    if (-not $tunnelProc.HasExited) {
        Stop-Process -Id $tunnelProc.Id -Force -ErrorAction SilentlyContinue
    }
    Add-Content -LiteralPath $LogFile -Value ("[{0}] Done." -f (Get-Date -Format s)) -Encoding utf8
}
