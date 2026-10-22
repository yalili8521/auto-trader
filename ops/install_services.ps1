# Install / reinstall the two NSSM services that keep the auto-trader running
# unattended: the webhook server and the Cloudflare tunnel in front of it.
#
# Run from an elevated PowerShell. Idempotent: removes any existing service of
# the same name first (including legacy names from an earlier deployment).
#
# Why services and not a scheduled task or a shell window: a process tied to an
# interactive logon dies on sleep, logout, or window close (this caused three
# outages in one week). NSSM-wrapped services run in Session 0, start at boot,
# and restart automatically on crash.
#
# Edit the five paths below for your machine. $Tunnel is the name of a
# Cloudflare tunnel you created with `cloudflared tunnel create <name>` and
# routed to localhost:8000 in its config.yml.

$ErrorActionPreference = 'Continue'
$nssm   = "C:\Path\To\nssm.exe"                       # NSSM 2.24+ (winget install NSSM.NSSM)
$cf     = "C:\Program Files (x86)\cloudflared\cloudflared.exe"
$proj   = Split-Path -Parent $PSScriptRoot             # repo root (this file lives in ops\)
$py     = "$proj\.venv\Scripts\python.exe"
$cfcfg  = "$env:USERPROFILE\.cloudflared\config.yml"
$Tunnel = "my-tunnel-name"
$logs   = "$proj\outputs\logs"
$result = "$logs\install_services_result.txt"

$WebhookSvc = 'AutoTrader-Webhook'
$TunnelSvc  = 'AutoTrader-Tunnel'

foreach ($p in @($nssm, $cf, $py, $cfcfg)) {
    if (-not (Test-Path $p)) { "MISSING: $p" | Set-Content $result; exit 1 }
}
New-Item -ItemType Directory -Force $logs | Out-Null

foreach ($svc in @($WebhookSvc, $TunnelSvc)) {
    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
        & $nssm stop $svc | Out-Null
        & $nssm remove $svc confirm | Out-Null
    }
}

# --- webhook server -------------------------------------------------------
& $nssm install $WebhookSvc $py "-u main.py webhook --port 8000"
& $nssm set $WebhookSvc AppDirectory $proj
# LocalSystem's default code page is not UTF-8; without this the .env loader
# crashes on non-ASCII characters (for example a passphrase with CJK text).
& $nssm set $WebhookSvc AppEnvironmentExtra PYTHONUTF8=1 PYTHONIOENCODING=utf-8
& $nssm set $WebhookSvc AppStdout "$logs\service_webhook.log"
& $nssm set $WebhookSvc AppStderr "$logs\service_webhook.log"
& $nssm set $WebhookSvc AppRotateFiles 1
& $nssm set $WebhookSvc AppRotateBytes 10485760
& $nssm set $WebhookSvc AppExit Default Restart
& $nssm set $WebhookSvc AppRestartDelay 5000
& $nssm set $WebhookSvc Start SERVICE_AUTO_START
& $nssm set $WebhookSvc Description "Auto-trader TradingView webhook server"

# --- cloudflare tunnel ----------------------------------------------------
# --config is passed explicitly because LocalSystem resolves ~ to the SYSTEM
# profile, not to the user who created the tunnel.
& $nssm install $TunnelSvc $cf "tunnel --config `"$cfcfg`" run $Tunnel"
& $nssm set $TunnelSvc AppStdout "$logs\service_tunnel.log"
& $nssm set $TunnelSvc AppStderr "$logs\service_tunnel.log"
& $nssm set $TunnelSvc AppRotateFiles 1
& $nssm set $TunnelSvc AppRotateBytes 10485760
& $nssm set $TunnelSvc AppExit Default Restart
& $nssm set $TunnelSvc AppRestartDelay 5000
& $nssm set $TunnelSvc Start SERVICE_AUTO_START
& $nssm set $TunnelSvc Description "Cloudflare tunnel $Tunnel -> localhost:8000"

& $nssm start $WebhookSvc
& $nssm start $TunnelSvc
Start-Sleep -Seconds 5
Get-Service "$WebhookSvc", "$TunnelSvc" -ErrorAction SilentlyContinue |
    Format-Table Name, Status, StartType -AutoSize | Out-String | Set-Content $result
