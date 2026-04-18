param(
    [string]$RedisAddr = "localhost:6379",
    [string]$Stream = "jobs_demo",
    [string]$DLQStream = "jobs_demo.dlq",
    [string]$Group = "workers_demo",
    [string]$Consumer = "demo-consumer-1",
    [string]$MetricsAddr = ":2112",
    [int]$DurationSeconds = 12
)

$ErrorActionPreference = 'Stop'

function Get-MetricsSnapshot {
    param([string]$Url)

    $snapshot = @{
        jobs_pending = 0
        jobs_processing = 0
        jobs_completed_total = 0
        jobs_failed_total = 0
    }

    try {
        $raw = (Invoke-WebRequest -UseBasicParsing $Url -TimeoutSec 2).Content
        foreach ($line in ($raw -split "`n")) {
            $parts = $line.Trim() -split '\s+'
            if ($parts.Length -eq 2 -and $snapshot.ContainsKey($parts[0])) {
                $value = 0
                if ([int]::TryParse($parts[1], [ref]$value)) {
                    $snapshot[$parts[0]] = $value
                }
            }
        }
    } catch {
        # Keep defaults when metrics endpoint is not ready yet.
    }

    return $snapshot
}

function Render-Dashboard {
    param(
        [hashtable]$Metrics,
        [string[]]$Events,
        [string[]]$LogTail,
        [int]$Elapsed,
        [int]$Total
    )

    $barWidth = 36
    $filled = [Math]::Min($barWidth, [Math]::Floor(($Elapsed / [Math]::Max($Total, 1)) * $barWidth))
    $progressBar = ('#' * $filled).PadRight($barWidth, '-')

    Clear-Host
    Write-Host "+---------------------------------------------------------------+" -ForegroundColor DarkCyan
    Write-Host "| GO JOB QUEUE DEMO (LIVE)                                     |" -ForegroundColor DarkCyan
    Write-Host "+---------------------------------------------------------------+" -ForegroundColor DarkCyan
    Write-Host ("| Time: {0,2}s / {1,2}s   Progress: [{2}] |" -f $Elapsed, $Total, $progressBar)
    Write-Host "+----------------------+----------------------+------------------+"
    Write-Host ("| pending:    {0,-10} | processing: {1,-10} | completed: {2,-8} |" -f $Metrics.jobs_pending, $Metrics.jobs_processing, $Metrics.jobs_completed_total)
    Write-Host ("| failed:     {0,-10} | stream: {1,-10} | dlq: {2,-8} |" -f $Metrics.jobs_failed_total, 'jobs_demo', 'jobs_demo.dlq')
    Write-Host "+----------------------+----------------------+------------------+"

    Write-Host ""
    Write-Host "Timeline:" -ForegroundColor Yellow
    if ($Events.Count -eq 0) {
        Write-Host "  (aguardando eventos...)"
    } else {
        foreach ($ev in $Events | Select-Object -Last 8) {
            Write-Host "  - $ev"
        }
    }

    Write-Host ""
    Write-Host "Worker log (tail):" -ForegroundColor Yellow
    if ($LogTail.Count -eq 0) {
        Write-Host "  (sem logs ainda)"
    } else {
        foreach ($line in $LogTail) {
            Write-Host "  $line"
        }
    }
}

$metricsUrl = "http://localhost" + $MetricsAddr + "/metrics"
$workerOut = "deployments/demo/worker_demo.out.log"
$workerErr = "deployments/demo/worker_demo.err.log"

foreach ($p in @($workerOut, $workerErr)) {
    if (Test-Path -LiteralPath $p) {
        try { Remove-Item -LiteralPath $p -Force } catch {}
    }
}

$events = New-Object System.Collections.Generic.List[string]
$start = Get-Date
$publishedSuccess = $false
$publishedFailure = $false

$events.Add("iniciando worker")
$worker = Start-Process -FilePath go -ArgumentList @(
    'run', './cmd/worker',
    '-redis-addr', $RedisAddr,
    '-stream', $Stream,
    '-dlq-stream', $DLQStream,
    '-group', $Group,
    '-consumer', $Consumer,
    '-concurrency', '2',
    '-retry-base', '1s',
    '-retry-max', '4s',
    '-shutdown-timeout', '8s',
    '-metrics-addr', $MetricsAddr
) -WorkingDirectory (Get-Location).Path -RedirectStandardOutput $workerOut -RedirectStandardError $workerErr -PassThru

try {
    while ($true) {
        $elapsed = [int]((Get-Date) - $start).TotalSeconds

        if (-not $publishedSuccess -and $elapsed -ge 2) {
            $events.Add("publicando job de sucesso (image.resize)")
            go run ./cmd/producer -redis-addr $RedisAddr -type image.resize -payload "{}" -stream $Stream | Out-Null
            $events.Add("job image.resize enfileirado")
            $publishedSuccess = $true
        }

        if (-not $publishedFailure -and $elapsed -ge 4) {
            $events.Add("publicando job sem handler (unknown.task)")
            go run ./cmd/producer -redis-addr $RedisAddr -type unknown.task -payload "{}" -max-attempts 3 -stream $Stream | Out-Null
            $events.Add("job unknown.task enfileirado (vai para retry + dlq)")
            $publishedFailure = $true
        }

        $metrics = Get-MetricsSnapshot -Url $metricsUrl
        $logTail = @()
        if (Test-Path -LiteralPath $workerErr) {
            $logTail = Get-Content -LiteralPath $workerErr -Tail 6

            if ($logTail -match 'job retry scheduled') {
                if (-not ($events -contains 'retry detectado no worker')) {
                    $events.Add('retry detectado no worker')
                }
            }
            if ($logTail -match 'job moved to dlq') {
                if (-not ($events -contains 'job enviado para DLQ')) {
                    $events.Add('job enviado para DLQ')
                }
            }
        }

        Render-Dashboard -Metrics $metrics -Events $events -LogTail $logTail -Elapsed $elapsed -Total $DurationSeconds

        if ($elapsed -ge $DurationSeconds) {
            break
        }

        Start-Sleep -Milliseconds 500
    }
}
finally {
    if ($null -ne $worker -and -not $worker.HasExited) {
        Stop-Process -Id $worker.Id -Force
    }
}

$finalMetrics = Get-MetricsSnapshot -Url $metricsUrl

Write-Host ""
Write-Host "Resumo final:" -ForegroundColor Green
Write-Host ("  pending={0} processing={1} completed={2} failed={3}" -f $finalMetrics.jobs_pending, $finalMetrics.jobs_processing, $finalMetrics.jobs_completed_total, $finalMetrics.jobs_failed_total)
Write-Host ""
Write-Host "Logs completos em:" -ForegroundColor Green
Write-Host "  $workerErr"
Write-Host "  $workerOut"
