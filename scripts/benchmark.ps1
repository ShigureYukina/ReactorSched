#Requires -Version 7.1
<#
.SYNOPSIS
    Reproducible load experiment for the performance report (PRD 12.2).

.DESCRIPTION
    Runs one scenario against a freshly started scheduling center with the development MySQL and
    writes a machine-readable result next to the report:

      - a fixed downstream latency for every task (fault-lab, no jitter);
      - a burst of submissions from several threads;
      - an overload phase (a share of downstream calls failing) started right after the burst,
        lifted after a fixed number of seconds;
      - recovery time = from lifting the overload until the last instance reaches a terminal state.

    Every reported number is measured, never asserted: throughput and backlog come from
    /api/v1/metrics/summary, queue and execution latencies are computed from the stored
    task/task_execution rows (the same rows the API aggregates), and the run's environment is
    recorded in the result so two runs are comparable.

    The two scenarios of docs/PERFORMANCE.md:
      baseline  - fixed concurrency (reactorsched.adaptive.enabled=false)
      aimd      - the S4 shared AIMD controller (reactorsched.adaptive.enabled=true)

.EXAMPLE
    pwsh -NoProfile -File scripts/benchmark.ps1 -Scenario baseline
    pwsh -NoProfile -File scripts/benchmark.ps1 -Scenario aimd
#>
[CmdletBinding()]
param(
    [ValidateSet("baseline", "aimd")]
    [string]$Scenario = "baseline",
    [int]$Tasks = 400,
    [int]$DownstreamLatencyMs = 120,
    [double]$OverloadErrorRate = 0.6,
    [int]$OverloadSeconds = 20,
    [int]$Concurrency = 2,
    [int]$Port = 18080,
    [string]$JavaHome = "$env:USERPROFILE\.jdks\jdk-21.0.12.1+1",
    [string]$Container = "reactorsched-mysql",
    [string]$Database = "reactorsched",
    [string]$DbUser = "reactorsched",
    [string]$DbPassword = "reactorsched123",
    [int]$SubmitThreads = 8
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$jar = Join-Path $repoRoot "reactorsched-app\target\reactorsched-app-1.0.0-SNAPSHOT.jar"
$javaPath = Join-Path $JavaHome "bin\java.exe"
$resultsDir = Join-Path $repoRoot "benchmark-results"
$instance = $null

if (-not (Test-Path $jar)) {
    throw "the app jar is missing - run .\mvnw.cmd -B -DskipTests -pl app -am package first"
}
if (-not (Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }

$runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$scope = "bench-$Scenario-$runStamp"

function Write-Step { param([string]$Message) Write-Host "[$(Get-Date -Format HH:mm:ss)] $Message" }

function Stop-BenchmarkInstance {
    if ($null -ne $instance -and -not $instance.HasExited) {
        Stop-Process -Id $instance.Id -Force
        $instance.WaitForExit(10000) | Out-Null
    }
    $script:instance = $null
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)) { return }
        Start-Sleep -Milliseconds 300
    }
}

function Start-BenchmarkInstance {
    $arguments = @(
        "-jar", $jar,
        "--server.port=$Port",
        "--reactorsched.worker.concurrency=$Concurrency",
        "--reactorsched.adaptive.enabled=$(if ($Scenario -eq 'aimd') { 'true' } else { 'false' })",
        "--reactorsched.adaptive.min-concurrency=1",
        "--reactorsched.adaptive.max-concurrency=$Concurrency",
        "--reactorsched.adaptive.initial-concurrency=$Concurrency",
        "--reactorsched.retry.max-dispatches-per-second=0"
    )
    $script:instance = Start-Process -FilePath $javaPath -ArgumentList $arguments `
        -PassThru -RedirectStandardOutput "$env:TEMP\reactor-bench-$Scenario.log" `
        -RedirectStandardError "$env:TEMP\reactor-bench-$Scenario.err.log" -WindowStyle Hidden
    if ($instance.HasExited) { throw "the engine exited during startup - check $env:TEMP\reactor-bench-$Scenario.log" }
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        if ($instance.HasExited) { throw "the engine exited during startup - check the instance log" }
        try {
            Invoke-RestMethod -Method Get -Uri "http://localhost:$Port/api/v1/tasks" -TimeoutSec 10 | Out-Null
            return
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    throw "the engine did not become ready within 60 seconds"
}

function Invoke-Api {
    param([string]$Method, [string]$Path, [object]$Body, [switch]$AllowRejection)
    $headers = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("admin:reactor-dev")) }
    $invocation = @{ Method = $Method; Uri = "http://localhost:$Port$Path"; Headers = $headers; TimeoutSec = 30 }
    if ($null -ne $Body) {
        $invocation["ContentType"] = "application/json"
        $invocation["Body"] = ($Body | ConvertTo-Json -Depth 8)
    }
    if ($AllowRejection) { return Invoke-WebRequest @invocation -SkipHttpErrorCheck }
    return Invoke-RestMethod @invocation
}

try {
    Write-Step "scenario=$Scenario tasks=$Tasks latency=${DownstreamLatencyMs}ms concurrency=$Concurrency adaptive=$($Scenario -eq 'aimd')"

    # A clean table keeps this run's numbers about this run only: leftover instances from an
    # interrupted earlier run would both inflate the backlog and trip the entry backpressure.
    # MYSQL_PWD instead of -p keeps the password out of the process list and the warning out of the
    # log; stderr is left alone because redirecting it swallows the stdout of a native command.
    $env:MYSQL_PWD = $DbPassword
    docker exec -e MYSQL_PWD=$DbPassword $Container mysql "-u$DbUser" $Database -e @"
DELETE FROM task_execution;
DELETE FROM task;
DELETE FROM scheduler_decision;
DELETE FROM group_rate_window;
"@ | Out-Null

    Start-BenchmarkInstance
    Write-Step "engine up on port $Port"

    # A deterministic downstream: fixed latency, no jitter, no errors until the overload phase.
    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "benchmark"; reason = "fixed latency for the run"
        latencyMs = $DownstreamLatencyMs; jitterMs = 0; errorRate = 0.0
    } | Out-Null

    # The container runs with TZ=UTC and the JDBC URL sets connectionTimeZone=UTC, so stored
    # timestamps are UTC even though the process clock is local: the window filter below uses UTC.
    $submittedAt = (Get-Date).ToUniversalTime()
    $submission = 1..$Tasks | ForEach-Object -Parallel {
        $body = @{
            taskType = "downstream"
            payload = @{ benchmark = $true }
            idempotencyKey = "$using:scope-$using:Scenario-$_"
            maxAttempts = 5
        } | ConvertTo-Json
        $headers = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("admin:reactor-dev")) }
        try {
            Invoke-RestMethod -Method Post -Uri "http://localhost:$using:Port/api/v1/tasks" `
                -Headers $headers -ContentType "application/json" -Body $body -TimeoutSec 30 | Out-Null
        } catch {
            Write-Warning "submission $_ failed: $_"
        }
    } -ThrottleLimit $SubmitThreads
    $submittedDoneAt = (Get-Date).ToUniversalTime()
    Write-Step "submitted $Tasks instances in $([int]($submittedDoneAt - $submittedAt).TotalSeconds)s"

    # Overload phase: a share of downstream calls fail, so retries pile up behind the same latency.
    $overloadAt = (Get-Date).ToUniversalTime()
    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "benchmark"; reason = "overload phase"
        latencyMs = $DownstreamLatencyMs; jitterMs = 0; errorRate = $OverloadErrorRate
    } | Out-Null
    Start-Sleep -Seconds $OverloadSeconds
    $recoveredAt = (Get-Date).ToUniversalTime()
    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "benchmark"; reason = "end overload phase"
        latencyMs = $DownstreamLatencyMs; jitterMs = 0; errorRate = 0.0
    } | Out-Null
    Write-Step "overload lifted at $($recoveredAt.ToString('HH:mm:ss'))"

    # Sample the summary until every instance is terminal; the backlog readings give the peak.
    $peakBacklog = 0L
    $lastSummary = $null
    $deadline = (Get-Date).AddMinutes(15)
    while ((Get-Date) -lt $deadline) {
        $summary = Invoke-Api -Method Get -Path "/api/v1/metrics/summary"
        $lastSummary = $summary
        if ($summary.backlogNow -gt $peakBacklog) { $peakBacklog = $summary.backlogNow }
        if ($summary.completedTasks -ge $Tasks -and $summary.runningNow -eq 0) { break }
        Start-Sleep -Milliseconds 500
    }
    $drainedAt = (Get-Date).ToUniversalTime()
    if ($null -eq $lastSummary -or $lastSummary.completedTasks -lt $Tasks) {
        throw "the run did not drain: completed=$($lastSummary.completedTasks) of $Tasks"
    }
    Write-Step "drained at $($drainedAt.ToString('HH:mm:ss')) (peak backlog $peakBacklog)"

    # Queue and execution latencies from the stored rows: the same rows the API aggregates, read
    # once after the run so nothing races a running attempt.
    $rows = docker exec -e MYSQL_PWD=$DbPassword $Container mysql "-u$DbUser" $Database -N -e @"
SELECT TIMESTAMPDIFF(MICROSECOND, t.created_at, e.started_at) / 1000.0,
       TIMESTAMPDIFF(MICROSECOND, e.started_at, e.finished_at) / 1000.0
  FROM task t JOIN task_execution e ON e.task_id = t.id
 WHERE t.task_type = 'downstream'
   AND t.created_at >= '$($submittedAt.ToString('yyyy-MM-dd HH:mm:ss'))'
   AND e.finished_at IS NOT NULL;
"@
    $queue = @(); $exec = @()
    foreach ($line in ($rows -split "`n")) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $parts = $line -split "`t"
        if ($parts.Count -ne 2) { continue }
        $queue += [double]$parts[0]
        $exec += [double]$parts[1]
    }
    if ($queue.Count -eq 0) { throw "no finished attempts were found for this run" }

    function Percentile {
        param([double[]]$Values, [double]$P)
        $sorted = $Values | Sort-Object
        if ($sorted.Count -eq 1) { return [math]::Round($sorted[0], 1) }
        $rank = $P / 100.0 * ($sorted.Count - 1)
        $lower = [math]::Floor($rank); $upper = [math]::Ceiling($rank)
        if ($lower -eq $upper) { return [math]::Round($sorted[$lower], 1) }
        return [math]::Round($sorted[$lower] + ($rank - $lower) * ($sorted[$upper] - $sorted[$lower]), 1)
    }

    $windowSeconds = [math]::Max(0.001, ($drainedAt - $submittedAt).TotalSeconds)
    $javaVersion = (& $javaPath -version 2>&1 | Select-Object -First 1) -join " "
    $result = [ordered]@{
        scenario = $Scenario
        runStamp = $runStamp
        environment = [ordered]@{
            machine = $env:COMPUTERNAME
            os = (Get-CimInstance Win32_OperatingSystem).Caption
            cpu = (Get-CimInstance Win32_Processor).Name
            cores = [Environment]::ProcessorCount
            java = $javaVersion
            database = "MySQL 8.4 (docker, $Container)"
            schedulerInstances = 1
            executorInstances = 1
            executorKind = "embedded LOCAL worker"
            concurrency = $Concurrency
            adaptiveEnabled = ($Scenario -eq "aimd")
        }
        load = [ordered]@{
            instances = $Tasks
            downstreamLatencyMs = $DownstreamLatencyMs
            overloadErrorRate = $OverloadErrorRate
            overloadSeconds = $OverloadSeconds
            submitThreads = $SubmitThreads
        }
        results = [ordered]@{
            throughputPerSecond = [math]::Round($Tasks / $windowSeconds, 2)
            submissionSeconds = [math]::Round(($submittedDoneAt - $submittedAt).TotalSeconds, 2)
            succeeded = $lastSummary.succeededTasks
            failed = $lastSummary.failedTasks
            failureRate = [math]::Round($lastSummary.failureRate, 4)
            peakBacklog = $peakBacklog
            recoverySeconds = [math]::Round(($drainedAt - $recoveredAt).TotalSeconds, 2)
            queueP50Ms = Percentile $queue 50
            queueP95Ms = Percentile $queue 95
            queueP99Ms = Percentile $queue 99
            executionP50Ms = Percentile $exec 50
            executionP95Ms = Percentile $exec 95
            executionP99Ms = Percentile $exec 99
            attempts = $queue.Count
        }
    }
    $outFile = Join-Path $resultsDir "$Scenario-$runStamp.json"
    ($result | ConvertTo-Json -Depth 6) | Set-Content -Path $outFile -Encoding utf8
    Write-Step "result written to $outFile"
    Write-Host ($result | ConvertTo-Json -Depth 6)
}
finally {
    Stop-BenchmarkInstance
    Write-Step "engine stopped"
}
