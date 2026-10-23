#Requires -Version 7.1
<#
.SYNOPSIS
    Runnable walkthrough of the engine: submit and execute, cancel with the management credentials,
    inject a downstream fault and watch the retry, then read the timeline and the recorded
    decisions that explain what happened.

.DESCRIPTION
    The demo starts its own engine instance against the development MySQL and stops it again at
    the end. It walks through the three claims of PRD 13:

      1. correctness is verifiable      - tasks run to SUCCEEDED, a cancel is refused without
                                          credentials and audited with them;
      2. scheduling has evidence        - a real fault injection produces real retries and real
                                          decision rows (nothing is simulated in the output);
      3. failures are explainable       - one task's timeline and decisions are printed as the
                                          engine recorded them.

    It then walks the PRD 16 chain end to end: a sharded definition is expanded by the scheduler
    scan, a worker is crashed under an in-flight attempt, and the lease recovery path reruns the
    orphaned instance on the replacement executor.

    Every number shown comes from the REST API of the running application or from the database -
    the script never prints an expected value as if it were a measured one.

.EXAMPLE
    docker compose up -d mysql
    pwsh -NoProfile -File scripts/demo.ps1
#>
[CmdletBinding()]
param(
    [string]$JavaHome = "$env:USERPROFILE\.jdks\jdk-21.0.12.1+1",
    [string]$Container = "reactorsched-mysql",
    [int]$Port = 8080,
    [string]$AdminUser = "admin",
    [string]$AdminPassword = "",
    [int]$HealthyLatencyMs = 200,
    [int]$FaultLatencyMs = 1500,
    [int]$DemoTasks = 3,
    # The crash section needs an attempt that is still in flight when the fault lands: the demo
    # holds one open with a slow-but-healthy downstream call, then crashes the worker under it.
    # The lease TTL is shortened so the recovery cycle (lease expiry -> reclaim -> rerun) is
    # observable in seconds instead of the production default of a minute. application.yml pins
    # the renew interval explicitly, so both properties must be overridden together (renew < ttl).
    [int]$CrashLatencyMs = 5000,
    [string]$LeaseTtl = "PT15S",
    [string]$LeaseRenewInterval = "PT5S"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$jar = Join-Path $repoRoot "reactorsched-app\target\reactorsched-app-1.0.0-SNAPSHOT.jar"
$javaPath = Join-Path $JavaHome "bin\java.exe"
$instance = $null

# A fresh idempotency scope per run keeps the demo re-runnable: leftover rows from an earlier run
# (for example one that was interrupted) must never be mistaken for this run's tasks.
$runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$demoScope = "demo-$runStamp"
$demoStartedAt = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
$demoSucceeded = 0
$demoFailed = 0
$demoCancelled = 0

# Management endpoints require the ADMIN role since phase 5 (PRD 8.4). The default matches the
# development default in application.yml; override it with -AdminPassword or REACTORSCHED_ADMIN_PASSWORD.
if ([string]::IsNullOrEmpty($AdminPassword)) {
    $AdminPassword = if ($env:REACTORSCHED_ADMIN_PASSWORD) { $env:REACTORSCHED_ADMIN_PASSWORD } else { "reactor-dev" }
}

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "== $Message" -ForegroundColor Cyan
}

function Write-Line {
    param([string]$Message)
    Write-Host "  $Message"
}

function Stop-DemoInstance {
    if ($null -ne $instance -and -not $instance.HasExited) {
        Stop-Process -Id $instance.Id -Force
        $instance.WaitForExit(10000) | Out-Null
    }
    $script:instance = $null
    # The port needs a moment to be released before the next instance binds it.
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)) { return }
        Start-Sleep -Milliseconds 300
    }
    throw "port $Port was not released after stopping the instance"
}

function Start-DemoInstance {
    param([switch]$ApiOnly)
    $arguments = @("-jar", $jar, "--server.port=$Port",
        "--reactorsched.worker.lease-ttl=$LeaseTtl",
        "--reactorsched.worker.lease-renew-interval=$LeaseRenewInterval")
    if ($ApiOnly) { $arguments += "--reactorsched.worker.enabled=false" }
    $script:instance = Start-Process -FilePath $javaPath -ArgumentList $arguments `
        -PassThru -RedirectStandardOutput "$env:TEMP\reactor-demo-instance.log" `
        -RedirectStandardError "$env:TEMP\reactor-demo-instance.err.log" -WindowStyle Hidden
    if ($instance.HasExited) {
        throw "the engine exited during startup - check $env:TEMP\reactor-demo-instance.log"
    }
    Wait-ApiReady
}

function Invoke-Api {
    param([string]$Method, [string]$Path, [object]$Body, [switch]$Anonymous, [switch]$AllowRejection)
    $uri = "http://localhost:$Port$Path"
    $headers = @{}
    if (-not $Anonymous) {
        $headers["Authorization"] = "Basic " + [Convert]::ToBase64String(
            [Text.Encoding]::UTF8.GetBytes("$AdminUser`:$AdminPassword"))
    }
    $invocation = @{
        Method = $Method
        Uri = $uri
        Headers = $headers
        TimeoutSec = 15
    }
    if ($null -ne $Body) {
        $invocation["ContentType"] = "application/json"
        $invocation["Body"] = ($Body | ConvertTo-Json -Depth 8)
    }
    if ($AllowRejection) {
        # Invoke-WebRequest keeps the status code and the raw body of a rejected call.
        return Invoke-WebRequest @invocation -SkipHttpErrorCheck
    }
    # A JSON array response reaches the pipeline as ONE object (the array itself), so a caller
    # that collects the result - @(Invoke-Api ...) for the shard instances - would count 1 with
    # the elements nested inside instead of one entry per element. Piping through Write-Output
    # enumerates the array; single-object responses pass through unchanged.
    return Invoke-RestMethod @invocation | Write-Output
}

function Wait-ApiReady {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        if ($instance.HasExited) { throw "the engine exited during startup - check the instance log" }
        try {
            Invoke-Api -Method Get -Path "/api/v1/tasks" | Out-Null
            return
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    throw "the engine did not become ready within 60 seconds"
}

function Wait-TaskStatus {
    param([int]$TaskId, [string]$Expected, [int]$TimeoutSeconds = 60)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $task = Invoke-Api -Method Get -Path "/api/v1/tasks/$TaskId"
        if ($task.status -eq $Expected) { return $task }
        Start-Sleep -Milliseconds 300
    }
    throw "task $TaskId did not reach $Expected within ${TimeoutSeconds}s (last status: $($task.status))"
}

function Wait-TaskTerminal {
    param([int]$TaskId, [int]$TimeoutSeconds = 90)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $task = Invoke-Api -Method Get -Path "/api/v1/tasks/$TaskId"
        if ($task.status -in @("SUCCEEDED", "FAILED")) { return $task }
        Start-Sleep -Milliseconds 300
    }
    throw "task $TaskId did not reach a terminal state within ${TimeoutSeconds}s (last status: $($task.status))"
}

try {
    # ---------------------------------------------------------------- preflight
    Write-Step "0. 预检"
    $containerState = docker inspect --format='{{.State.Running}}' $Container 2>$null
    if ($containerState -ne "true") {
        throw "container $Container is not running - start it with: docker compose up -d mysql"
    }
    Write-Line "MySQL 容器 $Container 运行中"
    if (-not (Test-Path $jar)) {
        throw "jar not found at $jar - build it with: .\mvnw.cmd -B -DskipTests package"
    }
    Write-Line "jar 就绪：$jar"
    if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
        throw "port $Port is already in use - stop the running instance first (or pass -Port)"
    }
    Write-Line "端口 $Port 空闲"

    # ---------------------------------------------------------------- start
    Start-DemoInstance
    Write-Step "启动引擎实例（PID $($instance.Id)，端口 $Port，worker 已启用）"
    Write-Line "API 就绪：http://localhost:$Port"

    # ---------------------------------------------------------------- 1. submit and execute
    Write-Step "1. 提交即执行：三个 echo 任务跑到 SUCCEEDED"
    $echoIds = @()
    foreach ($i in 1..$DemoTasks) {
        $response = Invoke-Api -Method Post -Path "/api/v1/tasks" -Body @{
            taskType = "echo"
            payload = @{ sleepMs = $i * 100 }
            idempotencyScope = $demoScope
            idempotencyKey = "echo-$i"
        }
        $echoIds += $response.task.id
    }
    foreach ($id in $echoIds) {
        $task = Wait-TaskStatus -TaskId $id -Expected "SUCCEEDED"
        $script:demoSucceeded++
        Write-Line ("任务 {0}: {1}，尝试 {2} 次（提交成功只代表被接受，执行结果以状态为准）" -f $id, $task.status, $task.attemptCount)
    }

    # ---------------------------------------------------------------- 2. cancel and audit
    # The cancel target must still be PENDING, and RUNNING -> CANCELLED is not a legal transition
    # (PRD 4.2). Restarting as an API-only process (worker disabled) makes that deterministic and
    # shows the documented "API only" mode at the same time.
    Stop-DemoInstance
    Start-DemoInstance -ApiOnly
    Write-Step "2. 取消与审计：管理写操作需要 ADMIN 凭证"
    Write-Line "已切换为 API-only 实例（reactorsched.worker.enabled=false，PID $($instance.Id)）：任务不会被执行"
    $cancelTarget = (Invoke-Api -Method Post -Path "/api/v1/tasks" -Body @{
        taskType = "echo"
        payload = @{ sleepMs = 100 }
        idempotencyScope = $demoScope
        idempotencyKey = "cancel-me"
    }).task.id
    Write-Line "已提交任务 $cancelTarget（停在 PENDING，等着被取消）"
    $rejected = Invoke-Api -Method Post -Path "/api/v1/tasks/$cancelTarget/cancel" -Anonymous `
        -AllowRejection -Body @{ operator = "demo"; reason = "without credentials" }
    $rejectedBody = $rejected.Content | ConvertFrom-Json
    Write-Line ("无凭证取消 -> HTTP {0} {1}" -f [int]$rejected.StatusCode, ($rejectedBody | ConvertTo-Json -Compress))
    if ([int]$rejected.StatusCode -ne 401) {
        throw "expected the anonymous cancel to be rejected with 401"
    }
    $cancelled = Invoke-Api -Method Post -Path "/api/v1/tasks/$cancelTarget/cancel" -Body @{
        operator = "demo"
        reason = "duplicate order"
    }
    Write-Line "带凭证取消 -> HTTP 200，状态 $($cancelled.status)"
    Write-Line "审计留痕（task.last_error）：$($cancelled.lastError)"
    $script:demoCancelled++

    # ---------------------------------------------------------------- 3. fault and retry
    Stop-DemoInstance
    Start-DemoInstance
    Write-Step "3. 故障注入与重试：真实下游调用、真实退避"
    Write-Line "已切回启用 worker 的实例（PID $($instance.Id)）"
    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "demo"
        latencyMs = $HealthyLatencyMs
        errorRate = 0.0
    } | Out-Null
    Write-Line "下游恢复健康：${HealthyLatencyMs}ms / 0% 失败"
    $healthyIds = @()
    foreach ($i in 1..$DemoTasks) {
        $response = Invoke-Api -Method Post -Path "/api/v1/tasks" -Body @{
            taskType = "downstream"
            payload = @{}
            maxAttempts = 4
            idempotencyScope = $demoScope
            idempotencyKey = "downstream-healthy-$i"
        }
        $healthyIds += $response.task.id
    }
    foreach ($id in $healthyIds) {
        Wait-TaskStatus -TaskId $id -Expected "SUCCEEDED" | Out-Null
        $script:demoSucceeded++
    }
    Write-Line "$DemoTasks 个下游任务全部 SUCCEEDED"

    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "demo"
        latencyMs = $FaultLatencyMs
        errorRate = 1.0
        failureKind = "TRANSIENT"
    } | Out-Null
    Write-Line "注入故障：${FaultLatencyMs}ms / 100% 失败（TRANSIENT，可重试）"
    $faultIds = @()
    foreach ($i in 1..$DemoTasks) {
        $response = Invoke-Api -Method Post -Path "/api/v1/tasks" -Body @{
            taskType = "downstream"
            payload = @{}
            maxAttempts = 4
            idempotencyScope = $demoScope
            idempotencyKey = "downstream-fault-$i"
        }
        $faultIds += $response.task.id
    }
    $retried = $null
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline -and $null -eq $retried) {
        foreach ($id in $faultIds) {
            $task = Invoke-Api -Method Get -Path "/api/v1/tasks/$id"
            if ($task.status -eq "RETRY_WAIT") { $retried = $task; break }
        }
        if ($null -eq $retried) { Start-Sleep -Milliseconds 300 }
    }
    if ($null -eq $retried) { throw "no task entered RETRY_WAIT within 30s" }
    $nextAttempt = (Get-Date $retried.nextRunAt).ToString("HH:mm:ss")
    Write-Line "任务 $($retried.id) 进入 RETRY_WAIT（已尝试 $($retried.attemptCount) 次，下次执行 $nextAttempt）"

    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "demo"
        latencyMs = $HealthyLatencyMs
        errorRate = 0.0
    } | Out-Null
    Write-Line "下游恢复健康，等待重试完成"
    $faultOutcomes = @()
    foreach ($id in $faultIds) {
        $task = Wait-TaskTerminal -TaskId $id
        $faultOutcomes += "$id`: $($task.status)（$($task.attemptCount) 次尝试）"
        if ($task.status -eq "SUCCEEDED") { $script:demoSucceeded++ } else { $script:demoFailed++ }
    }
    foreach ($outcome in $faultOutcomes) { Write-Line "  任务 $outcome" }
    if ($script:demoFailed -eq 0) {
        Write-Line "$DemoTasks 个故障期任务全部重试到 SUCCEEDED（maxAttempts=4 未被击穿）"
    } else {
        Write-Line "$DemoTasks 个故障期任务中有 $script:demoFailed 个重试耗尽进入 FAILED（真实结果，不掩盖）"
    }

    # ---------------------------------------------------------------- 4. explain one task
    Write-Step "4. 解释一个任务：时间线与决策记录"
    $timeline = Invoke-Api -Method Get -Path "/api/v1/tasks/$($retried.id)/timeline"
    Write-Line "任务 $($retried.id) 的时间线（按时间合并状态、执行尝试与决策）："
    foreach ($event in $timeline.events) {
        $label = $event.type
        if ($event.reasonCode) { $label = "$label/$($event.reasonCode)" }
        $details = if ($event.details) { $event.details } else { "" }
        Write-Line ("  [{0}] {1} {2}" -f $event.at, $label, $details)
    }

    # ---------------------------------------------------------------- 5. shards, crash, rerun
    # PRD 16's walkthrough: definition -> trigger -> shards -> executor crash -> rerun. The
    # shards are created by the scheduler scan (the manual trigger endpoint does not expand
    # shards), the "crash" is the fault-lab endpoint, and the rerun is the ordinary lease
    # recovery path. Two executors with one of them killed is pinned by the automated evidence
    # TwoExecutorsKillOneIT; this section demonstrates the same chain through the public API.
    Write-Step "5. 分片、崩溃与重跑：定义 -> 触发 -> 分片 -> 执行器崩溃 -> 重跑"
    $runAt = (Get-Date).ToUniversalTime().AddSeconds(3).ToString("yyyy-MM-ddTHH:mm:ss")
    $shardJob = Invoke-Api -Method Post -Path "/api/v1/jobs?operator=demo" -Body @{
        jobKey = "$demoScope-sharded"
        name = "分片演示任务"
        taskType = "downstream"
        scheduleType = "ONE_TIME"
        runAt = $runAt
        shardTotal = 2
        payloadTemplate = @{}
        maxAttempts = 5
    }
    Write-Line "已创建分片定义 job $($shardJob.id)（shardTotal=2，runAt=$runAt，等扫描触发）"
    # A slow-but-healthy downstream keeps the crash section's attempt in flight when the fault
    # lands: crash() interrupts the execution pool, so an attempt that already finished would
    # report its result and leave nothing for the recovery path to rerun.
    Invoke-Api -Method Put -Path "/api/v1/fault-lab/downstream" -Body @{
        operator = "demo"; latencyMs = $CrashLatencyMs; errorRate = 0.0
    } | Out-Null
    Write-Line "下游放慢到 ${CrashLatencyMs}ms（0% 失败）：让在途 attempt 停在执行中，崩溃才能打断它"

    # Wait for the scan to expand the fire time into one instance per shard.
    $shardInstances = @()
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline -and $shardInstances.Count -lt 2) {
        $shardInstances = @(Invoke-Api -Method Get -Path "/api/v1/jobs/$($shardJob.id)/instances")
        if ($shardInstances.Count -lt 2) { Start-Sleep -Milliseconds 300 }
    }
    if ($shardInstances.Count -lt 2) { throw "the sharded definition did not create two instances within 30s" }
    foreach ($inst in $shardInstances) {
        Write-Line ("分片实例 {0}: shard {1}/{2}，初始状态 {3}" -f $inst.id, ($inst.shardIndex + 1), $inst.shardTotal, $inst.status)
    }

    # Second definition: this is the one that gets crashed mid-flight.
    $runAt2 = (Get-Date).ToUniversalTime().AddSeconds(6).ToString("yyyy-MM-ddTHH:mm:ss")
    $crashJob = Invoke-Api -Method Post -Path "/api/v1/jobs?operator=demo" -Body @{
        jobKey = "$demoScope-crash"
        name = "崩溃演示任务"
        taskType = "downstream"
        scheduleType = "ONE_TIME"
        runAt = $runAt2
        shardTotal = 2
        payloadTemplate = @{}
        maxAttempts = 5
    }
    $crashInstances = @()
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline -and $crashInstances.Count -lt 2) {
        $crashInstances = @(Invoke-Api -Method Get -Path "/api/v1/jobs/$($crashJob.id)/instances")
        if ($crashInstances.Count -lt 2) { Start-Sleep -Milliseconds 300 }
    }
    if ($crashInstances.Count -lt 2) { throw "the crash definition did not create two instances within 30s" }

    # Wait until one instance is actually RUNNING (claimed), then kill the worker under it.
    $claimed = $null
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline -and $null -eq $claimed) {
        foreach ($inst in $crashInstances) {
            $task = Invoke-Api -Method Get -Path "/api/v1/tasks/$($inst.id)"
            if ($task.status -eq "RUNNING") { $claimed = $task; break }
        }
        if ($null -eq $claimed) { Start-Sleep -Milliseconds 200 }
    }
    if ($null -eq $claimed) { throw "no instance reached RUNNING before the crash injection" }
    Write-Line "实例 $($claimed.id) 已被领取（RUNNING，attempt $($claimed.attemptCount)），准备强杀执行器"

    $crash = Invoke-Api -Method Post -Path "/api/v1/fault-lab/executor/crash?operator=demo&reason=executor%20crash%20demo"
    Write-Line "已强杀 worker $($crash.workerId)：停止拉取与续约，在途 attempt 的租约留给回收"

    # The process is still alive but its worker stopped; restarting the engine stands in for the
    # replacement executor taking over the group.
    Stop-DemoInstance
    Start-DemoInstance
    Write-Line "引擎已重启（替代执行器上线，PID $($instance.Id)）"

    foreach ($inst in $crashInstances) {
        $task = Wait-TaskTerminal -TaskId $inst.id -TimeoutSeconds 120
        Write-Line ("实例 {0}（分片 {1}/{2}）最终 {3}，尝试 {4} 次" -f $task.id, ($inst.shardIndex + 1), $inst.shardTotal, $task.status, $task.attemptCount)
        if ($task.status -eq "SUCCEEDED") { $script:demoSucceeded++ } else { $script:demoFailed++ }
    }
    $history = Invoke-Api -Method Get -Path "/api/v1/tasks/$($claimed.id)/executions"
    Write-Line "崩溃实例 $($claimed.id) 的执行历史（每个 attempt 一行；被崩溃打断的 attempt 不回传结果）："
    foreach ($attempt in $history) {
        $finished = if ($attempt.finishedAt) { $attempt.finishedAt } else { "未回传（租约过期被回收）" }
        Write-Line ("  attempt {0}: {1} -> {2}，worker {3}，结果 {4}" -f $attempt.attempt, $attempt.startedAt, $finished, $attempt.workerId, $(if ($attempt.result) { $attempt.result } else { "-" }))
    }

    # ---------------------------------------------------------------- 6. metrics and panel
    Write-Step "6. 指标与面板"
    # The demo's own tally counts only the tasks this run submitted; the metrics window covers
    # every task that reached a terminal state in the database since the run started, which can
    # include a leftover task of an interrupted earlier run being reclaimed.
    Write-Line ("本次演示提交 {0} 个任务：成功 {1}、失败 {2}、取消 {3}（取消不计入失败率）" -f `
        ($script:demoSucceeded + $script:demoFailed + $script:demoCancelled), $script:demoSucceeded, `
        $script:demoFailed, $script:demoCancelled)
    $summary = Invoke-Api -Method Get -Path "/api/v1/metrics/summary?from=$demoStartedAt"
    Write-Line ("指标窗口 [from, to)（库内全部任务）：完成 {0}（成功 {1} / 失败 {2}），失败率 {3}，吞吐 {4} 任务/秒，当前积压 {5}" -f `
        $summary.completedTasks, $summary.succeededTasks, $summary.failedTasks, `
        [math]::Round($summary.failureRate, 4), [math]::Round($summary.throughputPerSecond, 3), $summary.backlogNow)
    $decisions = Invoke-Api -Method Get -Path "/api/v1/decisions?from=$demoStartedAt&size=1"
    Write-Line "本次运行写入决策记录 $($decisions.total) 行（全部由调度/控制代码路径写入，可按 taskId / 原因码过滤）"
    Write-Line "只读面板：http://localhost:$Port/panel/index.html"

    Write-Step "演示完成"
    Write-Line "固定并发 vs 自适应并发的对照实验见 docs/reports/phase-4-comparison-experiment.md"
    Write-Line "复现实验：pwsh -NoProfile -File scripts/phase-4-comparison-experiment.ps1"
}
finally {
    $wasRunning = $null -ne $instance
    Stop-DemoInstance
    if ($wasRunning) {
        Write-Host ""
        Write-Line "引擎实例已停止（实例日志：$env:TEMP\reactor-demo-instance.log）"
    }
}
